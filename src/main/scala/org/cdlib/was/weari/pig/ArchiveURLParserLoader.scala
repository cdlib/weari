/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari.pig;

import java.io.{EOFException,File,FileInputStream};

import java.security.{MessageDigest=>JavaMessageDigest};

import java.util.ArrayList;

import java.net.URI;

import org.apache.hadoop.io.{Text};
import org.apache.hadoop.mapreduce.{Job,JobContext,RecordReader};
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat,TextInputFormat};

import org.apache.pig.{LoadFunc,PigException};
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.{BagFactory,Tuple,TupleFactory};

import org.cdlib.was.weari._;
import org.cdlib.was.weari.Utility.{date2string,flushStream,null2option,withFileOutputStream};

import grizzled.slf4j.Logging;

class ArchiveURLParserLoader extends LoadFunc with Logging {
  val tupleFactory = TupleFactory.getInstance();
  val bagFactory = BagFactory.getInstance();
  
  val httpClient = new SimpleHttpClient;
  val parser = new MyParser;
  var in : RecordReader[_,_] = null;

  var it : Option[Iterator[ArchiveRecordWrapper]] = None;
  var arcName : Option[String] = None;
  var tmpfile : Option[File] = None;

  def reset {
    this.tmpfile.map(_.delete);
    this.tmpfile = None;
    this.it = None; 
    this.arcName = None;
  }
    
  /**
   * Ensure that we have an iterator, or return false if we are done.
   */
  def ensureHasNext : Boolean = {
    if (this.it.isDefined && this.it.get.hasNext) {
      /* we already have an iterator! */
      return true;
    } else {
      /* keep trying while there are more URIs to process */
      while (in.nextKeyValue()) {
        /* clean up */
        reset;
        
        /* get a new ARC reader */
        val value = this.in.getCurrentValue.asInstanceOf[Text]
        val uri = new URI(value.toString);
        val matcher = Utility.ARC_RE.pattern.matcher(uri.getPath);
        if (!matcher.matches) {
          /* this probably won't happen, but we should know about it if it does */
          throw new Exception("Not an ARC file: %s".format(value));
        } else {
          this.arcName = Some(matcher.group(1));
          /* download to a temp file with the arc name */
          httpClient.getUri(uri) { is =>
            this.tmpfile = Some(new File(new File(System.getProperty("java.io.tmpdir")), this.arcName.getOrElse("")));
            withFileOutputStream(this.tmpfile.get) { os => flushStream(is, os) } 
          }
          if (this.tmpfile.isDefined) {
            try {
              this.it = Some(ArchiveReaderFactoryWrapper.get(this.tmpfile.get).iterator);
            } catch {
              case ex : EOFException => {
                /* probably a completely empty file */
                error("Could not open arc: %s".format(this.arcName));
              }
            }
          }
          if (this.it.isDefined && this.it.get.hasNext) {
            return true;
          }
          /* otherwise we keep trying */
        }
      }
      /* no more URIs in list */
      return false;
    }
  }

  /**
   * Checks if a gzip is valid. If it is, throw the exception,
   * otherwise do not, and move to the next ARC.
   */
  def checkGzip (ex : Exception) {
    if (this.tmpfile.isEmpty) {
      throw ex;
    } else {
      if (Utility.checkGzip(this.tmpfile.get)) {
        throw ex;
      } else {
        /* it is a bad gzip */
        error("Bad GZIP file: %s".format(this.arcName));
        reset;
      }
    }
  }

  /**
   * Do something with the next record, closing the rec when finished.
   * If we get an EOFException, check the gzip. If it is good, throw
   * the exception. Otherwise just return the results of the function.
   */
  def withNextRecord[T] (f : (ArchiveRecordWrapper) => T) : Option[T] = {
    var rec : ArchiveRecordWrapper = null;
    try {
      rec = this.it.get.next;
      return Some(f(rec));
    } catch {
      case ex : Exception => {
        /* log the exception before we rethrow it, so we know what arc was being read */
        error("Caught exception READING %s: {}".format(this.arcName.getOrElse("")), ex);
        checkGzip(ex);
        /* set to null so we don't try to close it later & throw ANOTHER exception */
        rec = null;
        return None;
      }
    } finally {
      /* now try to close the record */
      try {
        if (rec != null) rec.close;
      } catch {
        case ex : Exception => {
          /* log the exception before we rethrow it, so we know what arc was being closed */
          error("Caught exception CLOSING %s: {}".format(this.arcName.getOrElse("")), ex);
          checkGzip(ex);
        }
      }
    }
  }

  /**
   * Transform a ParsedArchiveRecord into a Pig Tuple.
   */
  def rec2tuple (rec : ParsedArchiveRecord) : Tuple = {
    var tuple = tupleFactory.newTupleNoCopy(new java.util.ArrayList[java.lang.Object]());
    val outlinks = bagFactory.newDistinctBag;
    for (link <- rec.outlinks) { 
      outlinks.add(tupleFactory.newTuple(link));
    }
    for (value <- Seq(rec.getFilename,                // 0
                      rec.getUrl,                     // 1\
                      rec.getDigestStr.getOrElse(""), // 2
                      date2string(rec.getDate),       // 3
                      rec.getLength,                  // 4
                      rec.content.getOrElse(""),      // 5
                      rec.detectedContentType.        // 
                        getOrElse(ContentType.DEFAULT).mediaType,
                      rec.suppliedContentType.mediaType,  // 7
                      rec.title.getOrElse(""),        // 8
                      outlinks)) {                    // 9
      tuple.append(value);
    }
    return tuple;
  }
  
  def getNext : Tuple = {
    try {
      var next : Option[ParsedArchiveRecord] = None;
      while (next.isEmpty) {
        if (!ensureHasNext) {
          return null;
        } else {
          /* we have Option[Option[ParsedArchiveRecord]] here, so we need to flatten */
          next = withNextRecord(parser.safeParse(_)).flatten.headOption;
        }
      }
      return rec2tuple(next.get);
    } catch {
      case ex : InterruptedException =>
        throw new ExecException("Error while reading input", 6018,
                                PigException.REMOTE_ENVIRONMENT, ex);
    }
  }
  
  override def getInputFormat = new ArcListInputFormat;

  override def prepareToRead(reader : RecordReader[_,_], split : PigSplit) {
    in = reader;
  }

  override def setLocation(location : String, job : Job) {
    FileInputFormat.setInputPaths(job, location);
  }
}
