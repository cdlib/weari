/* Copyright (c) 2011 The Regents of the University of California */


package org.cdlib.was.ngIndexer.pig;

import java.io.{File,FileInputStream};

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

import org.cdlib.was.ngIndexer._;
import org.cdlib.was.ngIndexer.Utility.{date2string,null2option};

class ArchiveURLParserLoader extends LoadFunc with Logger {
  val tupleFactory = TupleFactory.getInstance();
  val bagFactory = BagFactory.getInstance();
  
  val indexer = new SolrIndexer;
  var in : RecordReader[_,_] = null;

  var it : Option[Iterator[ArchiveRecordWrapper]] = None;
  var arcName : Option[String] = None;
  var tmpfile : Option[File] = None;

  def setupNextArchiveReader : Boolean = {
    if (!in.nextKeyValue()) {
      return false;
    } else {
      /* clean up */
      this.tmpfile.map(_.delete);
      this.it = None; 
      this.tmpfile = None;
      this.arcName = None;

      val value = this.in.getCurrentValue.asInstanceOf[Text]
      val uri = new URI(value.toString);
      val matcher = Utility.ARC_RE.pattern.matcher(uri.getPath);
      if (!matcher.matches) {
        return false;
      } else {
        this.arcName = Some(matcher.group(1));
        indexer.httpClient.getUri(uri) {(is) =>
          this.tmpfile = Some(new File(new File(System.getProperty("java.io.tmpdir")), this.arcName.getOrElse("")));
          Utility.withFileOutputStream(this.tmpfile.get) { os =>
            Utility.flushStream(is, os);
          }
          this.it = Some(ArchiveReaderFactoryWrapper.get(this.tmpfile.get).iterator);
        }
        return this.it.isDefined;
      }
    }
  }      

  def getNextArchiveRecord : Option[ParsedArchiveRecord] = {
    var rec : Option[ArchiveRecordWrapper] = None;
    while (rec.isEmpty) {
      try {
        if (this.it.isEmpty || !this.it.get.hasNext) {
          /* try to get a new ArchiveReader, otherwise return null */
          if (!setupNextArchiveReader) {
            return None;
          }
        } 
        rec = Some(this.it.get.next);
        if (!rec.get.isHttpResponse || rec.get.getStatusCode != 200) {
          /* try again */
          rec = None;
        } else {
          val retval = indexer.parseArchiveRecord(rec.get);
          if (retval.isEmpty) {
            /* this should not happen */
            throw new Exception("Got empty parse: %s".format(rec.get.getUrl));
          }
          return retval;
        }
      } catch {
        case ex : Exception => {
          /* try again */
          logger.error("Caught exception parsing %s: %s".format(this.arcName.getOrElse(""), ex));
        }
      } finally {
        rec.map(_.close);
        rec = None;
      }
    }
    return None;
  }
  
  def getNext : Tuple = {
    try {
      getNextArchiveRecord match {
        case None => null;
        case Some(rec) => {
          var tuple = tupleFactory.newTupleNoCopy(new java.util.ArrayList[java.lang.Object]());
          val outlinks = bagFactory.newDistinctBag;
          for (link <- rec.outlinks) { 
            outlinks.add(tupleFactory.newTuple(link));
          }
          for (value <- Seq(rec.getFilename,                // 0
                            rec.getUrl,                     // 1
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
          tuple;
        }
      }
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
