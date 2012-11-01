/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
import org.cdlib.was.weari.Utility.{date2string,flushStream,null2option,withFileOutputStream,readStreamIntoFile};

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
  val tmpdir = new File(System.getProperty("java.io.tmpdir"));

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
        Utility.extractArcname(uri.toString) match {
          case None => {
            /* this probably won't happen, but we should know about it if it does */
            throw new Exception("Not an ARC file: %s".format(value));
          }
          case arcName => {
            this.arcName = arcName;
            this.tmpfile = this.arcName.map(new File(tmpdir, _));

            if (uri.getScheme == "jar") { 
              /* for testing only */
              val url = new java.net.URL(uri.toString);
              val in = url.openStream;
              try {
                readStreamIntoFile(this.tmpfile.get, in);
              } finally {
                in.close;
              }
            } else {
              /* download to a temp file with the arc name */
              httpClient.getUri(uri) { in => {
                readStreamIntoFile(this.tmpfile.get, in); 
              }}
            }

            try {
              this.it = Some(ArchiveReaderFactoryWrapper.get(this.tmpfile.get).iterator);
            } catch {
              case ex : EOFException => {
                /* probably a completely empty file */
                error("Could not open arc: %s".format(this.arcName));
              }
            }
          }
        }
        /* finished if we have a defined this.it and it has records */
        if (this.it.isDefined && this.it.get.hasNext) {
          return true;
        }
        /* otherwise we keep trying */
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
    for (value <- Seq(rec.getFilename,                   // 0
                      rec.getUrl,                        // 1
                      rec.getDigestStr.getOrElse(""),    // 2
                      date2string(rec.getDate),          // 3
                      rec.getLength,                     // 4
                      rec.content.getOrElse(""),         // 5
                      rec.detectedContentType.           // 6
                        getOrElse(ContentType.DEFAULT).mediaType,
                      rec.suppliedContentType.mediaType, // 7
                      rec.title.getOrElse(""),           // 8
                      rec.isRevisit.getOrElse(false),    // 9
                      outlinks)) {                       // 10
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
