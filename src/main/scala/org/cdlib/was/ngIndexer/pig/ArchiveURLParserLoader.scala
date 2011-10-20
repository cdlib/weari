/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer.pig;

import java.util.ArrayList;

import java.net.URI;

import org.apache.hadoop.io.{Text};
import org.apache.hadoop.mapreduce.{Job,RecordReader};
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat,TextInputFormat};

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import org.apache.pig.{LoadFunc,PigException};
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.{BagFactory,Tuple,TupleFactory};

import org.archive.io.{ArchiveReader,ArchiveReaderFactory,ArchiveRecord};
import org.cdlib.was.ngIndexer._;
import org.cdlib.was.ngIndexer.Utility.{date2string,null2option};

class ArchiveURLParserLoader extends LoadFunc {
  val tupleFactory = TupleFactory.getInstance();
  val bagFactory = BagFactory.getInstance();
  val client = new SimpleHttpClient;
  val parser = new MyParser;

  var in : RecordReader[_,_] = null;
  var response : HttpResponse = null;
  var reader : ArchiveReader = null;
  var it : java.util.Iterator[ArchiveRecord] = null;
  var arcName : String = _;
  val indexer = new SolrIndexer;

  def setupNextArchiveReader : Boolean = {
    if (!in.nextKeyValue()) {
      return false;
    } else {
      if (this.response != null) {
        EntityUtils.consume(response.getEntity);
      }
      this.reader = null;
      this.it = null;
      val value = this.in.getCurrentValue.asInstanceOf[Text]
      val uri = new URI(value.toString);
      val matcher = Utility.ARC_RE.pattern.matcher(uri.getPath);
      if (!matcher.matches) {
        return false;
      } else {
        arcName = matcher.group(1);
        client.getUriResponse(uri) match {
          case None => return setupNextArchiveReader;
          case Some(response) => {
            this.response = response;
            reader = ArchiveReaderFactory.get(arcName, response.getEntity.getContent, true);
            it = reader.iterator;
            return true;
          }
        }
      }
    }
  }      
  
  def getNextArchiveRecord : Option[ParsedArchiveRecord] = {
    if (it == null || !it.hasNext) {
      /* try to get a new ArchiveReader, otherwise return null */
      if (!setupNextArchiveReader) {
        return None;
      }
    }
    val rec = new ArchiveRecordWrapper(it.next, arcName);
    if (!rec.isHttpResponse || rec.getStatusCode != 200) {
      rec.close;
      /* try again */
      return getNextArchiveRecord;
    } else {
      val retval = indexer.parseArchiveRecord(rec);
      if (retval.isEmpty) {
        throw new Exception("GOT EMPTY PARSE!: %s".format(rec.getUrl));
      }
      return retval;
    }
  }

  override def getNext : Tuple = {
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
  
  override def getInputFormat = new TextInputFormat;

  override def prepareToRead(reader : RecordReader[_,_], split : PigSplit) {
    in = reader;
  }

  override def setLocation(location : String, job : Job) {
    FileInputFormat.setInputPaths(job, location);
  }
}
