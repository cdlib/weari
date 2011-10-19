/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer.pig;

import java.util.ArrayList;

import java.net.URI;

import org.apache.hadoop._;
import org.apache.hadoop.fs._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapreduce._;
import org.apache.hadoop.mapreduce.lib.input._;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import org.apache.pig._;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data._;

import org.archive.io.{ArchiveReader,ArchiveReaderFactory,ArchiveRecord};
import org.cdlib.was.ngIndexer._;
import org.cdlib.was.ngIndexer.Utility.null2option;

class ArchiveListLoader extends LoadFunc {
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
        println("GOT EMPTY PARSE!: %s".format(rec.getUrl));
      }
      return retval;
    }
  }

  override def getNext : Tuple = {
    try {
      getNextArchiveRecord match {
        case None => {
          return null;
        }
        case Some(rec) => {
          var tuple = tupleFactory.newTupleNoCopy(new java.util.ArrayList[java.lang.Object]());
          val outlinks = bagFactory.newDistinctBag;
          for (link <- rec.outlinks) { outlinks.add(tupleFactory.newTuple(link)); }
          tuple.append(rec.getUrl)
          tuple.append(rec.content.getOrElse("-"));
          tuple.append(rec.detectedContentType.getOrElse(ContentType.DEFAULT).mediaType);
          tuple.append(rec.suppliedContentType.mediaType);
          tuple.append(rec.title);
          /*tuple.append(rec.getDigestStr.getOrElse("-"));
          tuple.append(outlinks); */
          return tuple;
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
