/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer.pig;

import java.io.{FilterOutputStream,IOException,OutputStream,OutputStreamWriter};
import java.util.Date;

import org.apache.hadoop.fs.{Path};
import org.apache.hadoop.io.{compress,Text};
import org.apache.hadoop.mapreduce.{Job,OutputFormat,RecordWriter,TaskAttemptContext};
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat};
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.pig.{StoreFunc};
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.{DataBag,Tuple};

import org.cdlib.was.ngIndexer._;
import org.cdlib.was.ngIndexer.Utility.string2date;

import scala.collection.JavaConversions.asScalaIterator;

/**
 * Class to store a parsed archive record as JSON.
 */
class JsonParsedArchiveRecordStorer extends StoreFunc {
  class LineRecordWriter (os : OutputStream) 
        extends RecordWriter[Text, ParsedArchiveRecord] {

    val indexer = new SolrIndexer;
    
    val w = new OutputStreamWriter(os);
    
    var firstRecord = true;
    
    def write(key : Text, value : ParsedArchiveRecord) {
      this.synchronized {
        if (firstRecord) {
          firstRecord = false;
        } else {
          w.write(",\n");
        }
        indexer.writeRec(value, w);
      }
    }
    
    def close(context : TaskAttemptContext) {
      this.synchronized { 
        w.close();
      }
    }
  }
  
  class FinishJsonArrayOutputStream (os : OutputStream) 
        extends FilterOutputStream (os) {
    override def close = {
      write("]\n".getBytes("UTF-8"));
      super.close;
    }
  }
      
  class MyOutputFormat
        extends FileOutputFormat[Text, ParsedArchiveRecord] {

    def getRecordWriter(job : TaskAttemptContext) : LineRecordWriter = {
      val conf = job.getConfiguration;
      var codec : Option[compress.CompressionCodec] = 
        if (FileOutputFormat.getCompressOutput(job)) {
          val codecClass = 
            FileOutputFormat.getOutputCompressorClass(job, classOf[compress.GzipCodec]);
          Some(ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[compress.CompressionCodec]);
        } else {
          None;
        }
      val file = getDefaultWorkFile(job, codec.map(_.getDefaultExtension).getOrElse(""))
      val fileOutRaw = file.getFileSystem(conf).create(file, false);
      val fileOut = codec.map(_.createOutputStream(fileOutRaw)).getOrElse(fileOutRaw);
      fileOut.write("[\n".getBytes("UTF-8"));
      return new LineRecordWriter(new FinishJsonArrayOutputStream(fileOut));
    }
  }

  var writer : RecordWriter[Text,ParsedArchiveRecord] = _;

  implicit def tuple2rec (f : Tuple) : ParsedArchiveRecord = {
    val outlinks = for (link <- f.get(9).asInstanceOf[DataBag].iterator)
      yield link.get(0).asInstanceOf[Long];

    return ParsedArchiveRecord(filename            = f.get(0).asInstanceOf[String],
                               digest              = Some(f.get(2).asInstanceOf[String]),
                               url                 = f.get(1).asInstanceOf[String],
                               date                = f.get(3).asInstanceOf[String],
                               title               = Some(f.get(8).asInstanceOf[String]),
                               length              = f.get(4).asInstanceOf[Long],
                               content             = Some(f.get(5).asInstanceOf[String]),
                               suppliedContentType = 
                                 ContentType.forceParse(f.get(7).asInstanceOf[String]),
                               detectedContentType = 
                                 ContentType.parse(f.get(6).asInstanceOf[String]),
                               outlinks            = outlinks.toSeq)
  }

  override def putNext(f : Tuple) {
    try {
      writer.write(null.asInstanceOf[Text], f);
    } catch {
      case ex : InterruptedException =>
        throw new IOException(ex);
    }
  }

  override def getOutputFormat = new MyOutputFormat;

  def prepareToWrite(writer : RecordWriter[_,_]) {
    this.writer = writer.asInstanceOf[RecordWriter[Text,ParsedArchiveRecord]];
  }

  override def setStoreLocation(location : String, job : Job) {
    FileOutputFormat.setOutputPath(job, new Path(location));
    if (location.endsWith(".bz2")) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job,  classOf[compress.BZip2Codec]);
    } else if (location.endsWith(".gz")) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, classOf[compress.GzipCodec]);
    }
  }
}
