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

import java.io.{FilterOutputStream,IOException,OutputStream,OutputStreamWriter};

import org.apache.hadoop.fs.{Path};
import org.apache.hadoop.io.{compress,Text};
import org.apache.hadoop.mapreduce.{Job,OutputFormat,RecordWriter,TaskAttemptContext};
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat};
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.pig.{StoreFunc};
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.{DataBag,Tuple};

import org.cdlib.was.weari._;
import org.cdlib.was.weari.Utility.string2date;

import scala.collection.JavaConversions.asScalaIterator;

/**
 * Class to store a parsed archive record as JSON.
 */
class JsonParsedArchiveRecordStorer extends StoreFunc {

  class MultiLineRecordWriter (job : TaskAttemptContext,
                               basePath : Path)
  extends RecordWriter[Text, ParsedArchiveRecord] {
    var writerMap = Map[String, LineRecordWriter]();
    def write (key : Text, value : ParsedArchiveRecord) {
      val arcName = value.getFilename;
      if (!writerMap.contains(arcName)) {
        val path = new Path(basePath,
                            "%s.json".format(arcName));
        writerMap += (arcName -> new LineRecordWriter(job, path));
      }
      writerMap(arcName).write(key,value);
    }
    
    def close (job : TaskAttemptContext) = writerMap.values.map(_.close(job));
  }

  class LineRecordWriter (job : TaskAttemptContext, 
                          var path : Path)
  extends RecordWriter[Text,ParsedArchiveRecord] {
    val conf = job.getConfiguration;
    val codec : Option[compress.CompressionCodec] = 
      if (FileOutputFormat.getCompressOutput(job)) {
        val codecClass = 
          FileOutputFormat.getOutputCompressorClass(job, classOf[compress.GzipCodec]);
          Some(ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[compress.CompressionCodec]);
        } else {
          None;
        }
    
    path = new Path("%s%s".format(path.toString, codec.map(_.getDefaultExtension).getOrElse("")))
    val fileOutRaw = path.getFileSystem(conf).create(path, false);
    val fileOut = codec.map(_.createOutputStream(fileOutRaw)).getOrElse(fileOutRaw);
    fileOut.write("[\n".getBytes("UTF-8"));
    val os = new FinishJsonArrayOutputStream(fileOut);

    val w = new OutputStreamWriter(os, "UTF-8");
    
    var firstRecord = true;
    
    def write(key : Text, value : ParsedArchiveRecord) {
      this.synchronized {
        if (firstRecord) {
          firstRecord = false;
        } else {
          w.write(",\n");
        }
        value.writeJson(w);
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

    def getRecordWriter(job : TaskAttemptContext) = {
      new MultiLineRecordWriter(job, getDefaultWorkFile(job, ""));
    }
  }

  var writer : RecordWriter[Text,ParsedArchiveRecord] = _;

  implicit def tuple2rec (f : Tuple) : ParsedArchiveRecord = {
    val outlinks = for (link <- f.get(10).asInstanceOf[DataBag].iterator)
      yield link.get(0).asInstanceOf[Long];

    return ParsedArchiveRecord(filename            = f.get(0).asInstanceOf[String],
                               url                 = f.get(1).asInstanceOf[String],
                               digest              = Some(f.get(2).asInstanceOf[String]),
                               date                = string2date(f.get(3).asInstanceOf[String]),
                               length              = f.get(4).asInstanceOf[Long],
                               content             = Some(f.get(5).asInstanceOf[String]),
                               detectedContentType = 
                                 ContentType.parse(f.get(6).asInstanceOf[String]),
                               suppliedContentType = 
                                 ContentType.forceParse(f.get(7).asInstanceOf[String]),
                               title               = Some(f.get(8).asInstanceOf[String]),
                               isRevisit           = Some(f.get(9).asInstanceOf[Boolean]),
                               outlinks            = outlinks.toSeq);
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
