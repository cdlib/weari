/* Copyright (c) 2009-2012 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.{File,InputStream,IOException,OutputStream};
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.cdlib.was.weari.Utility.{flushStream};

import com.typesafe.scalalogging.slf4j.Logging;

/**
 * Utility to decompress gz files in a Hadoop FS.
 */
object DecompressJSON extends Logging with ExceptionLogger {

  def main (args : Array[String]) {
    val glob = new Path(args(0));
    val conf = new Configuration();
    val fs = FileSystem.get(conf);

    for (childStatus <- fs.globStatus(glob)) {
      val path = childStatus.getPath;
      val name = path.getName;
      val newPath = new Path(path.getParent, name.substring(0, name.length-3));
      if (fs.isFile(newPath)) {
        println("Already decompressed: %s.".format(name));
      } else {
        catchAndLogExceptions("Problem with file %s: {}.".format(path.toString)) {
          var in : InputStream = null;
          var out : OutputStream = null;
          try {
            in = fs.open(path);
            out = fs.create(newPath);
            val gzin = new GZIPInputStream(in);
            flushStream(gzin, out);
            if (out != null) out.close;
            gzin.close;
            println("Decompressed %s.".format(name));
            fs.delete(path, false);
          } catch {
            case ex : IOException => {
              logger.error(getStackTrace(ex));
              println(ex.getMessage);
              if (ex.getMessage == "Not in GZIP format") {
                val mvPath = new Path(path.getParent, "%s.bad".format(path.getName))
                fs.rename(path, mvPath);
                println("Bad gzip: %s.".format(name));
                Thread.sleep(1000);
              } else {
                // hitting hdfs too hard?
                println("Sleeping...");
                Thread.sleep(10000);
              }
            }
          } finally {
            if (in != null) in.close;
            if (out != null) out.close;
          } 
        }
      }
    }
  }
}
