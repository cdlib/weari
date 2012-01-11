package org.cdlib.was.weari;

import java.io.{File,InputStream,IOException,OutputStream};
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.cdlib.was.weari.Utility.{flushStream};

/**
 * Utility to decompress gz files in a Hadoop FS.
 */
object DecompressJSON extends Logger {

  def main (args : Array[String]) {
    val glob = new Path(args(0));
    val conf = new Configuration();
    val fs = FileSystem.get(conf);

    for (childStatus <- fs.globStatus(glob)) {
      val path = childStatus.getPath;
      val name = path.getName;
      val newPath = new Path(path.getParent, name.substring(0, name.length-3));
      if (!fs.isFile(newPath)) {
        val in = fs.open(path);
        catchAndLogExceptions("Problem with file %s: {}.".format(path.toString)) {
          try {
            val gzin = new GZIPInputStream(in);
            val out = fs.create(newPath);
            flushStream(gzin, out);
            out.close;
            gzin.close;
          } catch {
            case ex : IOException => {
              if (ex.getMessage == "Not in GZIP format.") {
                val mvPath = newPath(path.getParent, "%s.bad".format(path.getName))
                fs.rename(path, mvPath);
              }
            }
          }   
        }
      }
    }
  }
}
