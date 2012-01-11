package org.cdlib.was.weari;

import java.io.{File,InputStream,IOException,OutputStream};
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.cdlib.was.weari.Utility.{flushStream};

object DecompressJSON {

  val glob = new Path("/user/was/json/*.gz");

  def main (args : Array[String]) {
    val conf = new Configuration();
    val fs = FileSystem.get(conf);
    for (childStatus <- fs.globStatus(glob)) {
      val path = childStatus.getPath;
      val name = path.getName;
      val newPath = new Path(path.getParent, name.substring(0, name.length-3));
      if (!fs.isFile(newPath)) {
        val in = fs.open(path);
        val gzin = new GZIPInputStream(in);
        val out = fs.create(newPath);
        flushStream(gzin, out);
        out.close;
        gzin.close;
      }
    }
  }
}
