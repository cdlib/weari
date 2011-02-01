package org.cdlib.was.ngIndexer;

import org.archive.io.ArchiveRecord;
import org.archive.io.arc.{ARCRecord,ARCReaderFactory};

import java.io.File;

object Utility {
  def skipHttpHeader (rec : ARCRecord) {
    val contentBegin = rec.getMetaData.getContentBegin;
    var totalbytesread = 0;
    var bytesread = 0;
    var buffer = new Array[Byte](1024);
    while (bytesread != -1 && totalbytesread + bytesread < contentBegin) {
      totalbytesread += bytesread;
      bytesread = rec.read(buffer, 0, scala.math.min(1024, contentBegin - totalbytesread));
    }
  }

  def eachArcRecursive[T](file : File) (func : (ArchiveRecord) => Unit) {
    if (file.isDirectory) {
      for (child <- file.listFiles) {
        eachArcRecursive(child)(func);
      }
    } else if (file.getName.indexOf("arc.gz") != -1) {
      eachArc(file, func);
    }
  }

  def eachArc (arcFile : java.io.File, f: (ArchiveRecord)=>Unit) {
    val reader = ARCReaderFactory.get(arcFile)
    val it = reader.iterator;
    while (it.hasNext) {
      val next = it.next;
      f (next);
      next.close;
    }
    reader.close;
  }

  def eachArc (stream : java.io.InputStream, arcName : String, f: (ArchiveRecord)=>Unit) {
    val reader = ARCReaderFactory.get(arcName, stream, true);
    val it = reader.iterator;
    while (it.hasNext) {
      val next = it.next;
      f (next);
      next.close;
    }
    reader.close;
  }
}
