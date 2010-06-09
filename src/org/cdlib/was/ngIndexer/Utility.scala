package org.cdlib.was.ngIndexer;

import org.archive.io._;
import org.archive.io.arc._;

object Utility {
  def skipHttpHeader (rec : ARCRecord) {
    val contentBegin = rec.getMetaData.getContentBegin;
    var totalbytesread = 0;
    var bytesread = 0;
    var buffer = new Array[Byte](1024);
    while (bytesread != -1 && totalbytesread + bytesread < contentBegin) {
      totalbytesread += bytesread;
      bytesread = rec.read(buffer, 0, Math.min(1024, contentBegin - totalbytesread));
    }
  }

  def eachArc (arcFile : java.io.File, f: (ArchiveRecord)=>Unit, count : Int) {
    val reader = ARCReaderFactory.get(arcFile)
    val it = reader.iterator;
    for (n <- 0 to count) {
      if (it.hasNext) 
        f (it.next);
    }
    reader.close;
  }

  def eachArc (arcFile : java.io.File, f: (ArchiveRecord)=>Unit) {
    val reader = ARCReaderFactory.get(arcFile)
    val it = reader.iterator;
    while (it.hasNext) {
      f (it.next);
    }
    reader.close;
  }
}
