package org.cdlib.was.ngIndexer;

import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.message.ParserCursor;
import org.apache.http.Header;
import org.apache.http.ParseException;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.CharArrayBuffer;
import org.apache.http.impl.io.{AbstractMessageParser,HttpResponseParser};
import org.apache.http.impl.DefaultHttpResponseFactory;

import org.archive.io.{ArchiveReaderFactory,ArchiveRecord};
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

import org.apache.http.message.BasicLineParser;

import java.io.{BufferedInputStream,BufferedReader,File,InputStream,InputStreamReader};

import scala.util.matching.Regex;

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
    val reader = ArchiveReaderFactory.get(arcFile)
    val it = reader.iterator;
    while (it.hasNext) {
      val next = it.next;
      f (next);
      next.close;
    }
    reader.close;
  }

  def eachArc (stream : java.io.InputStream, arcName : String, f: (ArchiveRecord)=>Unit) {
    val reader = ArchiveReaderFactory.get(arcName, stream, true);
    val it = reader.iterator;
    while (it.hasNext) {
      val next = it.next;
      f (next);
      next.close;
    }
    reader.close;
  }

  def readLine (br : BufferedReader) : CharArrayBuffer = {
    val buff = new CharArrayBuffer(1024);
    val line = br.readLine;
    val arr = line.toCharArray;
    buff.append(arr, 0, arr.length);
    return buff;
  }

  def joinHeaderLines (lines : Seq[CharArrayBuffer]) : Seq[CharArrayBuffer] = {
    var lastLine = new CharArrayBuffer(0);
    var retval = List[CharArrayBuffer]();
    for (line <- lines) {
      if ((line.charAt(0) == ' ') || (line.charAt(0) == '\t')) {
        lastLine.append(line);
      } else {
        retval = line :: retval;
        lastLine = line;
      }
    }
    return retval;
  }

  def readHeaderLines (br : BufferedReader) : Seq[CharArrayBuffer] = {
    var lines = List[CharArrayBuffer]();
    var line = readLine(br);
    while (!line.isEmpty) {
      lines = line :: lines;
      line = readLine(br);
    }
    return joinHeaderLines(lines);
  }

  def parseHeaders (rec : WARCRecord) : Option[Pair[Int,Map[String,Header]]] = {
    val header = rec.getHeader();
    val br = new BufferedReader(new InputStreamReader(rec, "UTF-8"));
    val firstLine = readLine(br);
    val lineParser = new BasicLineParser;
    try {
      val statusLine = lineParser.parseStatusLine(firstLine, new ParserCursor(0, firstLine.length - 1));
      val headers = readHeaderLines(br).map(lineParser.parseHeader(_));
      var headerMap = Map[String,Header]();
      headerMap ++= headers.map { (h)=> h.getName.toLowerCase->h };
      return Some((statusLine.getStatusCode, headerMap));
    } catch {
      case ex : ParseException => {
        return None;
      }
    }
  } 
}
