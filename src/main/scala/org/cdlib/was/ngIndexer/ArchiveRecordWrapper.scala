package org.cdlib.was.ngIndexer;

import java.io.InputStream;

import org.apache.http.Header;
import org.apache.http.ParseException;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.impl.io.{AbstractMessageParser,HttpResponseParser};
import org.apache.http.message.ParserCursor;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.CharArrayBuffer;

import org.apache.tika.metadata.HttpHeaders;

import org.archive.io.{ArchiveReaderFactory,ArchiveRecord};
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

import org.apache.http.message.BasicLineParser;

class ArchiveRecordWrapper (rec : ArchiveRecord) extends InputStream {

  private var ready : Boolean = false;
  private var contentType : Option[String] = None;
  private var httpResponse : Boolean = false;
  
  private def readLine (is : InputStream) : CharArrayBuffer = {
    val buff = new CharArrayBuffer(1024);
    var b : Int = 0;
    b = is.read;
    while (b != -1) {
      if (b == 13) {
        // CR - now read LF
        is.read;
        return buff;
      } else if (b == 10) {
        // LF
        return buff;
      } else {
        buff.append(b.asInstanceOf[Char]);
      }
      b = is.read;
    }
    return buff;
  }

  private def joinHeaderLines (lines : Seq[CharArrayBuffer]) : Seq[CharArrayBuffer] = {
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

  private def readHeaderLines (is : InputStream) : Seq[CharArrayBuffer] = {
    var lines = List[CharArrayBuffer]();
    var line = readLine(is);
    while (!line.isEmpty) {
      lines = line :: lines;
      line = readLine(is);
    }
    return joinHeaderLines(lines);
  }

  def parseHeaders (rec : WARCRecord) : Option[Pair[Int,Map[String,Header]]] = {
    val header = rec.getHeader();
    val firstLine = readLine(rec);
    val lineParser = new BasicLineParser;
    try {
      val statusLine = lineParser.parseStatusLine(firstLine, new ParserCursor(0, firstLine.length - 1));
      val headers = readHeaderLines(rec).map(lineParser.parseHeader(_));
      var headerMap = Map[String,Header]();
      headerMap ++= headers.map { (h)=> h.getName.toLowerCase->h };
      return Some((statusLine.getStatusCode, headerMap));
    } catch {
      case ex : ParseException => {
        ex.printStackTrace(System.err);
        return None;
      }
    }
  } 

  /** Cue up a record to its start, fill in httpResponse & contentType. */
  def cueUp {
    ready = true;
    rec match {
      case rec1 : WARCRecord => {
        if (rec1.getHeader.getMimetype == "application/http; msgtype=response") {
          httpResponse = true;
          val header = parseHeaders(rec1);
          header.map {(header1)=>
            val responseCode = header1._1;
            val headers = header1._2;
            contentType = headers.get(HttpHeaders.CONTENT_TYPE.toLowerCase).map(_.getValue);
          }
        }
      }
      case rec1 : ARCRecord => {
        val contentBegin = rec1.getMetaData.getContentBegin;
        var totalbytesread = 0;
        var bytesread = 0;
        var buffer = new Array[Byte](1024);
        while (bytesread != -1 && totalbytesread + bytesread < contentBegin) {
          totalbytesread += bytesread;
          bytesread = rec1.read(buffer, 0, scala.math.min(1024, contentBegin - totalbytesread));
        }
        val url = rec1.getMetaData.getUrl;
        httpResponse = !url.startsWith("filedesc:") && !url.startsWith("dns:");
        contentType = Some(rec1.getHeader.getMimetype.toLowerCase);
      }
    }
  }

  // def getUniqueId (archiveRecord : ArchiveRecord) : String = {
  //   archiveRecord match {
  //     case rec : ARCRecord => {
  //       Utility.skipHttpHeader(rec);
  //       val uuri = UURIFactory.getInstance(rec.getMetaData.getUrl);
  //       return "%s.%s".format(uuri.toString, rec.getDigestStr);
  //     }
  //   }
  // }
  
  def getContentType : Option[String] = {
    if (!ready) cueUp;
    return contentType;
  }

  def isHttpResponse : Boolean = {
    if (!ready) cueUp;
    return httpResponse;
  }

  def getUrl = rec.getHeader.getUrl;

  def getLength = rec.getHeader.getLength;

  def getDate = rec.getHeader.getDate;

  def getDigestStr = rec.getDigestStr;

  /* InputStream wrapper */
  override def available = { 
    if (!ready) cueUp;
    rec.available; 
  }
  
  override def close = rec.close;
  
  override def mark (readlimit : Int) = {
    if (!ready) cueUp; 
    rec.mark(readlimit);
  }
  
  override def markSupported = rec.markSupported;

  override def read = {
    if (!ready) cueUp; 
    rec.read;
  }

  override def read (buff : Array[Byte]) = {
    if (!ready) cueUp; 
    rec.read(buff);
  }

  override def read (buff : Array[Byte], off : Int, len : Int) = {
    if (!ready) cueUp; 
    rec.read(buff, off, len);
  }

  override def reset = {
    if (!ready) cueUp;
    rec.reset;
  }

  override def skip (n : Long) = {
    if (!ready) cueUp; 
    rec.skip(n);
  }
}
