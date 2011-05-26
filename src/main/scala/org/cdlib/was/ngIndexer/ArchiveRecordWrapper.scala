package org.cdlib.was.ngIndexer;

import java.io.InputStream;

import org.apache.http.{Header,HeaderElement,NameValuePair};
import org.apache.http.ParseException;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.impl.io.{AbstractMessageParser,HttpResponseParser};
import org.apache.http.message.{BasicHeaderValueParser,HeaderValueParser,ParserCursor};
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.CharArrayBuffer;

import org.apache.tika.metadata.HttpHeaders;

import org.archive.io.{ArchiveReaderFactory,ArchiveRecord};
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

import org.apache.http.message.BasicLineParser;

import scala.util.matching.Regex;

/**
 * A wrapper for ArchiveRecord objects to provide a more consistent
 * interface.
 */
class ArchiveRecordWrapper (rec : ArchiveRecord, filename : String) extends InputStream {
  private var statusCode : Option[Int] = None;
  private var ready : Boolean = false;
  private var contentType : Option[String] = None;
  private var httpResponse : Boolean = false;
  private var mediaType : Option[Pair[String, String]] = None;
  private var charset : Option[String] = None;

  /**
   * Method to read one line of input from an InputStream. A line
   * ends in \n or \r\n.
   *
   */
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

  /**
   * Join together header lines.
   *
   * Header lines can have continuation lines, if the following line
   * starts with a tab or space. This function takes a sequence of
   * header lines and joins up any continuation lines with the
   * previous line. It returns the sequence of joined lines.
   * 
   */
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

  /**
   * Read the header lines from an InputStream.
   *
   * Leaves the position of the inputstream at the start of the body
   * of the message.
   */
  private def readHeaderLines (is : InputStream) : Seq[CharArrayBuffer] = {
    var lines = List[CharArrayBuffer]();
    var line = readLine(is);
    while (!line.isEmpty) {
      lines = line :: lines;
      line = readLine(is);
    }
    return joinHeaderLines(lines);
  }

  /**
   * Parse the headers from a WARCRecord.
   *
   * Takes a WARCRecord, and returns the statuscode and headers of an HTTP
   * response message. 
   *
   * @return None if there is an error in parsing, otherwise returns
   * the status code and a map of the header values.
   */
  def parseHeaders (rec : WARCRecord) : Option[Pair[Int,Map[String,Header]]] = {
    val lineParser = new BasicLineParser;
    val firstLine = readLine(rec);
    try {
      val statusLine = lineParser.parseStatusLine(firstLine, new ParserCursor(0, firstLine.length - 1));
      val headers = readHeaderLines(rec).map(lineParser.parseHeader(_));
      val headerMap = headers.map {(h)=> h.getName.toLowerCase->h }.toMap;
      return Some((statusLine.getStatusCode, headerMap));
    } catch {
      case ex : ParseException => {
        ex.printStackTrace(System.err);
        return None;
      }
    }
  } 

  /**
   * Cue up a record to its start, fill in httpResponse & contentType.
   */
  private def cueUp {
    rec match {
      case rec1 : WARCRecord => {
        if (rec1.getHeader.getMimetype == "application/http; msgtype=response") {
          httpResponse = true;
          parseHeaders(rec1).map {(headers)=>
            statusCode = Some(headers._1);
            contentType = headers._2.get(HttpHeaders.CONTENT_TYPE.toLowerCase).map(_.getValue);
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
        statusCode = Some(rec1.getStatusCode);
      }
    }
    if (contentType.isDefined) {
      val (mediaType1 : Option[Pair[String,String]], charset1) = 
        ArchiveRecordWrapper.parseContentType(contentType.get);
      mediaType = mediaType1; charset = charset1;
    }
    ready = true;
  }

  /**
   * Return the response code. Returns None if this is not an HTTP response
   * record, or there was a failure in parsing.
   */
  def getStatusCode : Option[Int] = {
    if (!ready) cueUp;
    return statusCode;
  }

  def getMediaType : String = {
    if (!ready) cueUp;
    return "%s/%s".format(getMediaTopType, getMediaSubType);
  }
  
  def getMediaTopType : String = {
    if (!ready) cueUp;
    return mediaType.map(_._1).getOrElse("application")
  }

  def getMediaSubType : String = {
    if (!ready) cueUp;
    return mediaType.map(_._2).getOrElse("octet-stream")
  }

  def getCharset : Option[String] = {
    if (!ready) cueUp;
    return charset;
  }
    
  /**
   * Return true if this is an HTTP response record.
   */
  def isHttpResponse : Boolean = {
    if (!ready) cueUp;
    return httpResponse;
  }

  def getUrl = rec.getHeader.getUrl;

  def getLength = rec.getHeader.getLength;

  def getDate = rec.getHeader.getDate;

  def getDigestStr = rec.getDigestStr;

  def getFilename = filename;

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

object ArchiveRecordWrapper {
  val MIME_RE = new Regex("""(application|audio|image|text|video)/([a-zA-Z0-9-]+)""");

  val headerValueParser = new BasicHeaderValueParser;
  
  /**
   * Parse a Content-Type header.
   *
   * @return The optional media type, as a pair (type, subtype), and an optional charset
   */
  def parseContentType (line : String) : Pair[Option[Pair[String,String]], Option[String]] = {
    try {
      val buff = new CharArrayBuffer(80);
      buff.append(line);
      val parsed = headerValueParser.parseElements(buff, new ParserCursor(0, buff.length));
      val mediaType = parsed(0).getName match {
        case null => None;
        case MIME_RE(topType, subType) => Some(Pair(topType, subType));
        case _ => None;
      }
      val charset = parsed(0).getParameterByName("charset") match {
        case null => None;
        case p : NameValuePair => Some(p.getValue);
      }
      return (mediaType, charset);
    } catch {
      case ex : Exception => (None, None);
    }
  }
}
