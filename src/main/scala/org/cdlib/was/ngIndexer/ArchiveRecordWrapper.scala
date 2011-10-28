/* Copyright (c) 2011 The Regents of the University of California */

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

import org.archive.io.{ArchiveReaderFactory,ArchiveRecord,ArchiveRecordHeader};
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.{WARCConstants,WARCRecord};
import org.archive.util.ArchiveUtils;
import org.apache.http.message.BasicLineParser;

import org.cdlib.was.ngIndexer.Utility.string2date;

import scala.util.matching.Regex;

/**
 * A wrapper for ArchiveRecord objects to provide a more consistent
 * interface.
 */
class ArchiveRecordWrapper (rec : ArchiveRecord, filename : String) 
  extends InputStream with WASArchiveRecord {

  private var statusCode : Option[Int] = None;
  private var ready : Boolean = false;
  private var contentTypeStr : Option[String] = None;
  private var httpResponse : Boolean = false;
  private var contentType : Option[ContentType] = None;
  private var closed : Boolean = false;

  /**
   * Parse the headers from a WARCRecord.
   */
  def parseWarcHttpHeaders {
    val lineParser = new BasicLineParser;
    val firstLine = ArchiveRecordWrapper.readLine(rec);
    try {
      val statusLine = lineParser.parseStatusLine(firstLine, new ParserCursor(0, firstLine.length - 1));
      val headers = ArchiveRecordWrapper.readHeaderLines(rec).map(lineParser.parseHeader(_));
        val headerMap = headers.map {(h)=> h.getName.toLowerCase->h }.toMap;
      statusCode = Some(statusLine.getStatusCode);
      contentTypeStr = 
        headerMap.get(HttpHeaders.CONTENT_TYPE.toLowerCase).map(_.getValue);
    } catch {
      case ex : ParseException => {
        ex.printStackTrace(System.err);
      }
    }
  }
  
  /**
   * Cue up a record to its start, fill in httpResponse & contentType.
   */
  private def cueUp {
    if (((rec.getClass == classOf[WARCRecord]) &&
         (rec.getHeader.getMimetype == "application/http; msgtype=response")) ||
        ((rec.getClass == classOf[ARCRecord]) &&
         rec.asInstanceOf[ARCRecord].getMetaData.getUrl.startsWith("http:"))) {
           httpResponse = true;
           parseWarcHttpHeaders;
         }
    contentType = contentTypeStr.flatMap(ContentType.parse(_));
    ready = true;
  }

  /**
   * Return the response code. Returns -1 if this is not an HTTP response
   * record, or there was a failure in parsing.
   */
  def getStatusCode : Int = {
    if (!ready) cueUp;
    if (statusCode.isEmpty) {
      return -1;
    } else {
      return statusCode.get;
    }
  }

  lazy val getContentType : ContentType = {
    if (!ready) cueUp;
    contentType.getOrElse(ContentType.DEFAULT);
  }

  /**
   * Return true if this is an HTTP response record.
   */
  lazy val isHttpResponse : Boolean = {
    if (!ready) cueUp;
    httpResponse;
  }

  lazy val getUrl = rec.getHeader.getUrl;

  lazy val getLength = rec.getHeader.getLength;

  lazy val getDate : java.util.Date = rec.getHeader.getDate;

  val SHA1_RE = new Regex("""^sha1:([a-zA-Z0-9]+)$""");

  lazy val getDigestStr : Option[String] = {
    if (!ready) cueUp;
    rec match {
      case arc : ARCRecord => {
        if (!this.closed) {
            /* must wait until finished */
          None;
        }
        Some(rec.getDigestStr);
      }
      case warc : WARCRecord => {
        warc.getHeader.getHeaderValue(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST) match {
          case SHA1_RE(s) => Some(s);
          case _          => None;
        }
      }
    }
  }

  lazy val getFilename = filename;

  /* InputStream wrapper */
  override def available = { 
    if (!ready) cueUp;
    rec.available; 
  }
  
  override def close = {
    this.closed = true;
    rec.close;
  }

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
}
