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

package org.cdlib.was.weari;

import java.io.InputStream;
import java.security.MessageDigest;

import org.apache.http.{Header,HeaderElement,NameValuePair};
import org.apache.http.ParseException;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.impl.io.{AbstractMessageParser,HttpResponseParser};
import org.apache.http.message.{BasicHeaderValueParser,HeaderValueParser,ParserCursor};
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.CharArrayBuffer;

import org.apache.tika.metadata.HttpHeaders;

import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.{WARCConstants,WARCRecord};
import org.archive.util.{ArchiveUtils,Base32};

import org.apache.http.message.BasicLineParser;

import org.cdlib.was.weari.Utility.{dumpStream,string2date};

import scala.util.matching.Regex;

import grizzled.slf4j.Logging;

/**
 * A wrapper for ArchiveRecord objects to provide a more consistent
 * interface.
 */
class ArchiveRecordWrapper (rec : ArchiveRecord, filename : String) 
  extends InputStream with WASArchiveRecord with Logging with ExceptionLogger {

  private var statusCode : Option[Int] = None;
  private var ready : Boolean = false;
  private var contentTypeStr : Option[String] = None;
  private var httpResponse : Boolean = false;
  private var contentType : Option[ContentType] = None;
  private var closed : Boolean = false;
  private var digest : Option[MessageDigest] = 
    rec match {
      case arc : ARCRecord =>
        Some(MessageDigest.getInstance("SHA-1"));
      case _ => None;
    }

  lazy val isRevisit : Option[Boolean] = {
    if (rec.getClass == classOf[ARCRecord]) {
      None;
    } else {
    if (!ready) cueUp;
      if (!isHttpResponse) {
        Some(false);
      } else {
        val header = rec.getHeader();
        val headerVal = header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE).toString;
        Some(headerVal == WARCConstants.REVISIT);
      }
    }
  }

  /**
   * Parse the headers from a WARCRecord.
   */
  def parseWarcHttpHeaders {
    val lineParser = new BasicLineParser;
    val firstLine = ArchiveRecordWrapper.readLine(rec);
    catchAndLogExceptions {
      val statusLine = lineParser.parseStatusLine(firstLine, new ParserCursor(0, firstLine.length - 1));
      val headers = ArchiveRecordWrapper.readHeaderLines(rec).map(lineParser.parseHeader(_));
        val headerMap = headers.map {(h)=> h.getName.toLowerCase->h }.toMap;
      statusCode = Some(statusLine.getStatusCode);
      contentTypeStr = 
        headerMap.get(HttpHeaders.CONTENT_TYPE.toLowerCase).map(_.getValue);
    }
  }
  
  /**
   * Cue up a record to its start, fill in httpResponse & contentType.
   */
  private def cueUp {
    if (((rec.getClass == classOf[WARCRecord]) &&
         (rec.getHeader.getMimetype == "application/http; msgtype=response")) ||
        ((rec.getClass == classOf[ARCRecord]) &&
         rec.asInstanceOf[ARCRecord].getMetaData.getUrl.startsWith("http"))) {
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
          throw new Exception("You must close the ARC record before getting the digest string!");
        }
        this.digest.map(dig=>Base32.encode(dig.digest));
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
    dumpStream(this);
    if (!this.closed) rec.close;
    this.closed = true;
  }

  override def mark (readlimit : Int) = {
    if (!ready) cueUp; 
    rec.mark(readlimit);
  }
  
  override def markSupported = rec.markSupported;

  override def read = {
    if (!ready) cueUp; 
    val retval = rec.read;
    digest.map(_.update(retval.asInstanceOf[Byte]));
    retval;
  }

  override def read (buff : Array[Byte]) = {
    if (!ready) cueUp; 
    val retval = rec.read(buff);
    if (retval > 0) digest.map(_.update(buff, 0, retval));
    retval;
  }

  override def read (buff : Array[Byte], off : Int, len : Int) = {
    if (!ready) cueUp; 
    val retval = rec.read(buff, off, len);
    if (retval > 0) digest.map(_.update(buff, off, retval));
    retval;
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
