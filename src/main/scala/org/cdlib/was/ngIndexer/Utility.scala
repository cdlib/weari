package org.cdlib.was.ngIndexer;

import java.io.{BufferedInputStream,File,InputStream,FileOutputStream,OutputStream};

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

import scala.util.matching.Regex;

object Utility {
  /**
   *
   * Read a set of bytes from an input stream and pass them on to a function.
   *
   * @param stream The InputStream to return from.
   * @param f a function taking two arguments, an Int and Array[byte]. Used for side effects.
   * @returns Unit
   * @author egh
   */
  def readBytes (stream : InputStream, f : (Int, Array[Byte]) => Unit) : Unit = {
    var buffer = new Array[Byte](1024);
    var bytesRead = stream.read(buffer);
    while (bytesRead != -1)  {
      f (bytesRead, buffer);
      bytesRead = stream.read(buffer);
    }
  }

  /**
   *
   * Open an OutputStream on a File, ensuring that the stream is closed.
   *
   * @param file The File to open.
   * @param f a function taking two arguments, an Int and Array[byte]. Used for side effects.
   * @returns Unit
   * @author egh
   */
  def withFileOutputStream[A] (file : File, f : OutputStream => A) : A = {
    val out = new FileOutputStream(file);
    try {
      f (out);
    } finally {
      out.close();
    }
  }

  def readStreamIntoFile (file : File, in : InputStream) = {
    withFileOutputStream (file, (out) =>
      readBytes(in, (bytesRead, buffer) =>
        out.write(buffer, 0, bytesRead)));
  }

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

  def readLine (is : InputStream) : CharArrayBuffer = {
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

  def readHeaderLines (is : InputStream) : Seq[CharArrayBuffer] = {
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
}
