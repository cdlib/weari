/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io.{BufferedInputStream,EOFException,File,InputStream,FileInputStream,FileOutputStream,OutputStream};
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.archive.util.ArchiveUtils;

import scala.util.matching.Regex;

object Utility {
  /* some of arcs end in .open or .gz.gz - we should fix */
  val ARC_RE = new Regex(""".*?([A-Za-z0-9\.-]+arc\.gz)(?:\.open|\.gz)?""");

  /**
   * Convert a possibly null thing into an Option version.
   */
  def null2option[A] (what : A) : Option[A] = {
    if (what == null) None;
    else Some(what);
  }

  def writeStreamToTempFile (prefix : String, in : InputStream) : File = { 
    val tempFile = File.createTempFile(prefix, null);
    readStreamIntoFile(tempFile, in);
    tempFile.deleteOnExit();
    return tempFile;
  }

  /**
   * Read a set of bytes from an input stream and pass them on to a function.
   *
   * @param stream The InputStream to return from.
   * @param f a function taking two arguments, an Int and Array[byte]. Used for side effects.
   * @return Unit
   */
  def readBytes (stream : InputStream) (f : (Int, Array[Byte]) => Unit) : Unit = {
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
   */
  def withFileOutputStream[A] (file : File) (f : OutputStream => A) : A = {
    val out = new FileOutputStream(file);
    try {
      f (out);
    } finally {
      out.close();
    }
  }

  def withFileInputStream[A] (file : File) (f : InputStream => A) : A = {
    val in = new FileInputStream(file)
    try {
      f(in);
    } finally {
      in.close();
    }
  }

  def readStreamIntoFile (file : File, in : InputStream) = {
    withFileOutputStream (file) { out =>
      readBytes(in) { (bytesRead, buffer) =>
        out.write(buffer, 0, bytesRead);
      }
    }
  }

  def flushStream (in : InputStream, out : OutputStream) : Unit = {
    readBytes(in) { (bytesRead, buffer) =>
      out.write(buffer, 0, bytesRead);
    }
  }

  def dumpStream (in : InputStream) : Unit =
    readBytes(in) { (bytesRead, buffer) => () };

  val dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
  dateFormatter.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
  
  implicit def date2string (d : Date) : String =
    dateFormatter.format(d);
    
  implicit def string2date (s : String) : Date = s match {
    case ds : String if ds.length == 14 =>
      ArchiveUtils.parse14DigitDate(s);
    case ds : String if ds.length == 20 =>
      dateFormatter.parse(s);
  }
  
  /**
   * Check the integrity of a gzipped file.
   */
  def checkGzip (f : File) : Boolean = {
    try {
      withFileInputStream (f) { is =>
        val gis = new GZIPInputStream(is);
        dumpStream(gis);
        return true;
      }
    } catch {
      case ex : EOFException => return false;
    }
  }

  def timeout[T] (msec : Int) (f: => T) (finalBlock: => Unit) : Option[T] = {
    class TimeoutThread extends Thread {
      var retval : Option[T] = None;
      var ex : Option[Throwable] = None;
      override def run {
        try {
          retval = Some(f);
        } catch {
          /* finished, do nothing */ 
          case t : InterruptedException => ()
          case t : java.io.InterruptedIOException => ()
          case t : Throwable => {
            ex = Some(t);
          } 
        } finally {
          finalBlock;
        }
      }
    }
    val thread = new TimeoutThread;
    thread.start();
    var endTimeMillis = System.currentTimeMillis + msec;
    while (thread.isAlive) {
      if (System.currentTimeMillis > endTimeMillis) {
        thread.interrupt;
        thread.join;
      }
      try { Thread.sleep(50); }
      catch { case ex : InterruptedException => () }
    }
    if (thread.retval.isEmpty && thread.ex.isDefined) {
      throw new Exception("%s in timeout block.".format(thread.ex.get.toString), thread.ex.get);
    } else {
      return thread.retval;
    }
  }
}
