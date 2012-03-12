/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.{BufferedInputStream,EOFException,File,InputStream,FileInputStream,FileOutputStream,OutputStream};
import java.util.{Collection=>JCollection,Date};
import java.util.zip.GZIPInputStream;

import org.archive.util.ArchiveUtils;

import scala.util.matching.Regex;
import scala.collection.JavaConversions.collectionAsScalaIterable;

object Utility {
  /* some of arcs end in .open or .gz.gz - we should fix */
  val ARC_RE = new Regex("""^.*?([A-Za-z0-9\.-]+arc\.gz)(?:\.open|\.gz)?$""");

  /**
   * Remove .open or extra .gz at end of arc file, and junk at beginning.
   * Returns None if we cannot extract an arcname.
   */
  def extractArcname(arcname : String) : Option[String] = arcname match {
    case ARC_RE(extracted) => Some(extracted);
    case _                 => None;
  }
    
  /**
   * Convert a possibly null thing into an Option version.
   */
  def null2option[A] (what : A) : Option[A] = {
    if (what == null) None;
    else Some(what);
  }

  /**
   * Convert a possibly null Java Collection into a scala Seq.
   * If the collection is null, returns an empty collection
   */
  def null2seq[A](collection : JCollection[A]) : Seq[A] = 
    null2option(collection).map(collectionAsScalaIterable(_)).getOrElse(List[A]()).toSeq;

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
}
