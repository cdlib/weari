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

import java.io.{ BufferedInputStream, File, InputStream, IOException, FileInputStream, FileOutputStream, OutputStream};
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
      case ex : IOException => return false;
    }
  }
}
