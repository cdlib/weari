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

import java.io.{File,InputStream};
import java.net.URI;

import org.archive.io.{ArchiveReader}
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.arc.ARCReader;

import org.cdlib.was.weari.Utility.readStreamIntoFile;

import dispatch._, Defaults._

/**
 * Wrapper for an ArchiveReader.
 */
class ArchiveReaderWrapper (wrapped : ArchiveReader) 
    extends Iterable[ArchiveRecordWrapper] {

  wrapped match {
    case arcReader : ARCReader => {
      /* we parse headers ourselves, and bad headers sometimes make for bad reads */
      arcReader.setParseHttpHeaders(false);
    }
    case _ => ()
  }
  
  def iterator : Iterator[ArchiveRecordWrapper] = {
    return new Iterator[ArchiveRecordWrapper] {
      private val it = wrapped.iterator;

      def hasNext = it.hasNext;
      
      def next = new ArchiveRecordWrapper(it.next, wrapped.getFileName);
    }
  }
      
  def close = wrapped.close;
}

object ArchiveReaderFactoryWrapper {
  def get (f : File) =
    new ArchiveReaderWrapper (ArchiveReaderFactory.get(f));

  /**
    * Get from a URI. Either returns an error string or a Pair[File,
    * ArchiveReaderWrapper] containing the contents.
    */
  def get (arcname : String, uri : URI, tmpdir : File) : Either[Throwable, Pair[File,ArchiveReaderWrapper]] = {
    val tmpfile = new File(tmpdir, arcname);
    if (uri.getScheme == "jar") {
      /* for testing only */
      val url = new java.net.URL(uri.toString);
      val in = url.openStream;
      try {
        readStreamIntoFile(tmpfile, in);
      } finally {
        in.close;
      }
      Right(tmpfile, ArchiveReaderFactoryWrapper.get(tmpfile));
    } else {
      /* download to a temp file with the arc name */
      val resp = Http(url(uri.toString) > as.Response(_.getResponseBodyAsStream));
      resp.either.right.map(stream=>{
        readStreamIntoFile(tmpfile, stream);
        Pair(tmpfile, ArchiveReaderFactoryWrapper.get(tmpfile));
      }).apply;
    }
  }

  def get (id : String, is : InputStream, atFirstRecord : Boolean) =
    new ArchiveReaderWrapper (ArchiveReaderFactory.get(id, is, atFirstRecord));

  def get (id : String, is : InputStream) : ArchiveReaderWrapper =
    get (id, is, true);
    
}
