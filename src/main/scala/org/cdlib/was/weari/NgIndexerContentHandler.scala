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

import java.io.{CharArrayReader,CharArrayWriter,File,FileReader,FileWriter,Reader,StringReader,StringWriter,Writer};
import org.xml.sax.{Attributes,ContentHandler,Locator};

class NgIndexerContentHandler (useTempFile : Boolean)
  extends ContentHandler {
  
  def contentReader : Option[Reader] = 
    contentWriter match {
      case w : FileWriter      => tempFile.map(new FileReader(_));
      case w : CharArrayWriter => Some(new CharArrayReader(w.toCharArray));
      case w : StringWriter    => Some(new StringReader(w.toString));
    }

  /* maxSize param is not *strictly* followed */
  def reader2string (r : Reader, maxSize : Int) : String = {
    val BUF_SIZE = 65536;
    var buf = new Array[Char](BUF_SIZE);
    var sb  = new StringBuffer();
    var i = 0;
    var totalRead = 0;
    do {
      sb.append(buf, 0, i);
      i = r.read(buf, 0, buf.length);
      totalRead += i;
    } while (i != -1 && totalRead + BUF_SIZE <= maxSize);
    return sb.toString;
  }

  def contentString (maxSize : Int) : Option[String] = {
    val what = contentWriter match {
      /* replace all repeated whitespace with a single space */
      case s : StringWriter => Some(s.toString.replaceAll("""\s+""", " "));
      case w : Writer       => contentReader.map(reader2string(_, maxSize).replaceAll("""\s+""", " "));
    }
    /* we are done with the temp file, delete */
    tempFile.map(_.delete());
    what;
  }

  val tempFile = if (useTempFile) {
    val f = File.createTempFile("ng-indexer", "data");
    f.deleteOnExit;
    Some(f);
  } else {
    None
  }
  
  val contentWriter : Writer = 
    tempFile.map(new FileWriter(_)).getOrElse(new StringWriter(262144));

  val WHITESPACE = Array(' ');
  def addWhitespace {
    characters(WHITESPACE, 0, 1);
  }

  override def characters (ch : Array[Char], start : Int, length : Int) {
    contentWriter.write(ch, start, length);
  }

  def endDocument {
    contentWriter.close;
  }
  
  def endElement (namespaceURI : String, localName : String, qName : String) = ()
  
  def endPrefixMapping (prefix : String) = ()

  def ignorableWhitespace(ch : Array[Char], start : Int, length : Int) = ()
  
  def processingInstruction(target : String, data : String) = ()

  def setDocumentLocator(locator : Locator) = ()

  def skippedEntity(name : String) = ()

  def startDocument() = ()

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) = {
    localName.toLowerCase match {
      case "address" | "blockquote" | "br" | "center" | "dir" | "div" | "dl" |
           "fieldset" | "form" | "h1" | "h2" | "h3" | "h4" | "h5" | 
           "h6" | "hr" | "isindex" | "menu" | "noframes" | "noscript" |
           "ol" | "p" | "pre" | "table" | "ul" | "dd" | "dt" | 
           "frameset" | "li" |"tbody" | "td" | "tfoot" | "th" | "thead" | "tr" =>
             addWhitespace;
      case _ => ();
    }
  }
    
  def startPrefixMapping(prefix : String, uri : String) = ()
}
