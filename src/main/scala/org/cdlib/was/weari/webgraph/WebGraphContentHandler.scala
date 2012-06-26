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

package org.cdlib.was.weari.webgraph;

import java.io.StringWriter;
import java.util.Date;

import org.archive.util.ArchiveUtils;

import org.xml.sax.{Attributes,ContentHandler,Locator};

import scala.collection.mutable.ArrayBuffer;

class WebGraphContentHandler (url : String, date : Date)
  extends ContentHandler {
    
  var outlinks = new ArrayBuffer[Outlink]();
  
  var inAnchorText = false;
  var outlinkText = new StringWriter(262144);
  var outlinkTo = "";

  override def characters (ch : Array[Char], start : Int, length : Int) {
    if (inAnchorText) {
      outlinkText.write(ch, start, length);
    }
  }

  def endDocument = ()
  
  def endAElement {
    if (inAnchorText) {
      try {
        outlinks += new Outlink(url, 
                                outlinkTo,
                                date,
                                outlinkText.toString);
        inAnchorText = false;
      } catch {
        case ex : org.apache.commons.httpclient.URIException => {
          /* no big deal, we just got a bad URL */
        }
      }
    }
  }

  def endElement (namespaceURI : String, localName : String, qName : String) {
    localName match {
      case "a" => endAElement;
      case _   => {}
    }
  }
  
  def endPrefixMapping (prefix : String) = ()

  def ignorableWhitespace(ch : Array[Char], start : Int, length : Int) = ()
  
  def processingInstruction(target : String, data : String) = ()

  def setDocumentLocator(locator : Locator) = ()

  def skippedEntity(name : String) = ()

  def startDocument() = {
    outlinks = new ArrayBuffer[Outlink]();
  }

  def startAElement(atts : Attributes) {
    atts.getValue(atts.getIndex("href")) match {
      case null          => {}
      case href : String => {
        if (!href.startsWith("javascript") && !href.startsWith("mailto")) {
          inAnchorText = true;
          outlinkTo = href;
          outlinkText = new StringWriter(10);
        }
      }
    }
  }

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) {
    localName match {
      case "a" => startAElement(atts);
      case _   => {}
    }
  }
    
  def startPrefixMapping(prefix : String, uri : String) = ()
  
  def getLinks = outlinks;
}
