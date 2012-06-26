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

import org.xml.sax.{Attributes,ContentHandler,Locator};

/**
 * A proxy ContentHandler that passes everything off to multiple
 * child ContentHandlers.
 */
class MultiContentHander (handlers : Seq[ContentHandler]) 
  extends ContentHandler {
  
  def eachHandler(f : ContentHandler=>Unit) {
    for (h <- handlers) f(h);
  }
  
  def characters(ch : Array[Char], start : Int, length : Int) =
    eachHandler(_.characters(ch, start, length));

  def endDocument = eachHandler(_.endDocument);

  def endElement(namespaceURI : String, localName : String, qName : String) =
    eachHandler(_.endElement(namespaceURI, localName, qName));
  
  def endPrefixMapping(prefix : String) =
    eachHandler(_.endPrefixMapping(prefix));

  def ignorableWhitespace(ch : Array[Char], start : Int, length : Int) =
    eachHandler(_.ignorableWhitespace(ch, start, length));
  
  def processingInstruction(target : String, data : String) =
    eachHandler(_.processingInstruction(target, data));

  def setDocumentLocator(locator : Locator) =
    eachHandler(_.setDocumentLocator(locator));

  def skippedEntity(name : String) =
    eachHandler(_.skippedEntity(name));

  def startDocument =
    eachHandler(_.startDocument);

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) =
    eachHandler(_.startElement(namespaceURI, localName, qName, atts))

  def startPrefixMapping(prefix : String, uri : String) =
    eachHandler(_.startPrefixMapping(prefix, uri))
}
