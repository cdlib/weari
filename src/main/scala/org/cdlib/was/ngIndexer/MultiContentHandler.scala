/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import org.xml.sax.{Attributes,ContentHandler,Locator};

/** A proxy ContentHandler that passes everything off to multiple
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
