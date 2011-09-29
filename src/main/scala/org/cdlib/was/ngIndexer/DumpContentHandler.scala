/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import org.xml.sax.{Attributes,ContentHandler,Locator};

class DumpContentHandler extends ContentHandler {
  
  def characters (ch : Array[Char], start : Int, length : Int) {
  }

  def endDocument {
  }
  
  def endElement (namespaceURI : String, localName : String, qName : String) {
  }
  
  def endPrefixMapping (prefix : String) {
  }

  def ignorableWhitespace(ch : Array[Char], start : Int, length : Int) {
  }
  
  def processingInstruction(target : String, data : String) {
  }

  def setDocumentLocator(locator : Locator) {
  }

  def skippedEntity(name : String) {
  }

  def startDocument() {
  }

  def startAElement(atts : Attributes) {
  }

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) {
  }
    
  def startPrefixMapping(prefix : String, uri : String) {
  }
  
  def close {
  }
}
