package org.cdlib.was.ngIndexer;

import org.xml.sax.{Attributes,ContentHandler,Locator};

class MultiContentHander (handlers : Seq[ContentHandler]) 
  extends ContentHandler {
  
  def eachHandler (f : ContentHandler=>Unit) {
    for (h <- handlers) {
      f(h);
    }
  }
  
  def characters (ch : Array[Char], start : Int, length : Int) {
    eachHandler { h =>
      h.characters(ch, start, length);
    }
  }

  def endDocument {
    eachHandler { h =>
      h.endDocument;
    }
  }
  
  def endElement (namespaceURI : String, localName : String, qName : String) {
    eachHandler { h =>
      h.endElement(namespaceURI, localName, qName);
    }
  }
  
  def endPrefixMapping (prefix : String) {
    eachHandler { h =>
      h.endPrefixMapping(prefix);
    }
  }

  def ignorableWhitespace(ch : Array[Char], start : Int, length : Int) {
    eachHandler { h =>
      h.ignorableWhitespace(ch, start, length);
    }
  }
  
  def processingInstruction(target : String, data : String) {
    eachHandler { h =>
      h.processingInstruction(target, data);
    }
  }

  def setDocumentLocator(locator : Locator) {
    eachHandler { h =>
      h.setDocumentLocator(locator);
    }
  }

  def skippedEntity(name : String) {
    eachHandler { h =>
      h.skippedEntity(name);
    }
  }

  def startDocument {
    eachHandler { h =>
      h.startDocument;
    }
  }

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) {
    eachHandler { h =>
      h.startElement(namespaceURI, localName, qName, atts);
    }
  }
    
  def startPrefixMapping(prefix : String, uri : String) {
    eachHandler { h =>
      h.startPrefixMapping(prefix, uri);
    }
  }
}
