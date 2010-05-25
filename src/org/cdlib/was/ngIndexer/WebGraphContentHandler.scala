package org.cdlib.was.ngIndexer;

import org.xml.sax._;
import scala.collection.mutable._;
import java.io._;

class WebGraphContentHandler (url : String, date : String)
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
      outlinks += new Outlink(url, outlinkTo, date, outlinkText.toString);
      inAnchorText = false;
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
          outlinkText = new StringWriter(262144);
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
