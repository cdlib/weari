package org.cdlib.was.ngIndexer;

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

  def reader2string (r : Reader) : String = {
    var buf = new Array[Char](65536);
    var sb  = new StringBuffer();
    var i = 0;
    do {
      sb.append(buf, 0, i);
      i = r.read(buf, 0, buf.length);
    } while (i != -1);
    return sb.toString;
  }

  def contentString : Option[String] = 
    contentWriter match {
      /* replace all repeated whitespace with a single space */
      case s : StringWriter => Some(s.toString.replaceAll("""\s+""", " "));
      case w : Writer       => contentReader.map(reader2string(_).replaceAll("""\s+""", " "));
    }

  val tempFile = if (useTempFile) {
    Some(File.createTempFile("ng-indexer", "data"));
  } else {
    None
  }
  
  val contentWriter : Writer = tempFile match {
    case Some(f) => new FileWriter(f);
    case None    => new StringWriter(262144);
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

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) = ()
    
  def startPrefixMapping(prefix : String, uri : String) = ()
}
