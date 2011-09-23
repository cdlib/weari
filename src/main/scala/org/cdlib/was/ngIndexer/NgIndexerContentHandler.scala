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
