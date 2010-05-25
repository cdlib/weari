package org.cdlib.was.ngIndexer;

import org.archive.io._;
import org.archive.io.arc._;
import java.io._;
import java.lang.Math;
import org.apache.lucene.index._;
import org.apache.lucene.document._;
import org.apache.lucene.store._;
import org.apache.lucene.analysis.standard._;
import org.apache.lucene.util._;
import org.xml.sax._;
import scala.collection.mutable._;

class NgIndexerContentHandler (size : Long, doc : Document, store : Boolean)
  extends ContentHandler {
  
  def this (size : Long, doc : Document) = this(size, doc, true);

  var outlinks = new ArrayBuffer[String]();

  val tempFile = if (!store && size >= 1048576) {
    Some(File.createTempFile("ng-indexer", "data"));
  } else {
    None
  }
  
  val contentWriter = tempFile match {
    case Some(f) => new FileWriter(f);
    case None    => if (store) { new StringWriter(262144); }
                    else {       new CharArrayWriter(262144); }
  }

  var contentReader : Option[Reader] = null;

  override def characters (ch : Array[Char], start : Int, length : Int) {
    contentWriter.write(ch, start, length);
  }

  def endDocument {
    contentWriter.close;
    if (store) {
      val c = contentWriter.toString;
      doc.add(new Field("content", c,
                        Field.Store.YES, 
                        Field.Index.ANALYZED));
    } else {
      contentReader = contentWriter match {
        case w : FileWriter => tempFile match {
          case Some (f) => Some(new FileReader(f));
          case None => None;
        }
        case w : CharArrayWriter => Some(new CharArrayReader(w.toCharArray));
      }
      contentReader.foreach((r)=>{ doc.add(new Field("content", r)) });
    }
  }
  
  def endElement (namespaceURI : String, localName : String, qName : String) = ()
  
  def endPrefixMapping (prefix : String) = ()

  def ignorableWhitespace(ch : Array[Char], start : Int, length : Int) = ()
  
  def processingInstruction(target : String, data : String) = ()

  def setDocumentLocator(locator : Locator) = ()

  def skippedEntity(name : String) = ()

  def startDocument() = ()

  def startAElement(atts : Attributes) {
    atts.getValue(atts.getIndex("href")) match {
      case null => {}
      case href : String =>
        outlinks += href;
    }
  }

  def startElement(namespaceURI : String, localName : String, qName : String, atts : Attributes) {
    localName match {
      case "a" => startAElement(atts);
      case _ => {
      }
    }
  }
    
  def startPrefixMapping(prefix : String, uri : String) = ()
}
