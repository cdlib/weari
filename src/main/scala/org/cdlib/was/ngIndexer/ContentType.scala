/* Copyright (c) 2011 The Regents of the University of California */
package org.cdlib.was.ngIndexer;

import net.liftweb.json.{Formats,JField,JObject,JString,JValue,MappingException,Serializer,TypeInfo};

import org.apache.http.NameValuePair;
import org.apache.http.message.{BasicHeaderValueParser,ParserCursor};

import org.apache.http.util.CharArrayBuffer;

import scala.util.matching.Regex;

/**
 * Trait to represent a content-type, including a media type and
 * encoding, as supplied by, e.g., the Content-Type header.
 */
case class ContentType (val top     : String,
                        val sub     : String,
                        val charset : Option[String]) {

  lazy val mediaType : String = 
      "%s/%s".format(top, sub);

  lazy val mediaTypeGroup : Option[String] = top match {
    case "audio" => Some("audio");
    case "video" => Some("video");
    case "image" => Some("image");
    case "application" => sub match {
      case "pdf"    => Some("pdf");
      case "zip"    => Some("compressed");
      case "x-gzip" => Some("compressed");
      case s if s.startsWith("ms") || s.startsWith("vnd.ms") =>
        Some("office");
      case _ => None;
    } 
    case "text" => sub match {
      case "html" => Some("html");
      case _ => None;
    }
    case _ => None;
  }
  
  override def toString = charset match {
    case Some(cs) => "%s; charset=%s".format(mediaType, cs);
    case None     => mediaType;
  }

}

object ContentType {
  val DEFAULT = ContentType("application", "octet-string", None);

  val MIME_RE = 
    new Regex("""(application|audio|image|text|video)/([a-zA-Z0-9\.-]+)""");

  val headerValueParser = new BasicHeaderValueParser;

  /**
   * Parse a Content-Type header.
   *
   * @return The optional media type, as a ContentType object.
   */
  def parse (line : String) : Option[ContentType] = {
    val buff = new CharArrayBuffer(80);
    buff.append(line);
    val parsed = headerValueParser.parseElements(buff, new ParserCursor(0, buff.length));
    val mediaType = parsed(0).getName match {
      case null => None;
      case MIME_RE(topType, subType) => Some(Pair(topType, subType));
      case _ => None;
    }
    val charset = parsed(0).getParameterByName("charset") match {
      case null => None;
        case p : NameValuePair => Some(p.getValue);
    }
    if (mediaType.isEmpty) {
      return None;
    } else {
      return Some(ContentType (mediaType.get._1, mediaType.get._2, charset))
    }
  }
}
