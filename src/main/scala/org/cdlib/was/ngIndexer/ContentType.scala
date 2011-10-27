/* Copyright (c) 2011 The Regents of the University of California */
package org.cdlib.was.ngIndexer;

import net.liftweb.json.{Formats,JField,JObject,JString,JValue,MappingException,Serializer,TypeInfo};

import org.apache.http.NameValuePair;
import org.apache.http.message.{BasicHeaderValueParser,ParserCursor};

import org.apache.http.util.CharArrayBuffer;

import org.cdlib.was.ngIndexer.Utility.null2option;

import scala.util.matching.Regex;

/**
 * Represents a content-type, including a media type and
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
    if (parsed.isEmpty) {
      return None;
    } else {
      val mediaType = null2option(parsed(0).getName) match {
        case Some(MIME_RE(topType, subType)) => Some(Pair(topType, subType));
        case _ => None;
      }
      val charset = null2option(parsed(0).getParameterByName("charset")).map(_.getValue);
      if (mediaType.isEmpty) {
        return None;
      } else {
        return Some(ContentType (mediaType.get._1, mediaType.get._2, charset))
      }
    }
  }
  
  def forceParse (line : String) = 
    parse(line).getOrElse(ContentType.DEFAULT);
}
