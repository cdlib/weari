/* Copyright (c) 2011 The Regents of the University of California */
package org.cdlib.was.weari;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import org.apache.http.{HeaderElement,NameValuePair};
import org.apache.http.message.{BasicHeaderValueParser,ParserCursor};

import org.apache.http.util.CharArrayBuffer;

import org.cdlib.was.weari.Utility.null2option;

import scala.util.matching.Regex;

/**
 * Represents a content-type, including a media type and
 * encoding, as supplied by, e.g., the Content-Type header.
 */
@JsonIgnoreProperties(Array("mediaType", "mediaTypeGroup"))
case class ContentType (val top     : String,
                        val sub     : String,
                        val charset : Option[String]) {

  lazy val mediaType : String = "%s/%s".format(top, sub);

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
  
  override def toString = 
    charset.map("%s; charset=%s".format(mediaType, _)).getOrElse(mediaType);
}

object ContentType {
  val DEFAULT = ContentType("application", "octet-string", None);

  val MIME_RE = 
    new Regex("""(application|audio|image|text|video)/([a-zA-Z0-9\.-]+)""");

  val headerValueParser = new BasicHeaderValueParser;

  private def extractContentType (el : HeaderElement) : Option[String] =
    null2option(el.getParameterByName("charset")).map(_.getValue);
    
  private def extractMediaType (el : HeaderElement) : Option[Pair[String,String]] = {
    null2option(el.getName).flatMap { src =>
      src match {
        case MIME_RE(topType, subType) => Some((topType, subType));
        case _                         => None;
      }
    }
  }

  /**
   * Parse a Content-Type header.
   *
   * @return The optional media type, as a ContentType object.
   */
  def parse (line : String) : Option[ContentType] = {
    var buff = new CharArrayBuffer(80);
    buff.append(line);
    val cursor = new ParserCursor(0, buff.length);

    return for { parsed <- headerValueParser.parseElements(buff, cursor).headOption
                 (topType, subType) <- extractMediaType(parsed) }
           yield ContentType(topType, subType, extractContentType(parsed));
  }
  
  def forceParse (line : String) = parse(line).getOrElse(ContentType.DEFAULT);
}
