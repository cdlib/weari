package org.cdlib.was.ngIndexer;

import org.apache.http.NameValuePair;
import org.apache.http.message.{BasicHeaderValueParser,ParserCursor};

import org.apache.http.util.CharArrayBuffer;

import scala.util.matching.Regex;

/**
 * Trait to represent a content-type, including a media type and
 * encoding, as supplied by, e.g., the Content-Type header.
 */
trait ContentType {
  def topMediaType : Option[String];

  lazy val topMediaTypeString = topMediaType.getOrElse("application");

  def subMediaType : Option[String];

  lazy val subMediaTypeString = subMediaType.getOrElse("octet-string");

  def charset : Option[String];

  lazy val mediaType : Option[String] = 
    if (topMediaType.isDefined && subMediaType.isDefined) {
      Some("%s/%s".format(topMediaType.get, subMediaType.get));
    } else {
      None;
    }
  
  lazy val mediaTypeString : String = 
    mediaType.getOrElse("application/octet-string");

  lazy val mediaTypeGroupString : Option[String] = topMediaTypeString match {
    case "audio" => Some("audio");
    case "video" => Some("video");
    case "image" => Some("image");
    case "application" => subMediaTypeString match {
      case "pdf"    => Some("pdf");
      case "zip"    => Some("compressed");
      case "x-gzip" => Some("compressed");
      case s if s.startsWith("ms") || s.startsWith("vnd.ms") =>
        Some("office");
      case _ => None;
    } 
    case "text" => subMediaTypeString match {
      case "html" => Some("html");
      case _ => None;
    }
    case _ => None;
  }
}

object ContentType {
  val MIME_RE = 
    new Regex("""(application|audio|image|text|video)/([a-zA-Z0-9\.-]+)""");

  val headerValueParser = new BasicHeaderValueParser;

  /**
   * Parse a Content-Type header.
   *
   * @return The optional media type, as a ContentType object.
   */
  def parse (line : String) : Option[ContentType] = {
    try {
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
      return Some(new ContentTypeImpl (mediaType.map(_._1), mediaType.map(_._2), charset));
    } catch {
      case ex : Exception => None;
    }
  }
}

class ContentTypeImpl (val topMediaType : Option[String],
                       val subMediaType : Option[String],
                       val charset      : Option[String])
  extends ContentType;
