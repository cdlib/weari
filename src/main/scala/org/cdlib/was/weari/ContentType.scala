/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari;

import org.apache.http.{HeaderElement,NameValuePair};
import org.apache.http.message.{BasicHeaderValueParser,ParserCursor};

import org.apache.http.util.CharArrayBuffer;

import org.cdlib.was.weari.Utility.null2option;

import scala.util.matching.Regex;

/**
 * Represents a content-type, including a media type and
 * encoding, as supplied by, e.g., the Content-Type header.
 */
case class ContentType (val top     : String,
                        val sub     : String,
                        val charset : Option[String]) {

  lazy val mediaType : String = "%s/%s".format(top, sub);

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

/**
 * Class for turning a ContentType into a group.
 */
class MediaTypeGroup(c : ContentType) {
  def mediaTypeGroup = c match {
    case ContentType ("audio", _, _) => 
      Some("audio");
    case ContentType ("video", _, _) => 
      Some("video");
    case ContentType ("image", _, _) => 
      Some("image");
    case ContentType ("application", "pdf", _) => 
      Some("pdf");
    case ContentType ("application", "zip", _) => 
      Some("compressed");
    case ContentType ("application", "x-gzip", _) => 
      Some("compressed");
    case ContentType ("application", s, _) if s.startsWith("ms") || s.startsWith("vnd.ms") =>
      Some("office");
    case ContentType ("application", _, _) => 
      None;
    case ContentType ("text", "html", _) => 
      Some("html");
    case ContentType (_, _, _) => 
      None;
  }
}

object MediaTypeGroup {
  implicit def groupWrapper (c : ContentType) : MediaTypeGroup = 
    new MediaTypeGroup(c);
}

