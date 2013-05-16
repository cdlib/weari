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

import java.io.{ByteArrayInputStream,InputStream};
import java.util.regex.Pattern

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.{Metadata, HttpHeaders};
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser};

import org.cdlib.was.weari.Utility.{null2option};
import org.cdlib.was.weari.webgraph.WebGraphContentHandler;

import org.xml.sax.ContentHandler;

import com.typesafe.scalalogging.slf4j.Logging;

/**
 * Used for parsing archive records.
 */
class MyParser extends Logging {
  /* max size, in bytes, of files to parse. If file is larger, do not parse */
  val MAX_PARSE_SIZE = 5000000;
  
  val parseContext = new ParseContext;
  val detector = (new TikaConfig).getMimeRepository;
  val parser = new AutoDetectParser(detector);
  parseContext.set(classOf[Parser], parser);

  /* regular expression to match against mime types which should have
     outlinks indexed */
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

  def safeParse(rec : WASArchiveRecord with InputStream) : 
      Option[ParsedArchiveRecord] = {
    if (!rec.isHttpResponse || (rec.getStatusCode != 200)) {
      rec.close;
      return None;
    } else {
      val parsed = if (rec.getLength > MAX_PARSE_SIZE) {
        None;
      } else {
        try {
          Some(parse(rec));
        } catch {
          case ex : TikaException => {
            logger.warn("Caught exception parsing %s in arc %s: {}".format(rec.getUrl, rec.getFilename), ex);
            None;
          }
          case ex : java.lang.StackOverflowError => {
            logger.warn("Caught StackOverflowError parsing %s in arc %s.".format(rec.getUrl, rec.getFilename));
            None;
          }
        }
      }
      rec.close;
      if (rec.getDigest.isEmpty) {
        /* need to check now because the ARC needs to be closed before we can get it */
        throw new Exception("No digest string found.");
      } else {
        return Some(parsed.getOrElse(ParsedArchiveRecord(rec)));
      }
    }
  }

  /**
   * Extract a ContentType from the metadata reported from Tika.
   */
  private def getTikaMediaType (md : Metadata) : Option[ContentType] = {
    val origMediaType = ContentType.parse(md.get(HttpHeaders.CONTENT_TYPE));
    /* tika returns the charset wrong */
    return origMediaType.map { t=>
      ContentType(t.top, t.sub,
                  null2option(md.get(HttpHeaders.CONTENT_ENCODING)))
    }
  }

  /**
   * Should we handle the outlinks of a given content type?
   */
  private def shouldHandleOutlinks (t : Option[ContentType]) : Boolean = t match {
    case Some(ContentType(_, "html", _)) | Some(ContentType("application", "pdf", _)) => 
      true;
    case _ => false;
  }

  /**
   * Parse a WASArchiveRecord.
   * Throws exceptions from tika.
   */
  def parse (rec : WASArchiveRecord with InputStream) : ParsedArchiveRecord = {
    try {
      val url = rec.getUrl;
      val contentType = rec.getContentType;
      val date = rec.getDate;
      val tikaMetadata = new Metadata;
      val indexContentHandler = new NgIndexerContentHandler(false);
      val wgContentHandler = new WebGraphContentHandler(url, date);
      val contentHandler = new 
      MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));
      tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
      tikaMetadata.set(HttpHeaders.CONTENT_TYPE, contentType.mediaType);
      
      parser.parse(rec, contentHandler, tikaMetadata, parseContext);
      val tikaMediaType = getTikaMediaType(tikaMetadata);

      /* finish webgraph */
      var outlinks : Seq[Long] = List[Long]();
      if (shouldHandleOutlinks(tikaMediaType)) {
        val outlinksRaw = wgContentHandler.outlinks;
        if (outlinksRaw.size > 0) {
          outlinks = (for (l <- outlinksRaw) 
                      yield UriUtils.fingerprint(UriUtils.canonicalize(l.to))).
          toList.distinct.sortWith((a,b)=>(a < b));
        }
      }

      /* we do this now to ensure that we get the digest string */
      rec.close;
      return ParsedArchiveRecord(rec,
                                 indexContentHandler.contentString(MAX_PARSE_SIZE),
                                 tikaMediaType,
                                 null2option(tikaMetadata.get("title")),
                                 outlinks);
    } finally {
      rec.close;
    }
  }
}
