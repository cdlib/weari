/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.{ByteArrayInputStream,InputStream};
import java.util.Date;
import java.util.regex.Pattern

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.{Metadata, HttpHeaders};
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser};

import org.cdlib.was.weari.Utility.{null2option};
import org.cdlib.was.weari.webgraph.WebGraphContentHandler;

import org.xml.sax.ContentHandler;

import grizzled.slf4j.Logging;

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
            error("Caught exception parsing %s in arc %s: {}".format(rec.getUrl, rec.getFilename), ex);
            None;
          }
        }
      }
      rec.close;
      if (rec.getDigestStr.isEmpty) {
        /* need to check now because the ARC needs to be closed before we can get it */
        throw new Exception("No digest string found.");
      } else {
        return Some(parsed.getOrElse(ParsedArchiveRecord(rec)));
      }
    }
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
      /* tika returns the charset wrong */
      val tikaMediaType = 
        ContentType.parse(tikaMetadata.get(HttpHeaders.CONTENT_TYPE)).map { t=>
          ContentType(t.top, t.sub,
                      null2option(tikaMetadata.get(HttpHeaders.CONTENT_ENCODING)))
      }

      /* finish webgraph */
      var outlinks : Seq[Long] = List[Long]();
      if (tikaMediaType.isDefined && 
          webGraphTypeRE.matcher(tikaMediaType.get.mediaType).matches) {
            val outlinksRaw = wgContentHandler.outlinks;
            if (outlinksRaw.size > 0) {
              outlinks = (for (l <- outlinksRaw) 
                          yield UriUtils.fingerprint(l.to)).
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
