/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io.{ByteArrayInputStream,InputStream};
import java.util.Date;
import java.util.regex.Pattern

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.{Metadata, HttpHeaders};
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser};

import org.cdlib.was.ngIndexer.Utility.{null2option,timeout};
import org.cdlib.was.ngIndexer.webgraph.WebGraphContentHandler;

import org.xml.sax.ContentHandler;

/**
 * Used for parsing archive records.
 */
class MyParser extends Logger {
  /* return max size of content 1Mb */
  val maxSize = 100000;
  val parseContext = new ParseContext;
  val detector = (new TikaConfig).getMimeRepository;
  val parser = new AutoDetectParser(detector);
  parseContext.set(classOf[Parser], parser);

  /* regular expression to match against mime types which should have
     outlinks indexed */
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

  /**
   * Parse a WASArchiveRecord.
   */
  def parse (rec : WASArchiveRecord with InputStream) : ParsedArchiveRecord = {
    val url = rec.getUrl;
    val contentType = rec.getContentType;
    val date = rec.getDate;
    val tikaMetadata = new Metadata;
    val indexContentHandler = new NgIndexerContentHandler(true);
    val wgContentHandler = new WebGraphContentHandler(url, date);
    val contentHandler = new 
      MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));
    tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
    tikaMetadata.set(HttpHeaders.CONTENT_TYPE, contentType.mediaType);

    timeout(30000) {
      parser.parse(rec, contentHandler, tikaMetadata, parseContext);
    }
    /* tika returns the charset wrong */
    val tikaMediaType = 
      ContentType.parse(tikaMetadata.get(HttpHeaders.CONTENT_TYPE)).map { t=>
        ContentType(t.top, t.sub,
                    null2option(tikaMetadata.get(HttpHeaders.CONTENT_ENCODING)))
    }

    /* finish webgraph */
    var outlinks : Seq[Long] = List[Long]();
    if (tikaMediaType.isDefined && webGraphTypeRE.matcher(tikaMediaType.get.mediaType).matches) {
      val outlinksRaw = wgContentHandler.outlinks;
      if (outlinksRaw.size > 0) {
        outlinks = (for (l <- outlinksRaw) 
                    yield UriUtils.fingerprint(l.to)).
                      toList.distinct.sortWith((a,b)=>(a < b));
      }
    }
    return ParsedArchiveRecord(rec,
                               indexContentHandler.contentString(maxSize),
                               tikaMediaType,
                               null2option(tikaMetadata.get("title")),
                               outlinks);
  }
}
