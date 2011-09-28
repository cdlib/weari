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
 * Represents the result of a Tika parse.
 */
class MyParseResult(val content  : Option[String],
                    val contentType : ContentType,
                    val title    : Option[String],
                    val outlinks : Seq[Long]);

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

  def parse (input : Array[Byte], contentType : Option[String], 
             url : String, date : Date) : MyParseResult = 
    parse(new ByteArrayInputStream(input), contentType, url, date);

  def parse (input : InputStream,
             contentType : Option[String],
             url : String,
             date : Date) : MyParseResult = {
    val tikaMetadata = new Metadata;
    val indexContentHandler = new NgIndexerContentHandler(true);
    val wgContentHandler = new WebGraphContentHandler(url, date);
    val contentHandler = new 
      MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));
    tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
    contentType.map(str=>tikaMetadata.set(HttpHeaders.CONTENT_TYPE, str));

    catchAndLogExceptions {
      timeout(30000) {
        parser.parse(input, contentHandler, tikaMetadata, parseContext);
      }
    }
    val tmp = ContentType.parse(tikaMetadata.get(HttpHeaders.CONTENT_TYPE)).getOrElse(ContentType.DEFAULT);
    val tikaMediaType =
      ContentType(tmp.topMediaType, tmp.subMediaType,
                  null2option(tikaMetadata.get(HttpHeaders.CONTENT_ENCODING)));

    /* finish webgraph */
    var outlinks : Seq[Long] = List[Long]();
    if (webGraphTypeRE.matcher(tikaMediaType.mediaType).matches) {
      val outlinksRaw = wgContentHandler.outlinks;
      if (outlinksRaw.size > 0) {
        outlinks = (for (l <- outlinksRaw) 
                    yield UriUtils.fingerprint(l.to)).
                      toList.distinct.sortWith((a,b)=>(a < b));
      }
    }
    return new MyParseResult(content      = indexContentHandler.contentString(maxSize),
                             contentType  = tikaMediaType,
                             title        = null2option(tikaMetadata.get("title")),
                             outlinks     = outlinks);
  }
}
