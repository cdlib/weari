package org.cdlib.was.ngIndexer;

import java.io.{ByteArrayInputStream,InputStream};
import java.util.Date;
import java.util.regex.Pattern

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.{Metadata, HttpHeaders};
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser};

import org.cdlib.was.ngIndexer.Utility.{elseIfNull,timeout};
import org.cdlib.was.ngIndexer.webgraph.WebGraphContentHandler;

import org.xml.sax.ContentHandler;


class MyParseResult(val content : String,
                    val mediaType : String,
                    val title : String,
                    val outlinks : Seq[Long]);

class MyParser {
  val parseContext = new ParseContext;
  val detector = (new TikaConfig).getMimeRepository;
  val parser = new AutoDetectParser(detector);
  parseContext.set(classOf[Parser], parser);

  /* regular expression to match against mime types which should have
     outlinks indexed */
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

  def parse (content : Array[Byte],
             mediaType : String,
             url : String,
             date : Date) : MyParseResult = {
    val bais = new ByteArrayInputStream(content);
    return parse(bais, mediaType, url, date);
  }

  def parse (content : InputStream,
             mediaType : String,
             url : String,
             date : Date) : MyParseResult = {
    val tikaMetadata = new Metadata;
    val indexContentHandler = new NgIndexerContentHandler(true);
    val wgContentHandler = new WebGraphContentHandler(url, date);
    val contentHandler = new 
      MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));
    tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
    tikaMetadata.set(HttpHeaders.CONTENT_TYPE, mediaType);
    timeout(30000) {
        parser.parse(content, contentHandler, tikaMetadata, parseContext);
    }
    val tikaMediaType =
      ArchiveRecordWrapper.parseContentType(tikaMetadata.get(HttpHeaders.CONTENT_TYPE));
    val realMediaType = tikaMediaType.get.mediaTypeString;
    /* finish webgraph */
    var outlinks : Seq[Long] = List[Long]();
    if (webGraphTypeRE.matcher(realMediaType).matches) {
      val outlinksRaw = wgContentHandler.outlinks;
      if (outlinksRaw.size > 0) {
        outlinks = (for (l <- outlinksRaw) 
                    yield UriUtils.fingerprint(l.to)).
                      toList.distinct.sortWith((a,b)=>(a < b));
      }
    }
    return new MyParseResult(content   = indexContentHandler.contentString.getOrElse(""),
                             mediaType = realMediaType,
                             title     = elseIfNull(tikaMetadata.get("title"), ""),
                             outlinks  = outlinks);
  }
}
