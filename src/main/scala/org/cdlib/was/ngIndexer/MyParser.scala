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

class MyParseResult(val content  : String,
                    topMediaType : Option[String],
                    subMediaType : Option[String],
                    charset      : Option[String],
                    val title    : String,
                    val outlinks : Seq[Long])
  extends ContentTypeImpl(topMediaType, subMediaType, charset);

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
             date : Date) : MyParseResult =
    parse(new ByteArrayInputStream(content), mediaType, url, date);

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
      ContentType.parse(tikaMetadata.get(HttpHeaders.CONTENT_TYPE));
    /* finish webgraph */
    var outlinks : Seq[Long] = List[Long]();
    if (webGraphTypeRE.matcher(tikaMediaType.get.mediaTypeString).matches) {
      val outlinksRaw = wgContentHandler.outlinks;
      if (outlinksRaw.size > 0) {
        outlinks = (for (l <- outlinksRaw) 
                    yield UriUtils.fingerprint(l.to)).
                      toList.distinct.sortWith((a,b)=>(a < b));
      }
    }
    val charset = tikaMetadata.get(HttpHeaders.CONTENT_ENCODING) match {
      case null => None;
      case s : String => Some(s);
    }
    return new MyParseResult(charset      = charset,
                             content      = indexContentHandler.contentString.getOrElse(""),
                             subMediaType = tikaMediaType.get.subMediaType,
                             topMediaType = tikaMediaType.get.topMediaType,
                             title        = elseIfNull(tikaMetadata.get("title"), ""),
                             outlinks     = outlinks);
  }
}
