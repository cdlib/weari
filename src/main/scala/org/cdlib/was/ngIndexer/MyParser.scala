package org.cdlib.was.ngIndexer;

import java.io.{ByteArrayInputStream,InputStream};
import java.util.Date;
import java.util.regex.Pattern

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.{Metadata, HttpHeaders};
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser};

import org.cdlib.was.ngIndexer.Utility.{elseIfNull,null2option,timeout};
import org.cdlib.was.ngIndexer.webgraph.WebGraphContentHandler;

import org.xml.sax.ContentHandler;

/**
 * Represents the result of a Tika parse.
 */
class MyParseResult(val content  : Option[String],
                    topMediaType : Option[String],
                    subMediaType : Option[String],
                    charset      : Option[String],
                    val title    : Option[String],
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
    timeout(30000) {
      parser.parse(input, contentHandler, tikaMetadata, parseContext);
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
    return new MyParseResult(charset      = null2option(tikaMetadata.get(HttpHeaders.CONTENT_ENCODING)),
                             content      = indexContentHandler.contentString,
                             subMediaType = tikaMediaType.get.subMediaType,
                             topMediaType = tikaMediaType.get.topMediaType,
                             title        = null2option(tikaMetadata.get("title")),
                             outlinks     = outlinks);
  }
}
