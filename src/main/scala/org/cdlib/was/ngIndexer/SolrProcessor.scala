package org.cdlib.was.ngIndexer;

import java.io.{InputStream, File}

import java.lang.{Object=>JObject}

import java.util.{Collection=>JCollection}
import java.util.regex.Pattern

import org.apache.solr.common.{SolrDocument, SolrInputDocument}

import org.apache.tika.config.TikaConfig
import org.apache.tika.metadata.{Metadata, HttpHeaders}
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser}

import org.archive.io.ArchiveRecord
import org.archive.io.arc.ARCRecord
import org.archive.io.warc.WARCRecord
import org.archive.net.UURIFactory

import org.cdlib.was.ngIndexer.webgraph.WebGraphContentHandler

import org.slf4j.LoggerFactory

import org.xml.sax.ContentHandler

import scala.collection.JavaConversions.asScalaIterable;

object SolrProcessor {
  val ARCNAME_FIELD        = "arcname";
  val BOOST_FIELD          = "boost";
  val CANONICALURL_FIELD   = "canonicalurl";
  val CONTENT_FIELD        = "content";
  val CONTENT_LENGTH_FIELD = "contentLength";
  val DATE_FIELD           = "date";
  val DIGEST_FIELD         = "digest";
  val HOST_FIELD           = "host";
  val ID_FIELD             = "id";
  val JOB_FIELD            = "job";
  val PROJECT_FIELD        = "project";
  val SERVER_FIELD         = "server";
  val SITE_FIELD           = "site";
  val SPECIFICATION_FIELD  = "specification";
  val TITLE_FIELD          = "title";
  val TSTAMP_FIELD         = "tstamp";
  val TYPE_FIELD           = "type";
  val URLFP_FIELD          = "urlfp";
  val URL_FIELD            = "url";

  /* fields which have a single value */
  val SINGLE_VALUED_FIELDS = 
      List(CANONICALURL_FIELD,
           CONTENT_FIELD,
           CONTENT_LENGTH_FIELD,
           DIGEST_FIELD,
           HOST_FIELD,
           ID_FIELD, 
           SITE_FIELD,
           TITLE_FIELD,
           TSTAMP_FIELD,
           TYPE_FIELD,
           URLFP_FIELD,
           URL_FIELD);

  val MULTI_VALUED_FIELDS =
    List(ARCNAME_FIELD,
         DATE_FIELD,
         JOB_FIELD,
         PROJECT_FIELD,
         SPECIFICATION_FIELD);
}

/** Class for processing (W)ARC files into Solr documents.
  *
  * @author egh
  */
class SolrProcessor {
  import SolrProcessor._;

  val logger = LoggerFactory.getLogger(classOf[SolrProcessor]);

  val parser : Parser = new AutoDetectParser();
  /* date formatter for solr */
  val dateFormatter = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS");
  /* regular expression to match against mime types which should have
     outlinks indexed */
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

  /** Update the boost in a document.
    */
  def updateDocBoost (doc : SolrInputDocument,
                      boost : Float) {
    doc.setDocumentBoost(boost);
    doc.addField(BOOST_FIELD, boost);
  }
    
  /** Update the url & digest fields in a document.
    */
  def updateDocUrlDigest (doc : SolrInputDocument, 
                          url : String,
                          digest : String) {
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;
    doc.addField(ID_FIELD, "%s.%s".format(uuri.toString, digest));
    doc.addField(DIGEST_FIELD, digest);
    doc.addField(HOST_FIELD, host);
    doc.addField(SITE_FIELD, host);
    doc.addField(URL_FIELD, url, 1.0f);
    doc.addField(TSTAMP_FIELD, dateFormatter.format(new java.util.Date(System.currentTimeMillis())), 1.0f);
    doc.addField(URLFP_FIELD, UriUtils.fingerprint(uuri));
    doc.addField(CANONICALURL_FIELD, uuri.toString, 1.0f);
  }

  /** Turn an existing SolrDocument into a SolrInputDocument suitable
    * for sending back to solr.
    */
  def doc2InputDoc (doc : SolrDocument) : SolrInputDocument = {
    val idoc = new SolrInputDocument();
    for (fieldName <- SINGLE_VALUED_FIELDS) {
      idoc.addField(fieldName, doc.getFirstValue(fieldName));
    }
    for (fieldName <- MULTI_VALUED_FIELDS) {
      for (value <- doc.getFieldValues(fieldName)) {
        idoc.addField(fieldName, value);
      }
    }    
    return idoc;
  }

  val MIN_BOOST = 0.1f;
  val MAX_BOOST = 10.0f;

  /** Cue up a record to its start, & return the content type. */
  def readyRecord (archiveRecord : ArchiveRecord) : Option[String] = {
    archiveRecord match {
      case rec : WARCRecord => {
        Utility.parseHeaders(rec) match {
          case None => return None;
          case Some((responseCode, headers)) => {
            return Some((headers.get(HttpHeaders.CONTENT_TYPE.toLowerCase).get.getValue));
          }
        }
      }
      case rec : ARCRecord => {
        Utility.skipHttpHeader(rec);
        return Some(rec.getMetaData.getMimetype.toLowerCase);
      }
    }
  }

  /** Take an archive record & return a solr document.
    *
    */
  def record2doc(rec : ArchiveRecord) : Option[SolrInputDocument] = {
    val contentType = readyRecord(rec);
    if (contentType.isEmpty) { return None; }
    val tikaMetadata = new Metadata();
    val parseContext = new ParseContext();
    val recHeader = rec.getHeader;
    val url = recHeader.getUrl;
    val doc = new SolrInputDocument();
    val indexContentHandler = new NgIndexerContentHandler(rec.getHeader.getLength  >= 1048576);
    val wgContentHandler = new WebGraphContentHandler(url, rec.getHeader.getDate);
    val contentHandler = new MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));
    val detector = (new TikaConfig).getMimeRepository;
    val parser = new AutoDetectParser(detector);
    parseContext.set(classOf[Parser], parser);

    tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
    tikaMetadata.set(HttpHeaders.CONTENT_TYPE, contentType.get);
    try {
      try {
        parser.parse(rec, contentHandler, tikaMetadata, parseContext);
      } catch {
        case ex : Throwable => {
          logger.error("Error reading {}", rec.getHeader.getUrl, ex);
        }
      }
      /* finish index */
      rec.close;
      indexContentHandler.contentString.map(str=>doc.addField(CONTENT_FIELD, str));

      val title = tikaMetadata.get("title") match {
        case s : String => s
        case null => ""
      }
      updateDocBoost(doc, 1.0f);
      updateDocUrlDigest(doc, url, rec.getDigestStr);
      doc.addField(DATE_FIELD, recHeader.getDate.toLowerCase, 1.0f);
      doc.addField(TYPE_FIELD, tikaMetadata.get(HttpHeaders.CONTENT_TYPE), 1.0f);
      doc.addField(TITLE_FIELD, title, 1.0f);
      doc.addField(CONTENT_LENGTH_FIELD, recHeader.getLength, 1.0f);
      /* finish webgraph */
      if (webGraphTypeRE.matcher(tikaMetadata.get(HttpHeaders.CONTENT_TYPE)).matches) {
        val outlinks = wgContentHandler.outlinks;
        if (outlinks.size > 0) {
          val outlinkFps = for (l <- outlinks) 
                           yield UriUtils.fingerprint(l.to);
          for (fp <- outlinkFps.toList.distinct.sortWith((a,b)=>(a < b))) {
            doc.addField("outlinks", fp);
          }
        }
      }
      return Some(doc);
    } catch {
      case ex : Exception => ex.printStackTrace(System.err);
      return None;
    }
  }

  def record2id (archiveRecord : ArchiveRecord) : String = {
    archiveRecord match {
      case rec : ARCRecord => {
        Utility.skipHttpHeader(rec);
        val uuri = UURIFactory.getInstance(rec.getMetaData.getUrl);
        return "%s.%s".format(uuri.toString, rec.getDigestStr);
      }
    }
  }

  /** For each record in a file, call the function.
    */
  def processFile (file : File) (func : (SolrInputDocument) => Unit) {
    Utility.eachArcRecursive(file) { (rec)=>
      record2doc(rec).map(func);
    }
  }

  def processStream (arcName : String, stream : InputStream) 
                    (func : (SolrInputDocument) => Unit) {
    Utility.eachArc(stream, arcName, (rec)=>{
      record2doc(rec).map(func);
    });
  }

  /** Merge two documents into one, presuming they have the same id.
    * Multi-value fields are appended.
    */
  def mergeDocs (a : SolrInputDocument, b : SolrInputDocument) : SolrInputDocument = {
    val retval = new SolrInputDocument;
    if (a.getFieldValue(ID_FIELD) != b.getFieldValue(ID_FIELD)) {
      throw new Exception;
    } else {
      /* identical fields */
      for (fieldName <- SINGLE_VALUED_FIELDS) {
        retval.setField(fieldName, a.getFieldValue(fieldName));
      }
      /* fields to merge */
      for (fieldName <- MULTI_VALUED_FIELDS) {
        def emptyIfNull(xs : JCollection[JObject]) : List[JObject] = xs match {
          case null => List();
          case seq  => seq.toList;
        }
        val values = (emptyIfNull(a.getFieldValues(fieldName)) ++
                      emptyIfNull(b.getFieldValues(fieldName))).distinct;
        for (value <- values) {
          retval.addField(fieldName, value);
        }
      }
    }
    return retval;
  }

  /** Remove a single value from a document's field.
    */
  def removeFieldValue (doc : SolrInputDocument, key : String, value : Any) {
    val oldValues = doc.getFieldValues(key);
    doc.removeField(key);
    for (value <- oldValues.filter(_==value)) {
      doc.addField(key, value);
    }
  }

  // def updateBoosts (g : RankedWebGraph) = {
  //   var fp2boost = new scala.collection.mutable.HashMap[Long, Float]();
  //   val it = g.nodeIterator;
  //   while (it.hasNext) {
  //     it.next;
  //     fp2boost.update(UriUtils.fingerprint(it.url), it.boost);
  //   }
  //   def updateBoost (doc : SolrDocument) : SolrInputDocument = {
  //     val idoc = doc2InputDoc(doc);
  //     val urlfp = doc.getFirstValue(SolrIndexer.URLFP_FIELD).asInstanceOf[Long];
  //     val boost1 = fp2boost.get(urlfp).getOrElse(doc.getFirstValue("boost").asInstanceOf[Float]);
  //     val boost = Math.min(MAX_BOOST, Math.max(MIN_BOOST, boost1));
  //     if (boost > 11.0f) throw new RuntimeException();
  //     idoc.setDocumentBoost(boost);
  //     idoc.removeField(SolrIndexer.BOOST_FIELD);
  //     idoc.setField(SolrIndexer.BOOST_FIELD, boost);
  //     idoc;
  //   }
  //   val q = new SolrQuery().setQuery("*:*").setRows(500);
  //   updateDocs(q, updateBoost);
  // }
}
