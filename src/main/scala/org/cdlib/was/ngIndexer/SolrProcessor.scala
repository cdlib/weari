package org.cdlib.was.ngIndexer;

import java.io.File;
import java.util.regex.Pattern;
import org.archive.net.UURIFactory;
import org.archive.io.ArchiveRecord;
import org.apache.tika.metadata.Metadata;
import org.apache.solr.common.{SolrDocument,SolrInputDocument};
import org.archive.io.arc.ARCRecord;
import org.apache.tika.parser.{AutoDetectParser,ParseContext,Parser};
import org.apache.tika.metadata.HttpHeaders;
import org.xml.sax.ContentHandler;
import org.apache.solr.client.solrj.SolrQuery;
import org.cdlib.was.ngIndexer.webgraph.WebGraphContentHandler;

/** Class for processing (W)ARC files into Solr documents.
  *
  * @author egh
  */
class SolrProcessor {
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
    doc.addField(SolrIndexer.BOOST_FIELD, boost);
  }
    
  /** Update the url & digest fields in a document.
    */
  def updateDocUrlDigest (doc : SolrInputDocument, 
                          url : String,
                          digest : String) {
    
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;
    doc.addField(SolrIndexer.ID_FIELD, "%s.%s".format(uuri.toString, digest));
    doc.addField(SolrIndexer.DIGEST_FIELD, digest);
    doc.addField(SolrIndexer.HOST_FIELD, host);
    doc.addField(SolrIndexer.SITE_FIELD, host);
    doc.addField(SolrIndexer.URL_FIELD, url, 1.0f);
    doc.addField(SolrIndexer.TSTAMP_FIELD, dateFormatter.format(new java.util.Date(System.currentTimeMillis())), 1.0f);
    doc.addField(SolrIndexer.URLFP_FIELD, UriUtils.fingerprint(uuri));
    doc.addField(SolrIndexer.CANONICALURL_FIELD, uuri.toString, 1.0f);
  }

  /** Turn an existing SolrDocument into a SolrInputDocument suitable
    * for sending back to solr.
    */
  def doc2InputDoc (doc : SolrDocument) : SolrInputDocument = {
    val idoc = new SolrInputDocument();

    updateDocUrlDigest(idoc, 
                       doc.getFirstValue(SolrIndexer.URL_FIELD).asInstanceOf[String],
                       doc.getFirstValue(SolrIndexer.DIGEST_FIELD).asInstanceOf[String])
    updateDocBoost(idoc, doc.getFirstValue(SolrIndexer.BOOST_FIELD).asInstanceOf[Float]);
    idoc.addField(SolrIndexer.DATE_FIELD, 
                  doc.getFirstValue(SolrIndexer.DATE_FIELD).asInstanceOf[String], 1.0f);
    idoc.addField(SolrIndexer.TITLE_FIELD, 
                 doc.getFirstValue(SolrIndexer.TITLE_FIELD).asInstanceOf[String], 1.0f);
    idoc.addField(SolrIndexer.TYPE_FIELD, 
                  doc.getFirstValue(SolrIndexer.TYPE_FIELD).asInstanceOf[String], 1.0f);
    idoc.addField(SolrIndexer.CONTENT_LENGTH_FIELD,
                  doc.getFirstValue(SolrIndexer.CONTENT_LENGTH_FIELD).asInstanceOf[Long], 1.0f);
    idoc.addField(SolrIndexer.CONTENT_FIELD, doc.getFirstValue(SolrIndexer.CONTENT_FIELD).asInstanceOf[String]);
    
    return idoc;
  }

  val MIN_BOOST = 0.1f;
  val MAX_BOOST = 10.0f;

  /** Take an archive record & return a solr document.
    *
    */
  def record2doc(archiveRecord : ArchiveRecord) : Option[Pair[String,SolrInputDocument]] = {
    archiveRecord match {
      case rec : ARCRecord => {
        Utility.skipHttpHeader(rec);
        val tikaMetadata = new Metadata();
        val parseContext = new ParseContext();
        val url = rec.getMetaData.getUrl;
        val recHeader = rec.getHeader;
        val contentType = rec.getMetaData.getMimetype.toLowerCase;
        val doc = new SolrInputDocument();
        val indexContentHandler = new NgIndexerContentHandler(rec.getHeader.getLength  >= 1048576);
        val wgContentHandler = new WebGraphContentHandler(url, rec.getHeader.getDate);
        val contentHandler = new MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));

        tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
        tikaMetadata.set(HttpHeaders.CONTENT_TYPE, contentType);
        try {
          try {
            parser.parse(rec, contentHandler, tikaMetadata, parseContext);
          } catch {
            case ex : Throwable => {
              System.err.println(String.format("Error reading %s", rec.getHeader.getUrl));
              ex.printStackTrace(System.err);
            }
          }
          /* finish index */
          rec.close;
          indexContentHandler.contentString.map(str=>doc.addField(SolrIndexer.CONTENT_FIELD, str));

          val title = tikaMetadata.get("title") match {
            case s : String => s
            case null => ""
          }
          val digest = archiveRecord match {
            case rec : ARCRecord => rec.getDigestStr;
            case _               => ""
          }
          updateDocBoost(doc, 1.0f);
          updateDocUrlDigest(doc, url, digest);
          doc.addField(SolrIndexer.DATE_FIELD, recHeader.getDate.toLowerCase, 1.0f);
          doc.addField(SolrIndexer.TYPE_FIELD, tikaMetadata.get(HttpHeaders.CONTENT_TYPE), 1.0f);
          doc.addField(SolrIndexer.TITLE_FIELD, title, 1.0f);
          doc.addField(SolrIndexer.CONTENT_LENGTH_FIELD, recHeader.getLength, 1.0f);
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
          return Some((url, doc));
        } catch {
          case ex : Exception => ex.printStackTrace(System.err);
          return None;
        }
      }
    }
  }

  /** For each record in a file, call the function.
    */
  def processFile (file : File) (func : (String,SolrInputDocument) => Unit) {
    if (file.isDirectory) {
      for (c <- file.listFiles) {
        processFile(c)(func);
      }
    } else if (file.getName.indexOf("arc.gz") != -1) {
      Utility.eachArc(file, (rec)=>record2doc(rec).map((p)=>func(p._1, p._2)));
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
