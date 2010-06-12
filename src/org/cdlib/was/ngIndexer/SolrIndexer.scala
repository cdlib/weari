package org.cdlib.was.ngIndexer;

import java.io._;
import java.lang.Math;
import java.util.ArrayList;
import java.util.regex._;
import org.apache.lucene.analysis._;
import org.apache.lucene.analysis.standard._;
import org.apache.lucene.document._;
import org.apache.lucene.index._;
import org.apache.lucene.store._;
import org.apache.lucene.util._;
import org.apache.nutch.analysis._;
import org.apache.solr.client.solrj._;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common._;
import org.apache.tika.metadata._;
import org.apache.tika.parser._;
import org.apache.tika.sax._;
import org.archive.io._;
import org.archive.io.arc._;
import org.archive.net.UURIFactory;
import org.xml.sax.ContentHandler;
import scala.collection.mutable._;

class SolrIndexer (server : SolrServer) {
  
  def this(url : String) = this(new StreamingUpdateSolrServer(url, 50, 5));

  val dateFormatter = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS");
  val parser : Parser = new AutoDetectParser();
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

  def commit = {
    server.commit;
    server match {
      case s : StreamingUpdateSolrServer => s.blockUntilFinished;
    }
  }
  
  def index (archiveRecord : ArchiveRecord) {
    archiveRecord match {
      case rec : ARCRecord => {
        Utility.skipHttpHeader(rec);
        val tikaMetadata = new Metadata();
        val parseContext = new ParseContext();
        val url = rec.getMetaData.getUrl;
        val contentType = rec.getMetaData.getMimetype;
        tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
        tikaMetadata.set(HttpHeaders.CONTENT_TYPE, contentType);
        if (!url.startsWith("filedesc:") && !url.startsWith("dns:")) {
          System.err.println("Indexing %s".format(url));
          val doc = new SolrInputDocument();
          val indexContentHandler = new NgIndexerContentHandler(rec.getHeader.getLength  >= 1048576);
          val wgContentHandler = new WebGraphContentHandler(url, rec.getHeader.getDate);
          val contentHandler = new MultiContentHander(List[ContentHandler](wgContentHandler, indexContentHandler));
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
            indexContentHandler.contentString.map(str=>doc.addField("content", str));
            mdHandler(rec, tikaMetadata, doc);
            /* finish webgraph */
            if (webGraphTypeRE.matcher(tikaMetadata.get(HttpHeaders.CONTENT_TYPE)).matches) {
              val outlinks = wgContentHandler.outlinks;
              if (outlinks.size > 0) {
                val outlinkFps = for (l <- outlinks) 
                                 yield UriUtils.fingerprint(l.to);
                for (fp <- outlinkFps.toList.removeDuplicates.sort((a,b)=>(a < b))) {
                  doc.addField("outlinks", fp);
                }
              }
            }
            server.add(doc);
          } catch {
            case ex : Exception => ex.printStackTrace(System.err);
          }
        }
      }
    }
  }

  def mdHandler (archiveRecord : ArchiveRecord, md : Metadata, doc : SolrInputDocument) {
    val recHeader = archiveRecord.getHeader;

    val title = md.get("title") match {
      case s : String => s
      case null => ""
    }
    val url = recHeader.getUrl;
    val digest = archiveRecord match {
      case rec : ARCRecord => rec.getDigestStr;
      case _               => ""
    }
    
    updateDoc(doc, 1.0f, url, recHeader.getDate.toLowerCase,
              title, recHeader.getMimetype.toLowerCase, recHeader.getLength, digest);
  }

  def updateDoc (doc : SolrInputDocument, boost : Float, url : String,
                 date : String, title : String, mediaType : String,
                 length : Long, digest : String) {
    
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;

    doc.addField(solrIndexer.ID_FIELD, "%s.%s".format(uuri.toString, digest));

    /* core fields */
    doc.setDocumentBoost(boost);
    doc.addField(solrIndexer.BOOST_FIELD, boost);
    doc.addField(solrIndexer.DIGEST_FIELD, digest);
    // no segment

    /* fields for index-basic plugin */
    doc.addField(solrIndexer.HOST_FIELD, host);
    doc.addField(solrIndexer.SITE_FIELD, host);
    doc.addField(solrIndexer.URL_FIELD, url, 1.0f);
    // doc.addField("content", ..., 1.0f);
    doc.addField(solrIndexer.TITLE_FIELD, title, 1.0f);
    // doc.add("cache", ..., 1.0f);
    doc.addField(solrIndexer.TSTAMP_FIELD, dateFormatter.format(new java.util.Date(System.currentTimeMillis())), 1.0f);
    
    /* fields for index-anchor plugin */
    // doc.addField("anchor", ..., 1.0f);

    /* fields for index-more plugin */
    doc.addField(solrIndexer.TYPE_FIELD, mediaType, 1.0f);
    doc.addField(solrIndexer.CONTENT_LENGTH_FIELD, length, 1.0f);
    // doc.add("lastModified", ..., 1.0f)
    doc.addField(solrIndexer.DATE_FIELD, date, 1.0f);

    /* fields for languageidentifier plugin */
    // doc.addField("lang", ..., 1.0f);

    /* my fields */
    doc.addField(solrIndexer.URLFP_FIELD, UriUtils.fingerprint(uuri));
    doc.addField(solrIndexer.CANONICALURL_FIELD, uuri.toString, 1.0f);

  }

  def doc2InputDoc (doc : SolrDocument) : SolrInputDocument = {
    val idoc = new SolrInputDocument();

    val boost = doc.getFirstValue(solrIndexer.BOOST_FIELD).asInstanceOf[Float];
    val date = doc.getFirstValue(solrIndexer.DATE_FIELD).asInstanceOf[String];
    val title = doc.getFirstValue(solrIndexer.TITLE_FIELD).asInstanceOf[String];
    val url = doc.getFirstValue(solrIndexer.URL_FIELD).asInstanceOf[String];
    val mediaType = doc.getFirstValue(solrIndexer.TYPE_FIELD).asInstanceOf[String];
    val length = doc.getFirstValue(solrIndexer.CONTENT_LENGTH_FIELD).asInstanceOf[Long];
    val digest = doc.getFirstValue(solrIndexer.DIGEST_FIELD).asInstanceOf[String];

    idoc.addField(solrIndexer.CONTENT_FIELD, doc.getFirstValue(solrIndexer.CONTENT_FIELD).asInstanceOf[String]);
    
    updateDoc(idoc, boost, url, date, title, mediaType, length, digest);
    return idoc;
  }

  def updateDocs (q : SolrQuery, f : (SolrDocument)=>SolrInputDocument) {
    val stream = new SolrDocumentStream(server, q);
    var i = 1;
    for (doc <- stream) {
      server.add(f(doc));
      i += 1; if ((i % 500) == 0) server.commit;
    }
    server.commit;
  }

  val MIN_BOOST = 0.1f;
  val MAX_BOOST = 10.0f;

  def updateBoosts (g : RankedWebGraph) = {
    var fp2boost = new scala.collection.mutable.HashMap[Long, Float]();
    val it = g.nodeIterator;
    while (it.hasNext) {
      it.next;
      fp2boost.update(UriUtils.fingerprint(it.url), it.boost);
    }
    def updateBoost (doc : SolrDocument) : SolrInputDocument = {
      val idoc = doc2InputDoc(doc);
      val urlfp = doc.getFirstValue(solrIndexer.URLFP_FIELD).asInstanceOf[Long];
      val boost1 = fp2boost.get(urlfp).getOrElse(doc.getFirstValue("boost").asInstanceOf[Float]);
      val boost = Math.min(MAX_BOOST, Math.max(MIN_BOOST, boost1));
      if (boost > 11.0f) throw new RuntimeException();
      idoc.setDocumentBoost(boost);
      idoc.removeField(solrIndexer.BOOST_FIELD);
      idoc.setField(solrIndexer.BOOST_FIELD, boost);
      idoc;
    }
    val q = new SolrQuery().setQuery("*:*").setRows(500);
    updateDocs(q, updateBoost);
  }      
  
  def indexFile (f : File) {
    if (f.isDirectory) {
      for (c <- f.listFiles) {
        indexFile(c);
      }
    } else if (f.getName.endsWith(".arc.gz")) {
      Utility.eachArc(f, index);
      commit;
    }
  }
}

object solrIndexer {

  var collection = "xxxxxxx";
  var segment    = "xxxxxxx";

  val BOOST_FIELD          = "boost";
  val CANONICALURL_FIELD   = "canonicalurl";
  val CONTENT_FIELD        = "content";
  val CONTENT_LENGTH_FIELD = "contentLength";
  val DATE_FIELD           = "date";
  val DIGEST_FIELD         = "digest";
  val HOST_FIELD           = "host";
  val ID_FIELD             = "id";
  val SITE_FIELD           = "site";
  val TITLE_FIELD          = "title";
  val TSTAMP_FIELD         = "tstamp";
  val TYPE_FIELD           = "type";
  val URLFP_FIELD          = "urlfp";
  val URL_FIELD            = "url";

  def main (args : Array[String]) {
    if (args.size < 2) {
      System.err.println("Please supply >= two arg!");
      System.exit(1);
    } else {
      collection = args(0);
      val indexer = new SolrIndexer("http://gales.cdlib.org:8983/solr");
      for (path <- args.drop(1)) {
        indexer.indexFile(new File(path));
      }
    }
  }
}
