package org.cdlib.was.ngIndexer;

import java.io._;
import java.lang.Math;
import java.lang.Math;
import org.apache.lucene.analysis._;
import org.apache.lucene.analysis.standard._;
import org.apache.lucene.document._;
import org.apache.lucene.index._;
import org.apache.lucene.store._;
import org.apache.lucene.util._;
import org.apache.nutch.analysis._;
import org.apache.tika.metadata._;
import org.apache.tika.parser._;
import org.apache.tika.sax._;
import org.archive.io._;
import org.archive.io.arc._;
import org.archive.net.UURIFactory;
import org.xml.sax.ContentHandler;
import java.util.regex._;
import scala.collection.mutable._;
import java.util.ArrayList;
import org.apache.solr.common._;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

class SolrIndexer (url : String,
                     metadataHandler : (ARCRecord, Metadata, SolrInputDocument)=>Unit) {
  val parser : Parser = new AutoDetectParser();
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

  val server = new CommonsHttpSolrServer(url);

  var docBuf = new ArrayList[SolrInputDocument]();
  
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
            metadataHandler(rec, tikaMetadata, doc);
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
            docBuf.add(doc);
          } catch {
            case ex : Exception => ex.printStackTrace(System.err);
          }
        }
      }
    }
    if (docBuf.size > 50) {
      addDocs;
    }
  }

  def addDocs {
    server.add(docBuf);
    server.commit;
    docBuf.clear;
  } 

  def finish {
    if (docBuf.size > 0) {
      addDocs;
    }
  }
}

object solrIndexer {
  val dateFormatter = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS");

  var collection = "xxxxxxx";
  var segment    = "xxxxxxx";

  val URLFP_FIELD = "urlfp";
  val URL_FIELD = "url";
  val CANONICALURL_FIELD = "canonicalurl";

  def mdHandler (archiveRecord : ArchiveRecord, md : Metadata, doc : SolrInputDocument) {
    val recHeader = archiveRecord.getHeader;

    val title = md.get("title") match {
      case s : String => s
      case null => ""
    }

    val url = recHeader.getUrl;
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;
    val date = recHeader.getDate.toLowerCase;

    doc.addField("id", "%s.%s".format(url, date));

    /* core fields */
    doc.addField("boost", 1.0f, 1.0f);
    // digest below
    // no segment
      
    /* fields for index-basic plugin */
    doc.addField("host", host, 1.0f);
    doc.addField("site", host, 1.0f);
    doc.addField(URL_FIELD, url, 1.0f);
    // doc.addField("content", ..., 1.0f);
    doc.addField("title", title, 1.0f);
    // doc.add("cache", ..., 1.0f);
    doc.addField("tstamp", dateFormatter.format(new java.util.Date(System.currentTimeMillis())), 1.0f);
    
    /* fields for index-anchor plugin */
    // doc.addField("anchor", ..., 1.0f);

    /* fields for index-more plugin */
    doc.addField("type", recHeader.getMimetype.toLowerCase, 1.0f);
    doc.addField("contentLength", recHeader.getLength, 1.0f);
    // doc.add("lastModified", ..., 1.0f)
    doc.addField("date", date, 1.0f);

    /* fields for languageidentifier plugin */
    // doc.addField("lang", ..., 1.0f);

    /* my fields */
    doc.addField(URLFP_FIELD, UriUtils.fingerprint(uuri));
    doc.addField(CANONICALURL_FIELD, uuri.toString, 1.0f);

    archiveRecord match {
      case rec : ARCRecord => {
        
        doc.addField("digest", String.format("sha1:%s", rec.getDigestStr));

        /* filename:false:true:no_norms */
        //doc.addField(new Field("filename", rec.getMetaData.getArcFile.getName, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

      }
      case _ => {}
    }
  }

  def main (args : Array[String]) : Unit = {
    if (args.size < 2) {
      System.err.println("Please supply >= two arg!");
      System.exit(1);
    } else {
      collection = args(0);
      val indexer = new SolrIndexer("http://localhost:8983/solr", mdHandler);
      for (path <- args.drop(1)) {
        Utility.eachArc(new File(path), indexer.index);
      }
      indexer.finish;
    }
  }
}

