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

/** Class for processing (W)ARC files into Solr documents.
  *
  * @author egh
  */
class SolrProcessor {
  val parser : Parser = new AutoDetectParser();
  val dateFormatter = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS");
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");

//   // def updateDocs (q : SolrQuery, f : (SolrDocument)=>SolrInputDocument) {
//   //   var i = 1;
//   //   for (doc <- new SolrDocumentCollection(server, q)) {
//   //     server.add(f(doc));
//   //     i += 1; if ((i % 500) == 0) server.commit;
//   //   }
//   //   server.commit;
//   // }

  def updateDoc (doc : SolrInputDocument, 
                 boost : Float, 
                 url : String,
                 date : String, 
                 title : String, 
                 mediaType : String,
                 length : Long,
                 digest : String) {
    
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;

    doc.addField(SolrIndexer.ID_FIELD, "%s.%s".format(uuri.toString, digest));

    /* core fields */
    doc.setDocumentBoost(boost);
    doc.addField(SolrIndexer.BOOST_FIELD, boost);
    doc.addField(SolrIndexer.DIGEST_FIELD, digest);
    // no segment

    /* fields for index-basic plugin */
    doc.addField(SolrIndexer.HOST_FIELD, host);
    doc.addField(SolrIndexer.SITE_FIELD, host);
    doc.addField(SolrIndexer.URL_FIELD, url, 1.0f);
    // doc.addField("content", ..., 1.0f);
    doc.addField(SolrIndexer.TITLE_FIELD, title, 1.0f);
    // doc.add("cache", ..., 1.0f);
    doc.addField(SolrIndexer.TSTAMP_FIELD, dateFormatter.format(new java.util.Date(System.currentTimeMillis())), 1.0f);
    
    /* fields for index-anchor plugin */
    // doc.addField("anchor", ..., 1.0f);

    /* fields for index-more plugin */
    doc.addField(SolrIndexer.TYPE_FIELD, mediaType, 1.0f);
    doc.addField(SolrIndexer.CONTENT_LENGTH_FIELD, length, 1.0f);
    // doc.add("lastModified", ..., 1.0f)
    doc.addField(SolrIndexer.DATE_FIELD, date, 1.0f);

    /* fields for languageidentifier plugin */
    // doc.addField("lang", ..., 1.0f);

    /* my fields */
    doc.addField(SolrIndexer.URLFP_FIELD, UriUtils.fingerprint(uuri));
    doc.addField(SolrIndexer.CANONICALURL_FIELD, uuri.toString, 1.0f);
  }

  def doc2InputDoc (doc : SolrDocument) : SolrInputDocument = {
    val idoc = new SolrInputDocument();

    val boost = doc.getFirstValue(SolrIndexer.BOOST_FIELD).asInstanceOf[Float];
    val date = doc.getFirstValue(SolrIndexer.DATE_FIELD).asInstanceOf[String];
    val title = doc.getFirstValue(SolrIndexer.TITLE_FIELD).asInstanceOf[String];
    val url = doc.getFirstValue(SolrIndexer.URL_FIELD).asInstanceOf[String];
    val mediaType = doc.getFirstValue(SolrIndexer.TYPE_FIELD).asInstanceOf[String];
    val length = doc.getFirstValue(SolrIndexer.CONTENT_LENGTH_FIELD).asInstanceOf[Long];
    val digest = doc.getFirstValue(SolrIndexer.DIGEST_FIELD).asInstanceOf[String];

    idoc.addField(SolrIndexer.CONTENT_FIELD, doc.getFirstValue(SolrIndexer.CONTENT_FIELD).asInstanceOf[String]);
    
    updateDoc(idoc, boost, url, date, title, mediaType, length, digest);
    return idoc;
  }

  val MIN_BOOST = 0.1f;
  val MAX_BOOST = 10.0f;

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

  /** Take an archive record & return a solr document.
    *
    */
  def record2doc(archiveRecord : ArchiveRecord) : Option[SolrInputDocument] = {
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
        } else {
          return None;
        }
      }
    }
  }

  /** For each record in a file, call the function.
    */
  def processFile (file : File) (func : (SolrInputDocument) => Unit) {
    if (file.isDirectory) {
      for (c <- file.listFiles) {
        processFile(c)(func);
      }
    } else if (file.getName.endsWith(".arc.gz")) {
      Utility.eachArc(file, (rec)=>record2doc(rec).map(func));
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
