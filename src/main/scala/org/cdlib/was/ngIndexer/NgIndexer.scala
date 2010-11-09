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
class NgIndexer (writer : IndexWriter, 
                 metadataHandler : (ARCRecord, Metadata, Document)=>Unit) {

  val parser : Parser = new AutoDetectParser();
  val webGraphTypeRE = Pattern.compile("^(.*html.*|application/pdf)$");
  //val webGraph = new CassandraWebGraph();
 
  def index (archiveRecord : ArchiveRecord) {
    archiveRecord match {
      case rec : ARCRecord  => {
        Utility.skipHttpHeader(rec);
        val tikaMetadata = new Metadata();
        val parseContext = new ParseContext();
        val url = rec.getMetaData.getUrl;
        val contentType = rec.getMetaData.getMimetype;
        tikaMetadata.set(HttpHeaders.CONTENT_LOCATION, url);
        tikaMetadata.set(HttpHeaders.CONTENT_TYPE, contentType);
        if (!url.startsWith("filedesc:") && !url.startsWith("dns:")) {
          val doc = new Document();
          val indexContentHandler = new NgIndexerContentHandler(rec.getHeader.getLength >= 1048576);
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
            doc.setBoost(1.0f);
            doc.add(new Field("boost", "1.0", Field.Store.YES, Field.Index.NO));
            indexContentHandler.contentString.map(str=>doc.add(new Field("content", str,
                                                                         Field.Store.YES, 
                                                                         Field.Index.ANALYZED)));
            metadataHandler(rec, tikaMetadata, doc);
            writer.addDocument(doc);

            /* finish webgraph */
            // if (webGraphTypeRE.matcher(tikaMetadata.get(HttpHeaders.CONTENT_TYPE)).matches) {
            //   val outlinks = wgContentHandler.getLinks;
            //   if (outlinks.size > 0) {
            //     webGraph.addLinks(outlinks);
            //   }
            // }
          } catch {
            case ex : Exception => ex.printStackTrace(System.err);
          }
        }
        if (false) { /* change false to true to get metadata printing */
          if (!tikaMetadata.get(HttpHeaders.CONTENT_TYPE).equals(contentType)) {
            System.err.println("Diff in content type, declared: %s detected: %s".format(tikaMetadata.get(HttpHeaders.CONTENT_TYPE), contentType));
          }
          System.err.println("metdata for: %s:".format(url));
          for (n <- tikaMetadata.names) {
            System.err.print("  %s:".format(n));
            for (v <- tikaMetadata.getValues(n)) {
              System.err.print(" %s".format(v));
            }
            System.err.println("");
          }
        }
        rec.close();
      }
    case _ => {}
    }
  }
}

object ngIndexer {
  val dateFormatter = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS");

  var collection = "xxxxxxx";
  var segment    = "xxxxxxx";

  def mdHandler (archiveRecord : ArchiveRecord, md : Metadata, doc : Document) {
    val recHeader = archiveRecord.getHeader;

    val title = md.get("title") match {
      case s : String => s
      case null => ""
    }

    /* title:false:true:tokenized */
    doc.add(new Field("title", title, Field.Store.YES, Field.Index.ANALYZED));

    /* collection:true:true:no_norms */
    doc.add(new Field("collection", collection, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

    doc.add(new Field("tstamp", dateFormatter.format(new java.util.Date(System.currentTimeMillis())), Field.Store.YES, Field.Index.NO));
    
    //doc.add(new Field("segment", segment, Field.Store.YES, Field.Index.NO));

    /* length:false:true:no */
    doc.add(new Field("length", recHeader.getLength.toString, Field.Store.YES, Field.Index.NO));

    /* type:true:true:no_norms */
    doc.add(new Field("type", recHeader.getMimetype.toLowerCase, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

    /* date:true:true:no_norms */
    doc.add(new Field("date", recHeader.getDate.toLowerCase, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

    /* fileoffset:false:true:no_norms */
    doc.add(new Field("fileoffset", String.valueOf(recHeader.getOffset), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

    /* url:false:true:untokenized:true:exacturl */
    doc.add(new Field("exacturl", recHeader.getUrl, Field.Store.YES, Field.Index.NOT_ANALYZED));
    
    /* url:false:true:tokenized */
    val url = recHeader.getUrl;
    val urlField = new Field("url", url, Field.Store.YES, Field.Index.ANALYZED);
    urlField.setTokenStream(new NutchDocumentTokenizer(new StringReader(url)));
    doc.add(urlField);
    
    val host = UURIFactory.getInstance(url).getHost;

    /* site:false:false:untokenized */
    doc.add(new Field("site", host, Field.Store.NO, Field.Index.NOT_ANALYZED));

    doc.add(new Field("host", new NutchDocumentTokenizer(new StringReader(host))));
    
    archiveRecord match {
      case rec : ARCRecord => {

        //site:false:false:untokenized
        /* digest:false:true:no */
        doc.add(new Field("digest", String.format("sha1:%s", rec.getDigestStr), Field.Store.YES, Field.Index.NO));

        /* filename:false:true:no_norms */
        doc.add(new Field("filename", rec.getMetaData.getArcFile.getName, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

      }
      case _ => {}
    }
  }

  def main (args : Array[String]) : Unit = {
    if (args.size < 2) {
      System.err.println("Please supply >= two arg!");
      System.exit(1);
    } else {
      val dir = new File(args(0));
      collection = args(1);
      val fsDir = FSDirectory.open(dir);
      val writer = 
        new IndexWriter(fsDir, 
                        new NutchyAnalyzer(Version.LUCENE_CURRENT), 
                        !dir.exists,
                        IndexWriter.MaxFieldLength.UNLIMITED);
      val indexer = new NgIndexer(writer, mdHandler);
      for (path <- args.drop(2)) {
        Utility.eachArc(new File(path), indexer.index);
      }
      writer.optimize;
      writer.close;
    }
  }
}

