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
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common._;
import org.apache.tika.metadata._;
import org.apache.tika.parser._;
import org.apache.tika.sax._;
import org.archive.io._;
import org.archive.io.arc._;
import org.xml.sax.ContentHandler;
import scala.collection.mutable._;
import scala.io.Source;
import org.cdlib.ssconf.Configurator;

// class SolrIndexer (server : SolrDistributedServer) {
  
//   //def this(url : String) = this(new StreamingUpdateSolrServer(url, 50, 5));
//   def this(urls : Seq[Pair[String,Int]]) = this(new SolrDistributedServer(urls));
  
// }

object SolrIndexer {
  var segment    = "xxxxxxx";

  val BOOST_FIELD          = "boost";
  val CANONICALURL_FIELD   = "canonicalurl";
  val CONTENT_FIELD        = "content";
  val CONTENT_LENGTH_FIELD = "contentLength";
  val DATE_FIELD           = "date";
  val DIGEST_FIELD         = "digest";
  val HOST_FIELD           = "host";
  val ID_FIELD             = "id";
  val SERVER_FIELD         = "server";
  val SITE_FIELD           = "site";
  val TITLE_FIELD          = "title";
  val TSTAMP_FIELD         = "tstamp";
  val TYPE_FIELD           = "type";
  val URLFP_FIELD          = "urlfp";
  val URL_FIELD            = "url";

  def main (args : Array[String]) {
    val configPath = System.getProperty("org.cdlib.was.ngIndexer.ConfigFile");
    if (configPath == null) {
      System.err.println("Please define org.cdlib.was.ngIndexer.ConfigFile!");
      System.exit(1);
    }
    val config : Config = 
      (new Configurator).loadSimple(configPath, classOf[Config]);
    if (args.size < 2) {
      System.err.println("Please supply >= two arg!");
      System.exit(1);
    } else {
      for (path <- args) {
        try {
          val server = new SolrDistributedServer(config.indexers());
          val processor = new SolrProcessor;
          processor.processFile(new File(path)) {(doc)=>
            server.add(doc);
          }
          server.commit;
        } catch {
          case ex : FileNotFoundException => ex.printStackTrace();
          //case ex : NoSuchMethodException => ex.printStackTrace();
          case ex : IOException => ex.printStackTrace();
        }
      }
    }
  }
}
