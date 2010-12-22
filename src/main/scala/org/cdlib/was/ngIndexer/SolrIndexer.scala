package org.cdlib.was.ngIndexer;

import java.io.{File,FileNotFoundException,IOException};
import org.cdlib.ssconf.Configurator;

object SolrIndexer {
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
    if (args.size < 1) {
      System.err.println("Please supply >= one arg!");
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
          case ex : IOException => ex.printStackTrace();
        }
      }
    }
  }
}
