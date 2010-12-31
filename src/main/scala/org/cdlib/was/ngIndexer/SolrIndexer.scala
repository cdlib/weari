package org.cdlib.was.ngIndexer;

import java.io.{File,FileNotFoundException,IOException};
import org.cdlib.ssconf.Configurator;

object SolrIndexer {
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

  def main (args : Array[String]) {
    val configPath = System.getProperty("org.cdlib.was.ngIndexer.ConfigFile");
    if (configPath == null) {
      System.err.println("Please define org.cdlib.was.ngIndexer.ConfigFile!");
      System.exit(1);
    }
    val configurator = new Configurator;
    val config : Config = 
      configurator.loadSimple(configPath, classOf[Config]);
    if (args.size < 3) {
      System.err.println("Please supply >= 3 args!");
      System.exit(1);
    } else {
      val job = args(0);
      val specification = args(1);
      val project = args(2)
      for (path <- args.drop(3)) {
        try {
          val server = new SolrDistributedServer(config.indexers());
          val processor = new SolrProcessor;
          processor.processFile(new File(path)) { (doc)=>
            doc.setField(ARCNAME_FIELD, new File(path).getName);
            doc.setField(JOB_FIELD, job);
            doc.setField(SPECIFICATION_FIELD, specification);
            doc.setField(PROJECT_FIELD, project);
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
