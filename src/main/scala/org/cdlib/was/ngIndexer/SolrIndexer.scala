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
          processor.processFileAsDocs(new File(path)) { (url, doc)=>
            if (!url.startsWith("filedesc:") && !url.startsWith("dns:")) {
              doc.setField(ARCNAME_FIELD, new File(path).getName);
              doc.setField(JOB_FIELD, job);
              doc.setField(SPECIFICATION_FIELD, specification);
              doc.setField(PROJECT_FIELD, project);
              val id = doc.getField(ID_FIELD).getValue.asInstanceOf[String];
              server.getById(id) match {
                case None => server.add(doc);
                case Some(olddoc) => {
                  val oldinputdoc = processor.doc2InputDoc(olddoc);
                  val mergedDoc = processor.mergeDocs(oldinputdoc, doc);
                  server.deleteById(id);
                  server.add(mergedDoc);
                }
              }
            }
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
