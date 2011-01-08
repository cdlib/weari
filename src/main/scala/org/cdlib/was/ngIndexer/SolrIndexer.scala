package org.cdlib.was.ngIndexer;

import java.io.{File,FileNotFoundException,IOException};
import org.cdlib.ssconf.Configurator;
import org.cdlib.was.ngIndexer.SolrProcessor.{ARCNAME_FIELD,
                                              ID_FIELD,
                                              JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD,
                                              URL_FIELD};

object SolrIndexer {
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
          processor.processFileAsDocs(new File(path)) { (doc)=>
            val url = doc.getFieldValue(URL_FIELD).asInstanceOf[String];
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
