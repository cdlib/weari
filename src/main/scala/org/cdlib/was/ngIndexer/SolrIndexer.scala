package org.cdlib.was.ngIndexer;

import java.io.{File,FileInputStream,FileNotFoundException,InputStream,IOException};
import org.cdlib.ssconf.Configurator;
import org.cdlib.was.ngIndexer.SolrProcessor.{ARCNAME_FIELD,
                                              ID_FIELD,
                                              JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD,
                                              URL_FIELD};

class SolrIndexer(config : Config) {
  val processor = new SolrProcessor;
  val server = new SolrDistributedServer(config.indexers());    

  def index (file : File, job : String, specification : String, 
             project : String) {
    index(new FileInputStream(file), file.getName, job, specification, project);
  }

  def index (stream : InputStream, arcName : String, 
             job : String, specification : String, project : String) {
    var counter = 0;
    processor.processStream(arcName, stream) { (doc) =>
      val url = doc.getFieldValue(URL_FIELD).asInstanceOf[String];
      if (!url.startsWith("filedesc:") && !url.startsWith("dns:")) {
        doc.setField(ARCNAME_FIELD, arcName);
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
        counter = counter + 1;
        if (counter > 10) {
          server.commit;
          counter = 0;
        }
      }
    }
  }

  def delete (file : File, job : String, specification : String, 
              project : String) {
    var counter = 0;
    Utility.eachArc(file, { (rec) =>
      val id = processor.record2id(rec);
      server.getById(id) match {
        case None => ();
        case Some(olddoc) => {
          val inputdoc = processor.doc2InputDoc(olddoc);
          processor.removeFieldValue(inputdoc, ARCNAME_FIELD, file.getName);
          processor.removeFieldValue(inputdoc, JOB_FIELD, job);
          processor.removeFieldValue(inputdoc, PROJECT_FIELD, project);
          processor.removeFieldValue(inputdoc, SPECIFICATION_FIELD, 
                                     specification);
          server.deleteById(id);
          server.add(inputdoc);
          counter = counter + 1;
          if (counter > 10) {
            server.commit;
            counter = 0;
          }
        }
      }
    });
  }
}

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
    val indexer = new SolrIndexer(config);

    if (args.size < 3) {
      System.err.println("Please supply >= 3 args!");
      System.exit(1);
    } else {
      try {
        val command = args(0);
        val job = args(1);
        val specification = args(2);
        val project = args(3)
        command match {
          case "delete" => {
            for (path <- args.drop(4)) {
              indexer.delete(new File(path), job, specification, project);
            }
          }
          case "index" => {
            for (path <- args.drop(4)) {
              indexer.index(new File(path), job, specification, project);
            }
          }
          case _ => {
            System.err.println("No command specified!");
            System.exit(1);
          }
        }
      } catch {
        case ex : FileNotFoundException => ex.printStackTrace();
        case ex : IOException => ex.printStackTrace();
      }
    }
  }
}
