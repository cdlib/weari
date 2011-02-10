package org.cdlib.was.ngIndexer;

import java.io.{File,FileInputStream,FileNotFoundException,InputStream,IOException};

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrProcessor.{ARCNAME_FIELD,
                                              ID_FIELD,
                                              JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD,
                                              URL_FIELD};

import org.slf4j.LoggerFactory

/** For updating a solr index
  */
class SolrIndexer(config : Config) {
  val processor = new SolrProcessor;

  val logger = LoggerFactory.getLogger(classOf[SolrIndexer]);

  /** Index an ARC file. */
  def index (file : File, extraFields : Map[String, String]) {
    index(new FileInputStream(file), file.getName, extraFields);
  }

  def index (stream : InputStream, arcName : String, extraFields : Map[String, String]) : Boolean = {
    val server = new SolrDistributedServer(config.indexers());    
    var counter = 0;
    try {
      processor.processStream(arcName, stream) { (doc) =>
        val url = doc.getFieldValue(URL_FIELD).asInstanceOf[String];
        if (!url.startsWith("filedesc:") && !url.startsWith("dns:")) {
          for ((k,v) <- extraFields) { doc.setField(k, v); }
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
    } catch {
      case ex : Exception => {
        logger.error("Exception while generating doc from arc ({}): {}.", arcName, ex);
        return false;
      }
    }
    return true;
  }

  def delete (file : File, removeFields : Map[String,String]) {
    val server = new SolrDistributedServer(config.indexers());    
    var counter = 0;
    Utility.eachArc(file, { (rec) =>
      val id = processor.record2id(rec);
      server.getById(id) match {
        case None => ();
        case Some(olddoc) => {
          val inputdoc = processor.doc2InputDoc(olddoc);
          processor.removeFieldValue(inputdoc, ARCNAME_FIELD, file.getName);
          for ((k,v) <- removeFields) {
            processor.removeFieldValue(inputdoc, k, v);
          }
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
  def loadConfigOrExit : Config = {
    val configPath = System.getProperty("org.cdlib.was.ngIndexer.ConfigFile") match {
      case null =>
        val default = new File("indexer.conf");
        if (default.exists) { Some(default.getPath); }
        else { None; }
      case path => Some(path);
    }
    if (configPath.isEmpty) {
      System.err.println("Please define org.cdlib.was.ngIndexer.ConfigFile! or create indexer.conf file.");
      System.exit(1);
    }
    return (new Configurator).loadSimple(configPath.get, classOf[Config]);
  }

  def main (args : Array[String]) {
    val config = loadConfigOrExit;
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
              indexer.delete(new File(path), Map(JOB_FIELD->job,
                                                 SPECIFICATION_FIELD->specification,
                                                 PROJECT_FIELD->project));
            }
          }
          case "index" => {
            for (path <- args.drop(4)) {
              indexer.index(new File(path), Map(JOB_FIELD -> job, 
                                                 SPECIFICATION_FIELD -> specification, 
                                                 PROJECT_FIELD -> project))
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
