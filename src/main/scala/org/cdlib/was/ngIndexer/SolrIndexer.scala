package org.cdlib.was.ngIndexer;

import java.io.{File,FileInputStream,FileNotFoundException,InputStream,IOException};

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrProcessor.{ARCNAME_FIELD,
                                              ID_FIELD,
                                              JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD,
                                              URL_FIELD};

import org.cdlib.was.ngIndexer.SolrProcessor.{doc2InputDoc,mergeDocs,processStream,removeFieldValue};

/** Class used to index ARC files.
  */
class SolrIndexer(config : Config) extends Retry with Logger {
  val server = new SolrDistributedServer(config.indexers(), 
                                         config.queueSize(), 
                                         config.queueRunners(),
                                         config.commitThreshold());

  /** Index an ARC file. */
  def index (file : File, extraId : String, extraFields : Map[String, Any], config : Config) : Boolean = 
    index(new FileInputStream(file), file.getName, extraId, extraFields, config);

  /** Index a single Solr document. If a document with the same ID
    * already exists, the documents will be merged.
    *
    * @param server to index with
    * @param doc Document to index.
    */
  def indexDoc(server : SolrDistributedServer, doc : SolrInputDocument) {
    val id = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
    server.getById(id) match {
      case None => server.add(doc);
      case Some(olddoc) => {
        val mergedDoc = mergeDocs(doc2InputDoc(olddoc), doc);
        server.deleteById(id);
        server.add(mergedDoc);
      }
    }
  }
        
  /**
   * Index an arc file.
   * @param extraId extra bit to be appended to the document id
   * @param extraFields Map of extra fields to be added to the document
   * @return true if indexing succeeded
   */
  def index (stream : InputStream, 
             arcName : String,
             extraId : String,
             extraFields : Map[String, Any],
             config : Config) : Boolean = {
    try {
      processStream(arcName, stream, config) { (doc) =>
        for ((k,v) <- extraFields) v match {
          case l : List[Any] => l.map(v2=>doc.addField(k, v2));
          case o : Any => doc.setField(k, o);
        }
        val oldId = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
        doc.setField(ID_FIELD, "%s.%s".format(oldId, extraId));
        retry(3) {
          indexDoc(server, doc);
          server.maybeCommit;
        } {
          case ex : Exception =>
            logger.error("Exception while indexing document from arc ({}): {}.", arcName, ex);
        }
      }
      /* ensure a commit at the end of the stream */
      server.commit;
    } catch {
      case ex : Exception => {
        logger.error("Exception while generating doc from arc ({}): {}.", arcName, ex);
        return false;
      }
    }
    return true;
  }

  def delete (file : File, removeFields : Map[String,String]) {
    Utility.eachArc(file) { 
      (rec) =>
        val id = "xxx" ; // TODO
        server.getById(id) match {
            case None => ();
          case Some(olddoc) => {
            val inputdoc = doc2InputDoc(olddoc);
            removeFieldValue(inputdoc, ARCNAME_FIELD, file.getName);
            for ((k,v) <- removeFields)
              removeFieldValue(inputdoc, k, v);
            server.deleteById(id);
              server.add(inputdoc);
            server.maybeCommit;
          }
        }
      }
    server.commit;
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
              indexer.delete(new File(path),
                             Map(JOB_FIELD->job,
                                 SPECIFICATION_FIELD->specification,
                                 PROJECT_FIELD->project));
            }
          }
          case "index" => {
            for (path <- args.drop(4)) {
              indexer.index(new File(path), specification,
                            Map(JOB_FIELD -> job, 
                                SPECIFICATION_FIELD -> specification, 
                                PROJECT_FIELD -> project),
                            config)
            }
          }
          case "test" => {
            for (path <- args.drop(4)) {
              indexer.index(new File(path), specification,
                            Map(JOB_FIELD -> job, 
                                SPECIFICATION_FIELD -> specification, 
                                PROJECT_FIELD -> project),
                            config)
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
