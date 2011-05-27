package org.cdlib.was.ngIndexer;

import java.io.{File,FileInputStream,FileNotFoundException,InputStream,IOException};

import java.net.URI;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.{SolrDocument, SolrInputDocument};

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrProcessor.{ARCNAME_FIELD,
                                              ID_FIELD,
                                              JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD,
                                              URL_FIELD};

import org.cdlib.was.ngIndexer.SolrProcessor.{doc2InputDoc,mergeDocs,processStream,removeFieldValue};

import scala.util.matching.Regex;

/**
 * Class used to index ARC files.
 */
class SolrIndexer(config : Config) extends Retry with Logger {
  val httpClient = new SimpleHttpClient;

  def getById(id : String, server : CommonsHttpSolrServer) : Option[SolrDocument] = {
    val q = new SolrQuery;
    q.setQuery("id:\"%s\"".format(id));
    try {
      return Some((new SolrDocumentCollection(server, q)).head);
    } catch {
      case ex : NoSuchElementException => {
        return None;
      }
    }
  }

  /** Index an ARC file. */
  def index (file : File, 
             extraId : String, 
             extraFields : Map[String, Any],
             server : CommonsHttpSolrServer,
             config : Config) : Boolean = 
    index(new FileInputStream(file), file.getName, extraId, extraFields, server, config);

  /**
   * Index a single Solr document. If a document with the same ID
   * already exists, the documents will be merged.
   *
   * @param server to index with
   * @param doc Document to index.
   */
  def indexDoc(doc : SolrInputDocument,
               server : CommonsHttpSolrServer) {
    val id = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
    getById(id, server) match {
      case None => server.add(doc);
      case Some(olddoc) => {
        val mergedDoc = mergeDocs(doc2InputDoc(olddoc), doc);
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
             server : CommonsHttpSolrServer,             
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
          indexDoc(doc, server);
        } {
          case ex : Exception =>
            logger.error("Exception while indexing document from arc ({}).", arcName, ex);
        }
      }
      /* ensure a commit at the end of the stream */
      server.commit;
    } catch {
      case ex : Exception => {
        logger.error("Exception while generating doc from arc ({}).", arcName, ex);
        return false;
      }
    }
    return true;
  }

  def dryrun (uri : URI, config : Config) {
    val ArcRE = new Regex(""".*?([A-Za-z0-9\.-]+arc.gz).*""");
    val ArcRE(arcName) = uri.getPath;
    httpClient.getUri(uri) { (stream)=>
      dryrun(stream, arcName, config);
    }
  }

  def dryrun (file : File, config : Config) {
    dryrun(new FileInputStream(file), file.getName, config);
  }

  def dryrun (stream : InputStream,
              arcName : String,
              config : Config) {
    try {
      processStream(arcName, stream, config) { (doc) =>
        /* noop */
      }
    } catch {
      case ex : Exception => {
        logger.error("Exception while generating doc from arc ({}).", arcName, ex);
      }
    }
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

    try {
      val command = args(0);
      command match {
        case "dryrun" => 
          for (path <- args.drop(1)) {
            if (path.startsWith("http")) {
              indexer.dryrun(new URI(path), config);
            } else {
              indexer.dryrun(new File(path), config);
            }
          }
        case "index" => {
          val job = args(1);
          val specification = args(2);
          val project = args(3)
          val server = new StreamingUpdateSolrServer(args(4),
                                                     config.queueSize(),
                                                     config.threadCount());

          command match {
            case "index" => {
              for (path <- args.drop(5)) {
                indexer.index(new File(path), specification,
                              Map(JOB_FIELD -> job, 
                                    SPECIFICATION_FIELD -> specification, 
                                  PROJECT_FIELD -> project),
                              server,
                              config)
              }
            }
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
