/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io.{BufferedWriter,File,FileInputStream,FileNotFoundException,FileOutputStream,InputStream,InputStreamReader,IOException,OutputStream,OutputStreamWriter};

import java.net.URI;

import java.util.zip.{GZIPInputStream,GZIPOutputStream};

import net.liftweb.json.{DefaultFormats,JsonParser,Serialization};

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.util.ClientUtils.toSolrInputDocument;
import org.apache.solr.common.{SolrDocument, SolrInputDocument};

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrFields._;
import org.cdlib.was.ngIndexer.Utility.null2option;

import org.cdlib.was.ngIndexer.SolrDocumentModifier.{mergeDocs};

import scala.collection.mutable.SynchronizedQueue;
import scala.util.matching.Regex;
/**
 * Class used to index ARC files.
 */
class SolrIndexer extends Retry with Logger {
  val httpClient = new SimpleHttpClient;

  val parser = new MyParser;

  /**
   * Take an archive record & return a solr document, or none if we
   * cannot parse.
   */
  def parseArchiveRecord(rec : WASArchiveRecord with InputStream) : 
      Option[ParsedArchiveRecord] = {
    if (!rec.isHttpResponse || (rec.getStatusCode != 200)) {
      rec.close;
      return None;
    }
    val parsed = catchAndLogExceptions("Caught exception parsing %s in arc %s: {}".format(rec.getUrl, rec.getFilename)) {
      parser.parse(rec, Some(rec.getContentType.mediaType), rec.getUrl, rec.getDate)
    }
    rec.close;
    if (rec.getDigestStr.isEmpty) {
      /* need to check now because the ARC needs to be closed before we can get it */
      return None;
    } else {
      return Some(ParsedArchiveRecord(rec, parsed));
    }
  }
  
  def getById(id : String, server : SolrServer) : Option[SolrDocument] = {
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

  /**
   * Convert an ARC file into a gzipped JSON file representing the parsed
   * content ready for indexing.
   */
  def arc2json (stream : InputStream,
                arcName : String,
                file : File) {
    val gzos = new GZIPOutputStream(new FileOutputStream(file));
    implicit val formats = DefaultFormats;
    val writer = new BufferedWriter(new OutputStreamWriter(gzos, "UTF-8"));
    writer.write("[", 0, 1);
    processStream(arcName, stream) { (rec) =>
      Serialization.write(rec, writer);
      writer.write(",\n", 0, 2);
    }
    writer.write("]", 0, 1);
    writer.close;
  }

  /**
   * Convert a gzipped JSON file (see #arc2json) into a sequence of ParsedArchiveRecord
   */
  def json2records (file : File) : Seq[ParsedArchiveRecord] = {
    implicit val formats = DefaultFormats;
    val gzis = new GZIPInputStream(new FileInputStream(file));
    return JsonParser.parse(new InputStreamReader(gzis, "UTF-8"), true).extract[List[ParsedArchiveRecord]];
  }

  /**
   * Index an ARC file.
   */
  def index (file : File, 
             extraId : String, 
             extraFields : Map[String, Any],
             server : SolrServer,
             filter : QuickIdFilter) : Boolean = 
    index(new FileInputStream(file), file.getName, extraId, extraFields, server, filter);

  /**
   * Index a single Solr document. If a document with the same ID
   * already exists, the documents will be merged.
   *
   * @param server to index with
   * @param doc Document to index.
   */
  def indexDoc(doc : SolrInputDocument,
               server : SolrServer,
               filter : QuickIdFilter) {
    val id = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
    if (!filter.contains(id)) {
      filter.add(id);
      server.add(doc);
    } else {
      getById(id, server) match {
        /* it could still be a false positive */
        case None => server.add(doc);
        case Some(olddoc) => {
        val mergedDoc = mergeDocs(toSolrInputDocument(olddoc), doc);
          server.add(mergedDoc);
        }
      }
    }
  }
        
  /**
   * Index an arc file.
   *
   * @param extraId extra bit to be appended to the document id
   * @param extraFields Map of extra fields to be added to the document
   * @return true if indexing succeeded
   */
  def index (stream : InputStream,
             arcName : String,
             extraId : String,
             extraFields : Map[String, Any],
             server : SolrServer,
             filter : QuickIdFilter) : Boolean = {
    try {
      processStream(arcName, stream) { (rec) =>
        val doc = rec.toDocument;
        for ((k,v) <- extraFields) v match {
          case l : List[Any] => l.map(v2=>doc.addField(k, v2));
          case o : Any => doc.setField(k, o);
        }
        val oldId = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
        doc.setField(ID_FIELD, "%s.%s".format(oldId, extraId));
        retry(3) {
          indexDoc(doc, server, filter);
        } { 
          case ex : Exception =>
            logger.error("Exception while indexing document from arc ({}).", arcName, ex);
        }
      }
    } catch {
      case ex : Exception => {
        server.rollback;
        logger.error("Exception while generating doc from arc ({}) {}.", arcName, ex);
        ex.printStackTrace();
        return false;
      }
    } finally {
      /* ensure a commit at the end of the stream */
      server.commit;
    }
    return true;
  }

  def dryrun (uri : URI) {
    val ArcRE = new Regex(""".*?([A-Za-z0-9\.-]+arc.gz).*""");
    val ArcRE(arcName) = uri.getPath;
    httpClient.getUri(uri) { (stream)=>
      dryrun(stream, arcName);
    }
  }

  def dryrun (file : File) {
    dryrun(new FileInputStream(file), file.getName);
  }

  def dryrun (stream : InputStream,
              arcName : String) {
    try {
      processStream(arcName, stream) { (res) =>
        System.err.println("%s = %s".format(res.url, res.digest));
        /* noop */
      }
    } catch {
      case ex : Exception => {
        logger.error("Exception while generating doc from arc ({}).", arcName, ex);
      }
    }
  }
   /**
    * For each record in a file, call the function.
    */
  def processFile (file : File) (func : (ParsedArchiveRecord) => Unit) {
    Utility.eachRecord(file) { rec =>
      catchAndLogExceptions("Caught exception processing %s: {}".format(file.getName)) {
        parseArchiveRecord(rec).map(func);
      }
    }
  }

  def processStream (arcName : String, stream : InputStream)
  (func : (ParsedArchiveRecord) => Unit) {
    Utility.eachRecord(stream, arcName) { rec =>
      catchAndLogExceptions("Caught exception processing %s: {}".format(arcName)) {
        parseArchiveRecord(rec).map(func);
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
    val indexer = new SolrIndexer;

    try {
      val command = args(0);
      command match {
        case "dryrun" => 
          for (path <- args.drop(1)) {
            if (path.startsWith("http")) {
              indexer.dryrun(new URI(path));
            } else {
              indexer.dryrun(new File(path));
            }
          }
        case "parse" => {
          var q = new SynchronizedQueue[String];
          val infile = new File(args(1));
          q ++= io.Source.fromFile(infile).getLines;
          var threads = List[Thread]();
          for (n <- 1.to(config.threadCount())) {
            val thread = new Thread() {
              val executor = new CommandExecutor(config);
              override def run {
                while (!q.isEmpty) {
                  val uri = q.dequeue;
                  executor.exec(ParseCommand(uri=uri));
                }
              }
            }
            thread.start;
            threads = thread :: threads;
          }
          threads.map(_.join);
        }
        case "index" => {
          val job = args(1);
          val specification = args(2);
          val project = args(3)
          val institution = args(4)
          val server = new StreamingUpdateSolrServer(args(5),
                                                     config.queueSize(),
                                                     config.threadCount());
          val filter = new QuickIdFilter("specification:\"%s\"".format(specification), server);
          command match {
            case "index" => {
              for (path <- args.drop(6)) {
                indexer.index(new File(path), 
                              specification,
                              Map(JOB_FIELD -> job, 
                                  INSTITUTION_FIELD -> institution,
                                  SPECIFICATION_FIELD -> specification, 
                                  PROJECT_FIELD -> project),
                              server,
                              filter)
              }
            }
          }
        }
        case "json" => {
          val cmds = Command.parse(new File(args(1)));
          val sortedCmds = cmds.sortWith(_.arcName < _.arcName);
          val executor = new CommandExecutor(config);
          sortedCmds.map { cmd =>
            executor.exec(cmd);
            System.out.println("Indexed %s".format(cmd.arcName));
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
