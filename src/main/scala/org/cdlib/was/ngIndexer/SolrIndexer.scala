/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io.{BufferedWriter,File,FileInputStream,FileNotFoundException,FileOutputStream,InputStream,InputStreamReader,IOException,OutputStream,OutputStreamWriter,StringWriter,Writer};

import java.net.URI;

import java.util.zip.{GZIPInputStream,GZIPOutputStream};

import net.liftweb.json.{DefaultFormats,JsonParser,Serialization};

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.util.ClientUtils.toSolrInputDocument;
import org.apache.solr.common.{SolrDocument, SolrInputDocument};

import org.apache.tika.exception.TikaException;

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrFields._;
import org.cdlib.was.ngIndexer.Utility.null2option;

import org.cdlib.was.ngIndexer.SolrDocumentModifier.{mergeDocs};

import scala.collection.mutable.SynchronizedQueue;
import scala.util.matching.Regex;

import sun.misc.{Signal, SignalHandler};

/**
 * Class used to index ARC files.
 */
class SolrIndexer extends Retry with Logger {
  val httpClient = new SimpleHttpClient;
  val parser = new MyParser;

  /* max size, in bytes, of files to parse. If file is larger, do not parse */
  val MAX_PARSE_SIZE = 5000000;

  /**
   * Take an archive record & return a solr document, or None if it is
   * not a 200 response. If the parse failed, log errors and return a
   * record without content.
   */
  def parseArchiveRecord(rec : WASArchiveRecord with InputStream) : 
      Option[ParsedArchiveRecord] = {
    if (!rec.isHttpResponse || (rec.getStatusCode != 200)) {
      rec.close;
      return None;
    } else {
      val parsed = if (rec.getLength > MAX_PARSE_SIZE) {
        None;
      } else {
        try {
          Some(parser.parse(rec));
        } catch {
          case ex : TikaException => {
            logger.error("Caught exception parsing %s in arc %s: {}".format(rec.getUrl, rec.getFilename), ex);
            None;
          }
        }
      }
      rec.close;
      if (rec.getDigestStr.isEmpty) {
        /* need to check now because the ARC needs to be closed before we can get it */
        throw new Exception("No digest string found.");
      } else {
        return Some(parsed.getOrElse(ParsedArchiveRecord(rec)));
      }
    }
  }
  
  def getById(id : String, server : SolrServer) : Option[SolrDocument] = {
    val q = new SolrQuery;
    q.setQuery("id:\"%s\"".format(id));
    try {
      return Some((new solr.SolrDocumentCollection(server, q)).head);
    } catch {
      case ex : NoSuchElementException => {
        return None;
      }
    }
  }

  def writeRec (rec : ParsedArchiveRecord, writer : Writer) {
    implicit val formats = DefaultFormats;
    Serialization.write(rec, writer);
  }
  
  def rec2json (rec : ParsedArchiveRecord) : String = {
    val w = new StringWriter;
    writeRec(rec, w);
    return w.toString;
  }

  /**
   * Convert an ARC file into a gzipped JSON file representing the parsed
   * content ready for indexing.
   */
  def arc2json (stream : InputStream,
                arcName : String,
                file : File) {
    val gzos = new GZIPOutputStream(new FileOutputStream(file));
    val writer = new BufferedWriter(new OutputStreamWriter(gzos, "UTF-8"));
    writer.write("[");
    processStream(arcName, stream) { rec =>
      writeRec(rec, writer);
      writer.write(",\n");
    }
    writer.write("]");
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

  /**
   * For each record in a file, call the function.
   */
  def processFile (file : File)
                  (func : (ParsedArchiveRecord) => Unit) {
    for (rec <- ArchiveReaderFactoryWrapper.get(file)) {
      catchAndLogExceptions("Caught exception processing %s: {}".format(file.getName)) {
        parseArchiveRecord(rec).map(func);
      }
    }
  }

  def processStream (arcName : String, stream : InputStream)
                    (func : (ParsedArchiveRecord) => Unit) {
    for (rec <- ArchiveReaderFactoryWrapper.get(arcName, stream)) {
      catchAndLogExceptions("Caught exception processing %s: {}".format(arcName)) {
        parseArchiveRecord(rec).map(func);
      }
    }
  }
}

object SolrIndexer {
  val indexer = new SolrIndexer;

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

  def parse(args : Array[String])(implicit config : Config) {
    var finished = false;

    val handler = new SignalHandler {
      def handle (signal : Signal) { 
        finished = true;
      }
    };

    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
    
    var q = new SynchronizedQueue[String];
    val infile = new File(args(1));
    q ++= io.Source.fromFile(infile).getLines;
    var threads = List[Thread]();
    for (n <- 1.to(config.threadCount())) {
      val thread = new Thread() {
        val executor = new CommandExecutor(config);
        override def run {
          while (!finished && !q.isEmpty) {
            val uri = q.dequeue;
            val Utility.ARC_RE(arcname) = new URI(uri).getPath;
            executor.exec(ParseCommand(uri=uri,
                                       jsonpath="%s.json.gz".format(arcname)));
          }
        }
      }
      thread.start;
      threads = thread :: threads;
    }
    threads.map(_.join);
  }

  def main (args : Array[String]) {
    implicit val config = loadConfigOrExit;

    try {
      val indexer = new SolrIndexer;
      val command = args(0);
      command match {
        case "test" => {
          import Utility.{withFileOutputStream,flushStream};
          for (urlS <- args.drop(1)) {
            System.err.println("Processing %s".format(urlS));
            val uri = new URI(urlS.toString);
            val matcher = Utility.ARC_RE.pattern.matcher(uri.getPath);
            if (!matcher.matches) {
              System.err.println("No arc name");
              System.exit(1);
            } else {
              val arcName = matcher.group(1);
              var tmpfile : Option[File] = None;
              indexer.httpClient.getUri(uri) { is =>
                tmpfile = Some(new File(new File(System.getProperty("java.io.tmpdir")), arcName));
                System.err.println("Fetching %s to %s".format(urlS, tmpfile.get));
                withFileOutputStream(tmpfile.get) { os =>
                  flushStream(is, os);
                }
              }
              if (tmpfile.isEmpty) {
                System.err.println("Didn't get file?");
              } else {
                val reader = ArchiveReaderFactoryWrapper.get(tmpfile.get);
                val it = reader.iterator;
                System.err.println("Iterating...");
                while (it.hasNext) {
                  val rec = it.next;
                  try {
                    if (!rec.isHttpResponse || rec.getStatusCode != 200) {
                      /* try again */
                    } else {
                      val retval = indexer.parseArchiveRecord(rec);
                      if (retval.isEmpty) {
                        /* this should not happen */
                        throw new Exception("Got empty parse: %s".format(rec.getUrl));
                      }
                    }
                  } finally {
                    if (rec != null) rec.close;
                  }
                }
              }
            }
          }
        }
        case "parse" => {
          parse(args);
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
