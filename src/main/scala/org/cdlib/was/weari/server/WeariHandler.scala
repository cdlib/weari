package org.cdlib.was.weari.server;

import org.cdlib.was.weari._
import org.cdlib.was.weari.thrift
import java.io.{InputStream, IOException, OutputStream}; 
import java.util.{ List => JList, Map => JMap }
import java.util.UUID;
import java.util.zip.{GZIPInputStream,GZIPOutputStream};
import com.codahale.jerkson.Json;
import com.codahale.jerkson.ParsingException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FSDataInputStream, FSDataOutputStream, Path }
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer
import scala.collection.JavaConversions.{ collectionAsScalaIterable, mapAsScalaMap, seqAsJavaList }
import scala.collection.mutable.{Map,SynchronizedMap,HashMap};
import scala.collection.immutable.HashSet
import grizzled.slf4j.Logging;
import org.apache.pig.PigServer;
import org.cdlib.was.weari.Utility.{extractArcname,null2option};
    
class WeariHandler(config: Config)
  extends thrift.Server.Iface with Logging {

  val conf = new Configuration();
  val fs = FileSystem.get(conf);
  val jsonDir = new Path(config.jsonBaseDir());
  var locks : Map[String,AnyRef] = new HashMap[String,AnyRef] 
    with SynchronizedMap[String,AnyRef];

  /**
   * Index a set of ARCs on a solr server.
   *
   * @param solr The URI of the solr server to index on.
   * @param filterQuery A solr query string to return candidates for
   *   documents to be possibly merged with.
   * @param arcs A list of ARC names to index
   * @param extraId String to append to solr document IDs.
   * @param extraFields Map of extra fields to append to solr documents.
   */
  def index(solr : String,
            filterQuery : String,
            arcs : JList[String],
            extraId : String,
            extraFields : JMap[String, String]) {
    val server = new StreamingUpdateSolrServer(solr,
      config.queueSize(),
      config.threadCount());
    val arcPaths = arcs.map(getPath(_));
    val filter = new QuickIdFilter(filterQuery, server);
    val indexer = new SolrIndexer(server = server,
                                  filter = filter,
                                  extraId = extraId,
                                  extraFields = extraFields.toMap);
    locks.getOrElseUpdate(solr, new Object).synchronized {
      indexer.commitOrRollback {
        for ((arcname, path) <- arcs.zip(arcPaths)) {
          var in : InputStream = null;
          try {
            in = fs.open(path);
    	    if (path.getName.endsWith("gz")) {
              in = new GZIPInputStream(in);
            }
              indexer.index(arcname, Json.parse[List[ParsedArchiveRecord]](in));
          } catch {
            case ex : ParsingException =>
              error("Bad JSON: %s".format(arcname));
            throw new thrift.BadJSONException(ex.toString, arcname);
            case ex : Exception =>
              error("Caught exception: %s".format(ex));
            throw new thrift.IndexException(ex.toString);
            } finally {
              if (in != null) in.close;
            }
        }
      }
    }
  }

  private def mkUUID : String = UUID.randomUUID().toString();

  private def mkArcList(arcs : Seq[String]) : Path = {
    val arclistName = mkUUID;
    val path = new Path(arclistName)
    val os = fs.create(path);
    for (arcname <- arcs) {
      os.writeBytes("%s\n".format(arcname));
    }
    os.close;
    return path;
  }

  /**
   * Moves all json.gz files beneath the source to their proper resting place.
   */
  private def refileJson (source : Path) {
    fs.mkdirs(jsonDir);
    for (children <- null2option(fs.listStatus(source));
         child <- children) {
      val path = child.getPath;
      if (child.isDir) {
        refileJson(path);
      } else {
        if (path.getName.endsWith(".json.gz")) {
          fs.rename(path, new Path(jsonDir, path.getName));
        }
      }
    }
  }

  /**
   * Check to see if a given ARC file has been parsed.
   */
  def isArcParsed (arcName : String) : Boolean = 
    getPathOption(arcName).isDefined;

  /**
   * Parse ARC files.
   */
  def parseArcs (arcs : JList[String]) =
    parseArcs(arcs.toSeq);
    
  def parseArcs (arcs : Seq[String]) {
    val pigServer = new PigServer(org.apache.pig.ExecType.MAPREDUCE);
    val arcListPath = mkArcList(arcs.toSeq);

    /* add jars in classpath to registered jars in pig */
    val cp = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
    for (entry <- cp if entry.endsWith("jar")) {
      pigServer.registerJar(entry);
    }

    pigServer.registerQuery("""
      Data = LOAD '%s' 
      USING org.cdlib.was.weari.pig.ArchiveURLParserLoader()
      AS (filename:chararray, url:chararray, digest:chararray, date:chararray, length:long, content:chararray, detectedMediaType:chararray, suppliedMediaType:chararray, title:chararray, outlinks);""".
        format(arcListPath));
    val storePath = "%s.json.gz".format(mkUUID);
    val job = pigServer.store("Data", storePath,
    		                 "org.cdlib.was.weari.pig.JsonParsedArchiveRecordStorer");
    
    while (!job.hasCompleted) {
      Thread.sleep(100);
    }
    if (job.getStatus == JOB_STATUS.FAILED) {
      throw new thrift.ParseException("");
    } else {
      refileJson(new Path(storePath));
      for (arc <- arcs if (!isArcParsed(arc))) {
        /* must have been an ARC without non-404 reponses, make an */
        /* empty JSON for it */
        makeEmptyJson(arc);
      }
    }
  }

  def deleteParse (arc : String) {
    getPathOption(arc).map(this.fs.delete(_, false));
  }

  /**
   * Get the HDFS path for the JSON parse of an ARC file.
   * Can return either a .gz or a non-gzipped file.
   * 
   * @param arc The name of the ARC file.
   * @return Path 
   */
  private def getPath (arcName : String): Path = 
    getPathOption(arcName).getOrElse(throw new thrift.UnparsedException(arcName));
    
  /**
   * Get the HDFS path for the JSON parse of an ARC file.
   * Can return either a .gz or a non-gzipped file. Returns None
   * if no JSON file can be found.
   * 
   * @param arc The name of the ARC file.
   * @return Option[Path]
   */
  private def getPathOption (arcName : String): Option[Path] = {
    extractArcname(arcName).flatMap { extracted =>
      val jsonPath = new Path(jsonDir, "%s.json.gz".format(extracted));
      if (fs.exists(jsonPath)) Some(jsonPath) else None;
    }
  }

  /**
   * Make an empty JSON file for arcs that parsed to nothing.
   */
  private def makeEmptyJson (arc : String) {
    val cleanArc = extractArcname(arc).get;
    val path = new Path(jsonDir, "%s.json.gz".format(cleanArc));
    var out : OutputStream = null;
    try {
      out = fs.create(path);
      val gzout = new GZIPOutputStream(out);
      gzout.write("[]".getBytes("UTF-8"));
      gzout.flush;
      gzout.close;
    } finally {
      if (out != null) out.close;
    } 
  }
}
