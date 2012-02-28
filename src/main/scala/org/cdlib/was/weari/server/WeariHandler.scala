package org.cdlib.was.weari.server;

import org.cdlib.was.weari._
import org.cdlib.was.weari.thrift
import java.io.{InputStream, IOException}; 
import java.util.{ List => JList, Map => JMap }
import java.util.UUID;
import java.util.zip.GZIPInputStream
import com.codahale.jerkson.Json.parse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FSDataInputStream, FSDataOutputStream, Path }
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer
import scala.collection.JavaConversions.{ collectionAsScalaIterable, mapAsScalaMap, seqAsJavaList }
import scala.collection.immutable.HashSet
import grizzled.slf4j.Logging;
import org.cdlib.was.weari.thrift.UnparsedException
import org.apache.pig.PigServer;
import org.cdlib.was.weari.Utility.null2option;
    
class WeariHandler(config: Config)
  extends thrift.Server.Iface with Logging {

  val conf = new Configuration();
  val fs = FileSystem.get(conf);
  val jsonDir = new Path(config.jsonBaseDir());

  /**
   * Get the HDFS path for the JSON parse of an ARC file.
   * Can return either a .gz or a non-gzipped file.
   * 
   * @param arc The name of the ARC file.
   * @return Path 
   */
  def getPath (arcName : String): Path = {
    val extracted = Utility.extractArcname(arcName);
    val json = "%s.json".format(extracted);
    val jsongz = "%s.gz".format(json);
    val jsonPath = new Path(jsonDir, json);
    if (fs.exists(jsonPath)) {
      return jsonPath;
    } else {
      val jsongzPath = new Path(jsonDir, jsongz);
      if (fs.exists(jsongzPath)) {
        return jsongzPath;
      }
    }
    throw new thrift.UnparsedException(arcName);
  }

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
    val filter = new QuickIdFilter(filterQuery, server);
    val indexer = new SolrIndexer(server = server,
                                  filter = filter,
                                  extraId = extraId,
                                  extraFields = extraFields.toMap);

    for ((arcname, path) <- arcs.zip(arcs.map(getPath(_)))) {
      var in : InputStream = null;
      try {
        in = fs.open(path);
    	if (path.getName.endsWith("gz")) {
          in = new GZIPInputStream(in);
        }
        indexer.index(parse[List[ParsedArchiveRecord]](in));
      } catch {
        case ex : Exception => {
          error("Error while indexing %s: %s".format(arcname, ex), ex);
          if (in != null) in.close;
        }
      }
    }
  }

  def ping {
    println("pinged");
  }
  
  def mkUUID : String = UUID.randomUUID().toString();

  def mkArcList(arcs : Seq[String]) : Path = {
    val arclistName = mkUUID;
    val path = new Path(arclistName)
    val os = fs.create(path);
    for (arcname <- arcs) {
      os.writeBytes("%s\n".format(arcname));
    }
    os.close;
    return path;
  }

  def refileJson (source : Path) {
    fs.mkdirs(jsonDir);
    null2option(fs.listStatus(source)).map { children =>
      for (child <- children) {
        val path = child.getPath;
        if (child.isDir) {
          refileJson(path);
        } else {
          if (path.getName.endsWith(".json") || path.getName.endsWith(".json.gz")) {
            val newPath = new Path(jsonDir, path.getName);
            println("moving %s to %s".format(path, newPath));
            fs.rename(path, new Path(jsonDir, path.getName));
          }
        }
      }
    }
  }

  def parseArcs (arcs: JList[String]) {
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
    pigServer.store("Data", storePath,
    		        "org.cdlib.was.weari.pig.JsonParsedArchiveRecordStorer");
    		              
    refileJson(new Path(storePath));
  }	
}

