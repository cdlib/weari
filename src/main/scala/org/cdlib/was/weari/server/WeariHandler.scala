/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari.server;

import com.codahale.jerkson.Json;
import com.codahale.jerkson.ParsingException;

import grizzled.slf4j.Logging;

import java.io.{ InputStream, IOException, OutputStream };
import java.util.UUID;
import java.util.zip.{ GZIPInputStream, GZIPOutputStream };
import java.util.{ List => JList, Map => JMap }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FSDataInputStream, FSDataOutputStream, Path }

import org.apache.pig.{ExecType,PigServer};
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.{SolrQuery,SolrServer};
import org.apache.solr.client.solrj.impl.{ ConcurrentUpdateSolrServer, HttpClientUtil, HttpSolrServer };
import org.apache.solr.common.params.ModifiableSolrParams;

import org.cdlib.was.weari.Utility.{extractArcname,null2option};
import org.cdlib.was.weari._;
import org.cdlib.was.weari.thrift;
import org.cdlib.was.weari.SolrDocumentModifier.{ addFields, record2inputDocument };

import scala.collection.JavaConversions.{ iterableAsScalaIterable, mapAsScalaMap, seqAsJavaList }
import scala.collection.immutable.HashSet;
import scala.collection.mutable;
import scala.util.matching.Regex;
    
class WeariHandler(config: Config)
  extends thrift.Server.Iface with Logging with ExceptionLogger {

  var mergeManagerCache = new mutable.HashMap[String,MergeManager]
    with mutable.SynchronizedMap[String,MergeManager];

  val conf = new Configuration();
  val fs = FileSystem.get(conf);
  val jsonDir = new Path(config.jsonBaseDir);
  var locks : mutable.Map[String,AnyRef] = new mutable.HashMap[String,AnyRef]
    with mutable.SynchronizedMap[String,AnyRef];

  def mkHttpClient = {
    val params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 16);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 16);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, 0);
    HttpClientUtil.createClient(params);
  }

  /**
   * Perform f, and either commit at the end if there were no exceptions,
   * or rollback if there were.
   */
  def commitOrRollback[A](server : SolrServer) (f: => A) : A = {
    try {
      val retval = f;
      server.commit;
      return retval;
    } catch {
      case ex : Exception => {
        server.rollback;
        throw ex;
      }
    }
  }
    
  def withArcParse[T](path : Path) (f: (Seq[ParsedArchiveRecord])=>T) : T = {
    val arcname = getArcname(path);
    var in : InputStream = null;
    try {
      in = fs.open(path);
      if (path.getName.endsWith("gz")) {
        in = new GZIPInputStream(in);
      }
      val records = Json.parse[List[ParsedArchiveRecord]](in);
      f(records);
    } catch {
      case ex : ParsingException => {
        error("Bad JSON: %s".format(arcname));
        throw new thrift.BadJSONException(ex.toString, arcname);
      }
      case ex : java.io.EOFException => {
        error("Bad JSON: %s".format(arcname));
        throw new thrift.BadJSONException(ex.toString, arcname);
      }
    } finally {
      if (in != null) in.close;
    }
  }

  def mkSolrServer (url : String) = 
    new ConcurrentUpdateSolrServer(url,
                                   mkHttpClient,
                                   config.queueSize,
                                   config.threadCount);

  def getMergeManager (solr : String, extraId : String, filterQuery : String) = 
    mergeManagerCache.getOrElseUpdate(extraId, 
                                      new MergeManager(config, filterQuery, 
                                                       new HttpSolrServer(solr)));

  def solrLock[T] (url : String) (f: => T) : T = {
    locks.getOrElseUpdate(url, new Object).synchronized {
      f;
    }
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
            extraFields : JMap[String, JList[String]]) {
    val arcPaths = arcs.map(getPath(_));
    solrLock (solr) {
      val server = mkSolrServer(solr)
      val manager = getMergeManager(solr, extraId, filterQuery);
      val extraFieldsMap = extraFields.toMap.mapValues(iterableAsScalaIterable(_));
      commitOrRollback(server) {
        for ((arcname, path) <- arcs.zip(arcPaths)) {
          try {
            withArcParse(path) { records =>
              manager.loadDocs(new SolrQuery("arcname:\"%s\"".format(arcname)));
              val docs = for (rec <- records)
                         yield record2inputDocument(rec, extraFieldsMap, extraId);
              /* group documents for batch merge */
              /* this will ensure that we don't build up a lot of merges before hitting the */
              /* trackCommitThreshold */
              for { group <- docs.grouped(config.batchMergeGroupSize);
                    merged <- manager.batchMerge(group) } {
                      server.add(merged); 
                    }
              if (manager.trackedCount > config.trackCommitThreshold) {
                info("Merge manager threshold reached: committing.");
                server.commit;
                manager.reset;
              }
            }
          } catch {
            case ex : thrift.BadJSONException => throw ex;
            case ex : Exception => {
              error("Caught exception: %s".format(ex), ex);
              debug(getStackTrace(ex));
              throw new thrift.IndexException(ex.toString);
            }
          }
        }
      }
    }
  }

  def unindex(solr : String,
              arcs : JList[String],
              extraId : String) {
    val arcPaths = arcs.map(getPath(_));
  }

  def clearMergeManager(managerId : String) {
    mergeManagerCache.remove(managerId);
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
    val properties = PropertiesUtil.loadDefaultProperties();
    properties.setProperty("pig.splitCombination", "false");
    val pigContext = new PigContext(ExecType.MAPREDUCE, properties);
    val pigServer = new PigServer(pigContext);
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
  val ARCNAME_RE = new Regex("""^(.*)\.json(.gz)?$""");

  private def getArcname(path : Path) : String = {
    path.getName match {
      case ARCNAME_RE(arcname) => arcname;
      case o : String => o;
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
