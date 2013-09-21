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

package org.cdlib.was.weari;

import com.typesafe.scalalogging.slf4j.Logging;

import java.io.{ File, FileOutputStream, InputStream, OutputStream };

import org.apache.solr.client.solrj.{SolrQuery, SolrServer};
import org.apache.solr.client.solrj.impl.{ConcurrentUpdateSolrServer, CloudSolrServer, HttpClientUtil, HttpSolrServer};
import org.apache.solr.common.{SolrDocument, SolrInputDocument, SolrInputField};
import org.apache.solr.common.params.ModifiableSolrParams;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.MergeManager.removeMerge;
import org.cdlib.was.weari.SolrUtils.{addFields, mkInputField, record2inputDocument, toSolrInputDocument};
import org.cdlib.was.weari.Utility.{extractArcname, null2option};

import org.apache.hadoop.fs.Path;

import scala.collection.mutable;
import scala.collection.JavaConversions.{ seqAsJavaList, mapAsJavaMap };
import scala.concurrent.Lock;
import scala.util.control.Breaks._;

import org.cdlib.was.weari.pig.PigUtil;
    
class Weari(config: Config)
  extends Logging with ExceptionLogger {

  var mergeManagerCache = new mutable.HashMap[String,MergeManager];

  var lock = new Lock;

  val pigUtil = new PigUtil(config);

  def isLocked = !lock.available;

  /**
   * Perform f, and either commit at the end if there were no exceptions,
   * or rollback if there were.
   */
  def commitOrRollback[A](server : SolrServer) (f: => A) : A = {
    try {
      val retval = f;
      /* don't wait for searcher */
      server.commit(false, false);
      return retval;
    } catch {
      case ex : Exception => {
        server.rollback; 
        logger.error("Rolled back!");
        throw ex;
      }
    }
  }

  def withSolrServer[T] (f: (SolrServer)=>T) : T = {
    val server = {
      if (config.solrZkHost != "") {
        val s = new CloudSolrServer(config.solrZkHost);
        s.setDefaultCollection(config.solrCollection);
        s;
      } else {
        new ConcurrentUpdateSolrServer(config.solrServer, config.queueSize, config.threadCount);
      }
    }
    val retval = f(server);
    server match {
      case s : ConcurrentUpdateSolrServer => s.blockUntilFinished;
    }
    return retval;
  }

  /**
   * Synchronize around a unique string.
   */
  def withLock[T] (f : => T) : T = {
    try {
      logger.info("Trying to acquire lock");
      lock.acquire;
      logger.info("Acquired lock.");
      val retval = f;
      return retval;
    } finally {
      lock.release;
      logger.info("Released lock.");
    }
  }

  /**
   * Synchronize around a solr server url, and call a function with the solr server.
   */ 
  def withLockedSolrServer[T] (f: (SolrServer)=>T) : T = {
    withLock { withSolrServer[T](f); }
  }

  def tryCommit (s : SolrServer, times : Int) {
    try {
      s.commit;
    } catch {
      case ex : org.apache.solr.common.SolrException => {
        if (times >= 10) {
          throw ex;
        } else {
          tryCommit(s, times + 1);
        }
      }
    }
  }

  def tryCommit (s : SolrServer) { tryCommit(s, 0); }

  /**
   * Index a set of ARCs on a solr server.
   *
   * @param arcs A list of ARC names to index
   * @param extraId String to append to solr document IDs.
   * @param extraFields Map of extra fields to append to solr documents.
   */
  def index (arcs : Seq[String], extraId : String,
    extraFields : Map[String, Seq[String]]) {
    withLockedSolrServer { (server) => {
      val arcPaths = arcs.map(pigUtil.getPath(_));
      for ((arcname, path) <- arcs.zip(arcPaths)) {
        val records : Seq[ParsedArchiveRecord] = pigUtil.readJson(path);
        for (rec <- records) {
          server.add(record2inputDocument(rec, extraFields, extraId));
        }
      }
      tryCommit(server);
    }}
  }

  def index (arcs : Seq[String], extraId : String) {
    index (arcs, extraId, Map[String, Seq[String]]());
  }

  def getDocs (query : String) : Iterable[SolrDocument] = 
    new SolrDocumentCollection(new HttpSolrServer(config.solrServer), new SolrQuery(query).setRows(config.numDocsPerRequest));
  
  /**
   * Set fields unconditionally on a group of documents retrieved by a query string.
   */
  def setFields(queryString : String,
                fields : Map[String, Seq[String]]) {
    withLockedSolrServer { writeServer =>
      commitOrRollback(writeServer) {
        val newq = new SolrQuery(queryString).setParam("fl", SolrFields.ID_FIELD).setRows(config.numDocIdsPerRequest);
        val docs = new SolrDocumentCollection(writeServer, newq);
        for (doc <- docs) {
          val updateDoc = new SolrInputDocument();
          updateDoc.setField(SolrFields.ID_FIELD, SolrFields.getId(doc));
          for ((name, value) <- fields) {
            updateDoc.setField(name, mapAsJavaMap(Map("set"->seqAsJavaList(value))));
          }
          writeServer.add(updateDoc);
        }
      }
    }
  }
               
  /**
   * Remove index entries for these ARC files from the solr server.
   */
  def remove(arcs : Seq[String]) {
    withLockedSolrServer { writeServer =>
      commitOrRollback(writeServer) {
        for (arcname <- arcs) {
          var deletes = mutable.ArrayBuffer[String]()
          // for each document that matches the arcname: query, we
          // will either a) delete it, if it is the last arc file to
          // contain that doc, or b) remove the column of merged
          // values corresponding to that arc from the document
          for (doc <- getDocs("arcname:%s".format(arcname))) {
            removeMerge(SolrFields.ARCNAME_FIELD, arcname, doc) match {
              case None => 
                deletes += SolrFields.getId(doc);
              case Some(inputDoc) =>
                writeServer.add(inputDoc);
            }
          }
            if (deletes.length > 0) 
              writeServer.deleteById(deletes);
          // commit between arcs to ensure that if our next arc to
          // remove has any of the same docs they will get removed too
          writeServer.commit;
        }
      }
    }
  }

  /**
   * Check to see if a given ARC file has been parsed.
   */
  def isArcParsed (arcName : String) : Boolean = 
    pigUtil.getPathOption(arcName).isDefined;

  /**
   * Parse ARC files.
   */
  def parseArcs (arcs : Seq[String]) {
    for (arcGroup <- arcs.grouped(config.batchArcParseSize)) {
      pigUtil.parseArcs(arcGroup);
    }
  }

  def deleteParse (arc : String) {
    pigUtil.getPathOption(arc).map(pigUtil.delete(_));
  }
}
