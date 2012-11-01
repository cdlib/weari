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

import grizzled.slf4j.Logging;

import java.io.{ File, FileOutputStream, InputStream, OutputStream };

import org.apache.solr.client.solrj.{SolrQuery, SolrServer};
import org.apache.solr.client.solrj.impl.{ConcurrentUpdateSolrServer, HttpClientUtil, HttpSolrServer};
import org.apache.solr.common.{SolrDocument, SolrInputDocument, SolrInputField};
import org.apache.solr.common.params.ModifiableSolrParams;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.MergeManager.removeMerge;
import org.cdlib.was.weari.SolrUtils.{addFields, mkInputField, record2inputDocument, toSolrInputDocument};
import org.cdlib.was.weari.Utility.{extractArcname, null2option};

import org.apache.hadoop.fs.Path;

import scala.collection.mutable;
import scala.collection.JavaConversions.seqAsJavaList;
import scala.util.control.Breaks._;

import org.cdlib.was.weari.pig.PigUtil;
    
class Weari(config: Config)
  extends Logging with ExceptionLogger {

  var mergeManagerCache = new mutable.HashMap[String,MergeManager]
    with mutable.SynchronizedMap[String,MergeManager];

  var locks : mutable.Map[String,AnyRef] = new mutable.HashMap[String,AnyRef]
    with mutable.SynchronizedMap[String,AnyRef];

  val pigUtil = new PigUtil(config);

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
        error("Rolled back!");
        error(ex);
        throw ex;
      }
    }
  }

  def getMergeManager (solr : String, extraId : String, filterQuery : String) = 
    mergeManagerCache.getOrElseUpdate(extraId, 
                                      new MergeManager(config, filterQuery, 
                                                       new HttpSolrServer(solr),
                                                       !config.commitBetweenArcs));

  def withSolrServer[T] (solrUrl : String) (f: (SolrServer)=>T) : T = {
      val httpClient = {
        val params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 16);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 16);
        params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
        params.set(HttpClientUtil.PROP_SO_TIMEOUT, 0);
        HttpClientUtil.createClient(params);
      }
      val server = new ConcurrentUpdateSolrServer(solrUrl,
                                                  httpClient,
                                                  config.queueSize,
                                                  config.threadCount);
      f(server);
  }
    
  /**
   * Synchronize around a unique string.
   */
  def withLock[T] (lockId : String) (f : => T) : T = {
    locks.getOrElseUpdate(lockId, new Object).synchronized {
      f;
    }
  }
  
  /**
   * Synchronize around a solr server url, and call a function with the solr server.
   */
  def withLockedSolrServer[T] (solrUrl : String) (f: (SolrServer)=>T) : T = {
    withLock(solrUrl) {
      withSolrServer[T](solrUrl)(f);
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
            arcs : Seq[String],
            extraId : String,
            extraFields : Map[String, Seq[String]]) {
    val arcPaths = arcs.map(pigUtil.getPath(_));
    withLock (extraId) {
      withSolrServer(solr) { (server) => {
        val manager = getMergeManager(solr, extraId, filterQuery);
        for ((arcname, path) <- arcs.zip(arcPaths)) {
          val records : Seq[ParsedArchiveRecord] = pigUtil.readJson(path);
          manager.loadDocs(new SolrQuery("arcname:\"%s\"".format(arcname)));
          val docs = for (rec <- records)
                     yield record2inputDocument(rec, extraFields, extraId);
          /* group documents for batch merge */
          /* this will ensure that we don't build up a lot of merges before hitting the */
          /* trackCommitThreshold */
          for (group <- docs.grouped(config.batchMergeGroupSize)) {
            for (merged <- manager.batchMerge(group)) {
              server.add(merged); 
            }
          }
          if (manager.trackedCount > config.trackCommitThreshold) {
            info("Merge manager threshold reached: committing.");
            server.commit;
            manager.reset;
          }
          if (config.commitBetweenArcs) {
            server.commit;
            manager.reset;
          }
        }
      }}
    }
  }

  def move(query : String,
           fromUrl : String,
           toUrl : String) {
    withLockedSolrServer(fromUrl) { from =>
      withLockedSolrServer(toUrl) { to =>
        commitOrRollback(from) {
          commitOrRollback(to) {
            breakable {
              var docs : SolrDocumentCollection = null;

              /* we have to group the docs here (see take(...) below) 
               * because we delete docs while we are iterating over 
               * them */

              while (true) {
                docs = new SolrDocumentCollection(from, new SolrQuery(query).setRows(config.numDocsPerRequest));

                if (docs.isEmpty) { break; }

                var deleteBuffer = mutable.Buffer[String]();
                
                for (doc <- docs.take(config.commitThreshold)) {
                  to.add(toSolrInputDocument(doc));
                  deleteBuffer += SolrFields.getId(doc);
                }
                
                info("Move threshold reached: committing.");
                if (!deleteBuffer.isEmpty) { from.deleteById(deleteBuffer); }
                to.commit;
                from.commit;
              }
            }
          }
        }
      }
    }
  }

  /**
   * Index a seq of ParsedArchiveRecods.
   * Used for testing. Simplified version of index with seq of arcnames to parse.
   */
  def indexRecords(solr : String,
                   filterQuery : String,
                   records : Seq[ParsedArchiveRecord],
                   extraId : String,
                   extraFields : Map[String, Seq[String]]) {
    withLockedSolrServer (solr) { server =>
      val manager = getMergeManager(solr, extraId, filterQuery);
      commitOrRollback(server) {
        val docs = for (rec <- records)
                   yield record2inputDocument(rec, extraFields, extraId);
        for (group <- docs.grouped(config.batchMergeGroupSize)) {
          for (merged <- manager.batchMerge(group)) {
            server.add(merged); 
          }
        }
      }
    }
  }

  def getDocs (server : SolrServer, query : String) : Iterable[SolrDocument] = 
    new SolrDocumentCollection(server, new SolrQuery(query).setRows(config.numDocsPerRequest));

  def getDocs (url : String, query : String) : Iterable[SolrDocument] = 
    getDocs(new HttpSolrServer(url), query);
  
  def getInputDocs (server : SolrServer, query : String) : Iterable[SolrInputDocument] =
    getDocs(server, query).map(toSolrInputDocument(_));

  def getInputDocs (url : String, query : String) : Iterable[SolrInputDocument] = 
    getInputDocs(new HttpSolrServer(url), query);
    
  /**
   * Set fields unconditionally on a group of documents retrieved by a query string.
   */
  def setFields(solr : String,
                queryString : String,
                fields : Map[String, Seq[String]]) {
    withLockedSolrServer(solr) { writeServer =>
      commitOrRollback(writeServer) {
        for (doc <- getInputDocs(solr, queryString)) {
          for ((name, value) <- fields) {
            doc.put(name, mkInputField(name, value))
          }
          writeServer.add(doc);
        }
      }
    }
  }
               
  /**
   * Remove index entries for these ARC files from the solr server.
   */
  def remove(solr : String,
             arcs : Seq[String]) {
    withLockedSolrServer(solr) { writeServer =>
      commitOrRollback(writeServer) {
        for (arcname <- arcs) {
          var deletes = mutable.ArrayBuffer[String]()
          // for each document that matches the arcname: query, we
          // will either a) delete it, if it is the last arc file to
          // contain that doc, or b) remove the column of merged
          // values corresponding to that arc from the document
          for (doc <- getDocs(solr, "arcname:%s".format(arcname))) {
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

  def clearMergeManager(managerId : String) {
    mergeManagerCache.remove(managerId);
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
    pigUtil.parseArcs(arcs);
  }

  def deleteParse (arc : String) {
    pigUtil.getPathOption(arc).map(pigUtil.delete(_));
  }
}
