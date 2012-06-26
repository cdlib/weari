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

import java.util.{Collection=>JCollection}

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};
import org.apache.solr.common.{SolrDocument,SolrInputDocument};
import org.apache.solr.client.solrj.util.ClientUtils.toSolrInputDocument;

import org.archive.util.BloomFilter64bit;

import org.cdlib.was.weari.SolrFields._;

import org.cdlib.was.weari.Utility.{null2option,null2seq};

import scala.collection.mutable.{Map,SynchronizedMap,HashMap};
import scala.math.max;

/**
 * Class used to keep track of merging.
 * We need to merge documents with ones that are already in the index.
 * This class provides a merge function to do this.
 * We also need to sometimes merge two documents with the same id
 * before we can commit to the indexer; for instance when two docs in the same
 * crawl have the same id.
 * So we also need to keep track of documents that we have previously merged, in memory.
 *
 * @author Erik Hetzner <erik.hetzner@ucop.edu>
 */
class MergeManager (config : Config, candidatesQuery : String, server : SolrServer)
    extends Logging {

  /* candiatesQuery is a query that should return all <em>possible</em> merge
   * candidates. It can return everything, though this would not be
   * efficient */

  /* A bloom filter used to check for POSSIBLE merge candidates */
  private var bf : BloomFilter64bit = null;
  this.resetBloomFilter;
  
  /* set the bloomfilter up. Should have size > number of current results */
  /* loads candidate docs into bloomfilter */
  private def resetBloomFilter {
    var expectedSize = 100000;
    if (server != null) {
      val countQuery = new SolrQuery(candidatesQuery).setRows(0);
      val resultCount = server.query(countQuery).getResults.getNumFound;
      if (resultCount > 0) {
        expectedSize = (resultCount * 1.5).toInt;
      }
    }
    logger.info("Building bloomfilter of size %d.".format(expectedSize));
    bf = new BloomFilter64bit(expectedSize, 12);
    /* initialize */
    if (server != null) {
      debug("Loading document ids into MergeManager.");
      val newq = new SolrQuery(candidatesQuery).setParam("fl", ID_FIELD).setRows(100000);
      val docs = new solr.SolrDocumentCollection(server, newq);
      for (doc <- docs) {
        bf.add(getId(doc))
      }
    }
  }

  /* keeps track of what has been merged so far */
  private var tracked : Map[String,SolrInputDocument] = null;
  this.resetTracked;

  private def cleanId (id : String) =
    id.replace("\\", "\\\\").replace("\"", "\\\"");

  private def resetTracked {
    tracked = new HashMap[String,SolrInputDocument] with SynchronizedMap[String,SolrInputDocument];
  }

  /**
   * Reset all tracked documents, and bloomfilter if the size > .75 maxSize
   */
  def reset {
    resetTracked;
    if (bf.size > (0.75 * bf.getExpectedInserts)) resetBloomFilter;
  }

  def trackedCount : Int = tracked.size;

  def isPotentialMerge (id : String) : Boolean =
    bf.contains(id);

  /**
   * Get a single document by its id. Return None if no document 
   * has that id. Will load first from tracked and then try from the server.
   * 
   */
  def getDocById(id : String) : Option[SolrInputDocument] =
    tracked.get(id).orElse {
      /* otherwise try to get it from the solr server */
      val q = buildIdQuery(List(id));
      val docs = new solr.SolrDocumentCollection(server, q);
      /* if we don't use toSeq, headOption hits the solr server TWICE */
      docs.toSeq.headOption.map(toSolrInputDocument(_));
    }

  /**
   * Merge a document with existing docs with the same id.
   */
  def merge(doc : SolrInputDocument) : SolrInputDocument = {
    val id = getId(doc);
    var retval = doc;
    
    if (bf.contains(id)) {
      /* if we got a hit in the bloomfilter, try to fetch the doc, then
         merge our new doc with the previous doc */
      retval = getDocById(id).map(mergeDocs(_, doc)).getOrElse(doc);
    }
    /* whether or not we merged this, we save it for possible later merging */
    tracked.put(id, retval);
    bf.add(id);
    return retval;
  }

  private def buildIdQuery (ids : Seq[String]) : SolrQuery = {
    val q1 = for (id <- ids) yield "id:\"%s\"".format(cleanId(id));
    return new SolrQuery().setQuery(q1.mkString("", " OR ", ""));
  }

  def batchMerge (docs : Seq[SolrInputDocument]) : Seq[SolrInputDocument] = {
    val docsToLoad = docs.filter(d=>bf.contains(getId(d)));
    for (group <- docsToLoad.grouped(config.maxIdQuerySize)) {
      loadDocs(buildIdQuery(group.map(getId(_))));
    }
    return for (doc <- docs) yield merge(doc);
  }

  /**
   * Returns the field value of either a or b. Assumes that they should
   * have the same value. Will not return the empty string or null;
   * will return None if the field is the empty string or null in both
   * document.
   */
  def getSingleFieldValue (fieldname : String, a : SolrInputDocument, b : SolrInputDocument) : Option[Any] = 
    List(a,b).map(_.getFieldValue(fieldname)).
      filter(f=>(f != null) && (f != "")).
      headOption;

  /**
   * Merge the values of a field in two documents into one long, distinct seq.
   */
  def mergeFieldValues (fieldname : String, a : SolrInputDocument, b : SolrInputDocument) : Seq[Any] =
    (null2seq(a.getFieldValues(fieldname)) ++
     null2seq(b.getFieldValues(fieldname))).distinct;

  /**
   * Remove the field values in one doc from a merged doc.
   */
  def unmergeFieldValues (fieldname : String, doc : SolrInputDocument, merged : SolrInputDocument) : Seq[Any] = {
    val valsToDelete = null2seq(doc.getFieldValues(fieldname)).toSet;
    return null2seq(merged.getFieldValues(fieldname)).filterNot(valsToDelete(_));
  }

  private def mergeOrUnmergeDocs (a : SolrInputDocument, 
                                  b : SolrInputDocument,
                                  f : (String, SolrInputDocument, SolrInputDocument)=>Seq[Any]) : SolrInputDocument = {
    val retval = new SolrInputDocument;
    if (a.getFieldValue(ID_FIELD) != b.getFieldValue(ID_FIELD)) {
      throw new Exception;
    } else {
      /* identical fields */
      for { fieldname <- SINGLE_VALUED_FIELDS;
            fieldvalue <- getSingleFieldValue(fieldname, a, b) }
        retval.setField(fieldname, fieldvalue);
      /* fields to merge */
      for { fieldname <- MULTI_VALUED_FIELDS;
            fieldvalue <- f(fieldname, a, b) }
        retval.addField(fieldname, fieldvalue);
    }
    return retval;
  }

  /**
   * Merge two documents into one, presuming they have the same id.
   * Multi-value fields are concatenated.
   */
  def mergeDocs (a : SolrInputDocument, b : SolrInputDocument) : SolrInputDocument =
    mergeOrUnmergeDocs(a, b, mergeFieldValues);

  /**
   * "Unmerge" doc from a merged doc.
   */
  def unmergeDocs (doc : SolrInputDocument, merged : SolrInputDocument) : SolrInputDocument = 
    mergeOrUnmergeDocs(doc, merged, unmergeFieldValues);

  /**
   * Load docs from the server for merging. Used for pre-loading docs when we know we will have
   * a lot of merges to perform. Returns the number of docs loaded.
   */
  def loadDocs (q : SolrQuery) : Int = {
    val docs = new solr.SolrDocumentCollection(server, q.getCopy.setRows(10000));
    var n = 0;
    for (doc <- docs) {
      n += 1;
      loadDoc(toSolrInputDocument(doc));
    }
    return n;
  }
      
  @inline
  def loadDoc (doc : SolrDocument) {
    loadDoc(toSolrInputDocument(doc));
  }

  /**
   * Load a single document into the merge manager for possible future merging.
   */
  @inline
  def loadDoc (doc : SolrInputDocument) {
    val id = getId(doc);
    if (bf.contains(id) && tracked.get(id).isDefined) {
      /* doc DOES exist in merge manager, merge with existing doc in */
      /* manager */
      merge(doc);
    } else {
      /* doc does not exist in merge manager yet */
      bf.add(id);
      tracked.put(id, doc);
    }
  }
}
