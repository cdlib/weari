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
 * NOT thread safe.
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
      val mergedDoc = getDocById(id).map(MergeManager.mergeDocs(_, doc));
      retval = mergedDoc.getOrElse(doc);
    }
    /* whether or not we merged this, we save it for possible later merging */
    tracked.put(id, retval);
    bf.add(id);
    return retval;
  }

  def unmerge(doc : SolrInputDocument) : Option[SolrInputDocument] = {
    val id = getId(doc);
    var retval : Option[SolrInputDocument] = None;
    if (bf.contains(id)) {
      val merged = getDocById(id);
      if (merged.isEmpty) {
        throw new Exception();
      } else {
        retval = MergeManager.unmergeDocs(merged.get, doc);
      }
    } else {
      throw new Exception();
    }
    if (retval.isDefined) {
      tracked.put(id, retval.get);
      bf.add(id);
    } else {
      tracked.remove(id);
    }
    return retval;
  }

  private def cleanId (id : String) =
    id.replace("\\", "\\\\").replace("\"", "\\\"");

  private def buildIdQuery (ids : Seq[String]) : SolrQuery = {
    val q1 = for (id <- ids) yield "id:\"%s\"".format(cleanId(id));
    return new SolrQuery().setQuery(q1.mkString("", " OR ", ""));
  }

  private def batchLoad(docs : Seq[SolrInputDocument]) {
    for (group <- docs.grouped(config.maxIdQuerySize)) {
      loadDocs(buildIdQuery(group.map(getId(_))));
    }
  }

  /**
   * Merge a bunch of docs at once. Hits the solr server more slowly.
   */
  def batchMerge (docs : Seq[SolrInputDocument]) : Seq[SolrInputDocument] = {
    batchLoad(docs.filter(d=>bf.contains(getId(d))));
    return for (doc <- docs) 
           yield merge(doc);
  }

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

object MergeManager {
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

  private def safeFieldValues(fieldname : String, doc : SolrInputDocument) : Seq[Any] = 
    null2option(doc.getField(fieldname)) match {
      case None => List[Any]();
      case Some(field) => null2seq(field.getValues);
    }

  /**
   * Remove the field values in one doc from a merged doc.
   */
  def unmergeFieldValues (fieldname : String, merged : SolrInputDocument, doc : SolrInputDocument) : Seq[Any] = {
    val valuesToDelete = safeFieldValues(fieldname, doc).toSet;
    val values = safeFieldValues(fieldname, merged);
    return values.filterNot(valuesToDelete.contains(_));
  }

  /**
   * Sets the field values of SINGLE_VALUED_FIELDS to either the
   * content in A or B, whichever is set.
   */
  private def setSingleValuedFields (a : SolrInputDocument, b : SolrInputDocument, merged : SolrInputDocument) {
    for { fieldname <- SINGLE_VALUED_FIELDS;
          fieldvalue <- getSingleFieldValue(fieldname, a, b) } {
            merged.setField(fieldname, fieldvalue);
          }
  }
    
  /**
   * Merge fields from MULTI_VALUED_MERGE_FIELDS
   */
  private def mergeMultiValuedMergeFields (a : SolrInputDocument, 
                                           b : SolrInputDocument, 
                                           merged : SolrInputDocument) {
    for { fieldname <- MULTI_VALUED_MERGE_FIELDS;
          fieldvalue <- mergeFieldValues(fieldname, a, b) } {
            merged.addField(fieldname, fieldvalue);
          }
  }

  /**
   * "Unmmerge" MULTI_VALUED_MERGE_FIELDS in doc from the values in merged.
   */
  private def unmergeMultiValuedMergeFields (merged : SolrInputDocument, 
                                             doc : SolrInputDocument, 
                                             unmerge : SolrInputDocument) {
    for { fieldname <- MULTI_VALUED_MERGE_FIELDS;
          fieldvalue <- unmergeFieldValues(fieldname, merged, doc) } {
            unmerge.addField(fieldname, fieldvalue);
          }
  }
  
  /**
   * Sets values from MULTI_VALUED_SET_FIELDS in the merged document to the value in
   * orig.
   */
  private def setMultiValuedSetFields (orig : SolrInputDocument, 
                                       merged : SolrInputDocument) {
    for { fieldname  <- MULTI_VALUED_SET_FIELDS;
          rawfield   <- null2option(orig.getField(fieldname));
          fieldvalue <- null2seq(rawfield.getValues) } {
            merged.addField(fieldname, fieldvalue);
          }
  }
    
  /**
   * Merge two documents into one, presuming they have the same id.
   * Multi-value fields are concatenated.
   * Document b is used for MULTI_VALUED_SET_FIELDS, e.g. tags.
   */
  def mergeDocs (a : SolrInputDocument, b : SolrInputDocument) : SolrInputDocument = {
    val retval = new SolrInputDocument;
    if (a.getFieldValue(ID_FIELD) != b.getFieldValue(ID_FIELD)) {
      throw new Exception;
    } else {
      setSingleValuedFields(a, b, retval);
      mergeMultiValuedMergeFields(a, b, retval);
      setMultiValuedSetFields(b, retval);
    }
    return retval;
  }

  /**
   * "Unmerge" doc from a merged doc.
   */
  def unmergeDocs (merged : SolrInputDocument, doc : SolrInputDocument) : Option[SolrInputDocument] = {
    val retval = new SolrInputDocument;
    if (merged.getFieldValue(ID_FIELD) != doc.getFieldValue(ID_FIELD)) {
      throw new Exception;
    } else {
      setSingleValuedFields(doc, merged, retval);
      unmergeMultiValuedMergeFields(merged, doc, retval);
      setMultiValuedSetFields(merged, retval);
    }
    /* check ARCNAME field. If this document no longer exists in some */
    /* arc, then we should delete it from the index */
    if (retval.getFieldValue(ARCNAME_FIELD) == null) {
       return None;
    } else {
      return Some(retval);
    }
  }
}
