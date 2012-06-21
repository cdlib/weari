/* Copyright (c) 2009-2012 The Regents of the University of California */

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
class MergeManager (candidatesQuery : String, server : SolrServer, n : Int) 
    extends Logging {
  def this (candidatesQuery : String, server : SolrServer) = 
    this(candidatesQuery, server, 100000);

  /* candiatesQuery is a query that should return all <em>possible</em> merge
   * candidates. It can return everything, though this would not be
   * efficient */

  /* A bloom filter used to check for POSSIBLE merge candidates */
  private val bf = new BloomFilter64bit(n, 12);

  /* keeps track of what has been merged so far */
  private var tracked : Map[String,SolrInputDocument] = null;
  this.reset;

  /* initialize */
  if (server != null) {
    debug("Loading document ids into MergeManager.");
    val newq = new SolrQuery(candidatesQuery).setParam("fl", ID_FIELD).setRows(100000);
    val docs = new solr.SolrDocumentCollection(server, newq);
    for (doc <- docs) bf.add(getId(doc))
  }

  private def cleanId (id : String) =
    id.replace("\\", "\\\\").replace("\"", "\\\"");

  /**
   * Reset all tracked documents, but *not* the bloomfilter.
   */
  def reset {
    tracked = new HashMap[String,SolrInputDocument] with SynchronizedMap[String,SolrInputDocument];
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
