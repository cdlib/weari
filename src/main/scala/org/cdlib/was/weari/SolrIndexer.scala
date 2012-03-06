/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.client.solrj.util.ClientUtils.toSolrInputDocument;
import org.apache.solr.client.solrj.{SolrServer,SolrQuery};
import org.apache.solr.common.{SolrDocument,SolrInputDocument};
import org.cdlib.was.weari.SolrDocumentModifier.mergeDocs;
import org.cdlib.was.weari.SolrFields.ID_FIELD;

import grizzled.slf4j.Logging;

/**
 * Class used to index ARC files.
 */
class SolrIndexer (server : SolrServer, 
                   filter : QuickIdFilter,
                   extraId : String, 
                   extraFields : Map[String, Any]) 
    extends Retry with Logging {

  val httpClient = new SimpleHttpClient;
  val parser = new MyParser;

  /**
   * Get a single document by its id. Return None if no document 
   * has that id.
   * 
   */
  def getById(id : String, server : SolrServer) : Option[SolrDocument] = {
    val q = (new SolrQuery).setQuery("id:\"%s\"".format(id));
    val docs = new solr.SolrDocumentCollection(server, q);
    return docs.headOption;
  }

  /**
   * Index a single Solr document. If a document with the same ID
   * already exists, the documents will be merged.
   *
   * @param doc Document to index.
   */
  def indexWithMerge(doc : SolrInputDocument) {
    retryOrThrow(3) { 
      val id = getId(doc);
      if (!filter.contains(id)) {
        server.add(doc);
      } else {
        val mergeddoc = getById(id,server).
        map(toSolrInputDocument(_)).
        map(mergeDocs(_, doc));
        server.add(mergeddoc.getOrElse(doc));
      }
    }
  }

  /**
   * Index a singel solr document without merging with any existing document.
   */
  def indexNoMerge(doc : SolrInputDocument) {
    retryOrThrow(3) { 
      server.add(doc);
    }
  }

  /**
   * Return the ID field in a solr document.
   */
  def getId (doc : SolrInputDocument) : String = 
    doc.getFieldValue(ID_FIELD).asInstanceOf[String];

  /**
   * Convert a ParsedArchiveRecord into a SolrInputDocument, merging
   * in extraFields and extraId (see SolrIndexer constructor).
   */
  def record2inputDocument (record : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = record.toDocument;
    for ((k,v) <- extraFields) v match {
      case l : List[Any] => l.map(v2=>doc.addField(k, v2));
      case o : Any => doc.setField(k, o);
    }
    doc.setField(ID_FIELD, "%s.%s".format(getId(doc), extraId));
    return doc;
  }

  /**
   * Generate a Map from IDs to SolrInputDocuments, based on a
   * Iterable[SolrDocument].
   */
  private def makeDocMap (docs : Iterable[SolrDocument]) : 
      Map[String,SolrInputDocument] = {
        val i = for (doc <- docs) 
                yield {
                  val idoc = toSolrInputDocument(doc);
                  (getId(idoc), idoc)
                };
        return i.toMap;
      }
        
  /**
   * Index a sequence of ParsedArchiveRecords.
   * Commit at the end, or rollback if we get an exception.
   * Existing documents will be merged with new documents.
   */
  def index (arc : String, recs : Seq[ParsedArchiveRecord]) {
    val q = (new SolrQuery).setQuery("arcname:\"%s\"".format(arc));
    val olddocs = new solr.SolrDocumentCollection(server, q);
    commitOrRollback {
      if (olddocs.isEmpty) {
        for (rec <- recs) indexWithMerge(record2inputDocument(rec));
      } else {
        /* If we have already indexed this ARC, we can retrieve all */
        /* the documents in the ARC and merge faster. */
        val olddocMap = makeDocMap(olddocs)
        for (rec <- recs) {
          val doc = record2inputDocument(rec);
          val olddoc = olddocMap.get(getId(doc)).getOrElse(
            throw new Exception("Bad map passed to fastIndex! Missing at least: %s from %s".format(getId(doc), arc)));
          indexNoMerge(mergeDocs(olddoc, doc));
        }
      }
    }
  }
  
  /**
   * Perform f, and either commit at the end if there were no exceptions,
   * or rollback if there were.
   */
  def commitOrRollback[A] (f: => A) : A = 
    SolrIndexer.commitOrRollback(server) (f)    
}

object SolrIndexer {
  /**
   * Perform f, and either commit at the end if there were no exceptions,
   * or rollback if there were.
   */
  def commitOrRollback[A] (server : SolrServer) (f: => A) : A = {
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
}
