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
                   manager : MergeManager,
                   extraId : String, 
                   extraFields : Map[String, Any]) 
    extends Retry with Logging {

  val httpClient = new SimpleHttpClient;
  val parser = new MyParser;

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
   * Index a single Solr document. If a document with the same ID
   * already exists, the documents will be merged.
   *
   * @param doc Document to index.
   */
  def indexWithMerge(doc : SolrInputDocument) {
    serverAdd(manager.merge(doc));
  }

  /**
   * Add a document to the solr server.
   */
  def serverAdd(doc : SolrInputDocument) {
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
  private def record2inputDocument (record : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = record.toDocument;
    for ((k,v) <- extraFields) v match {
      case l : List[Any] => l.map(v2=>doc.addField(k, v2));
      case o : Any => doc.setField(k, o);
    }
    doc.setField(ID_FIELD, "%s.%s".format(getId(doc), extraId));
    return doc;
  }

  /**
   * Index a sequence of ParsedArchiveRecords.
   * Existing documents will be merged with new documents.
   */
  def index (recs : Seq[ParsedArchiveRecord]) {
    for (rec <- recs) {
      serverAdd(manager.merge(record2inputDocument(rec)));
    }
  }

  /**
   * Perform f, and either commit at the end if there were no exceptions,
   * or rollback if there were.
   */
  def commitOrRollback[A] (f: => A) : A = {
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
