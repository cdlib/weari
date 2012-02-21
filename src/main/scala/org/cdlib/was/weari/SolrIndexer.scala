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
    val q = new SolrQuery;
    q.setQuery("id:\"%s\"".format(id));
    return try {
      Some((new solr.SolrDocumentCollection(server, q)).head);
    } catch {
      case ex : NoSuchElementException => None;
    }
  }

  /**
   * Index a single Solr document. If a document with the same ID
   * already exists, the documents will be merged.
   *
   * @param doc Document to index.
   */
  def index(doc : SolrInputDocument) {
    val id = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
    if (!filter.contains(id)) {
      filter.add(id);
      server.add(doc);
    } else {
      val mergeddoc = getById(id,server).
        map(toSolrInputDocument(_)).
        map(mergeDocs(_, doc));
      server.add(mergeddoc.getOrElse(doc));
    }
  }

  /**
   * Index a single ParsedArchiveRecord.
   * Adds extraFields to document, and extraId to end of id.
   * 
   * @param record Record to index.
   */
  def index (record : ParsedArchiveRecord) {
    val doc = record.toDocument;
    for ((k,v) <- extraFields) v match {
      case l : List[Any] => l.map(v2=>doc.addField(k, v2));
      case o : Any => doc.setField(k, o);
    }
    val oldId = doc.getFieldValue(ID_FIELD).asInstanceOf[String];
    doc.setField(ID_FIELD, "%s.%s".format(oldId, extraId));
    retryOrThrow(3) { index(doc); }
  }

  /**
   * Index a sequence of ParsedArchiveRecords.
   * Commit at the end, or rollback if we get an exception.
   */
  def index (recs : Seq[ParsedArchiveRecord]) {
    try {
      for (rec <- recs) { index(rec); }
      server.commit;
    } catch {
      case ex : Exception => {
        server.rollback;
        throw ex;
      }
    }
  }
}
