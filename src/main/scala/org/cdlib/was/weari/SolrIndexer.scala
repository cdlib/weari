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

  def getById(id : String, server : SolrServer) : Option[SolrDocument] = {
    val q = new SolrQuery;
    q.setQuery("id:\"%s\"".format(id));
    try {
      return Some((new solr.SolrDocumentCollection(server, q)).head);
    } catch {
      case ex : NoSuchElementException => {
        return None;
      }
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
      getById(id, server) match {
        /* it could still be a false positive */
        case None => server.add(doc);
        case Some(olddoc) => {
        val mergedDoc = mergeDocs(toSolrInputDocument(olddoc), doc);
          server.add(mergedDoc);
        }
      }
    }
  }

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

  def index (recs : Seq[ParsedArchiveRecord],
             arcName : String) : Boolean = {
    try {
      for (rec <- recs) { 
        index(rec);
      }
    } catch {
      case ex : Exception => {
        server.rollback;
        error({ "Exception while generating doc from arc (%s) %s.".format(arcName, ex) }, ex);
        ex.printStackTrace();
        return false;
      }
    } finally {
      /* ensure a commit at the end of the stream */
      server.commit;
    }
    return true;
  }
}
