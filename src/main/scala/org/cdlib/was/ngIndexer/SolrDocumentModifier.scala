package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.{SolrDocument, SolrInputDocument}

import org.cdlib.was.ngIndexer.Warc2Solr.TAG_FIELD;

import scala.collection.JavaConversions.asScalaIterable;

object SolrDocumentModifier extends Logger {
  /** Turn an existing SolrDocument into a SolrInputDocument suitable
    * for sending back to solr.
    */
  def doc2InputDoc (doc : SolrDocument) : SolrInputDocument = {
    val idoc = new SolrInputDocument();
    for (fieldName <- doc.getFieldNames) {
      if (Warc2Solr.MULTI_VALUED_FIELDS.contains(fieldName)) {
        val values = doc.getFieldValues(fieldName);
        if (values != null) {
          for (value <- values) { idoc.addField(fieldName, value); }
        }
      } else {
        idoc.addField(fieldName, doc.getFirstValue(fieldName));
      }
    }
    return idoc;
  }

  /** Remove a single value from a document's field.
    */
  def removeFieldValue (doc : SolrInputDocument, key : String, value : Any) {
    val oldValues = doc.getFieldValues(key);
    doc.removeField(key);
    for (value <- oldValues.filter(_==value)) {
      doc.addField(key, value);
    }
  }
  
  def modifyDocuments (query : String, 
                       server : CommonsHttpSolrServer)
                      (f : (SolrDocument) => Option[SolrInputDocument]) {
    val q = new SolrQuery;
    q.setQuery(query);
    val coll = new SolrDocumentCollection(server, q);
    for (doc <- coll) {
      f(doc) match {
        case None => ();
        case Some(idoc) => {
          server.add(idoc);
        }
      }
    }
    server.commit;
  }

  def addFieldValue(query : String, 
                    field : String, 
                    value : String, 
                    server : CommonsHttpSolrServer) {
    modifyDocuments (query, server) { (doc)=>
      val idoc = doc2InputDoc(doc);
      val oldfield = idoc.getFieldValues(field);
      if ((oldfield != null) && !oldfield.contains(value)) {
        idoc.addField(field, value);
        Some(idoc);
      } else {
        None;
      }
    }
  }

  def removeFieldValue(query : String, 
                       field : String, 
                       removeValue : String, 
                       server : CommonsHttpSolrServer) {
    modifyDocuments (query, server) { (doc)=>
      val idoc = doc2InputDoc(doc);
      val oldfield = idoc.getFieldValues(field);
      if ((oldfield != null) && oldfield.contains(removeValue)) {
        idoc.removeField(field);
        for (value <- oldfield.filter(_ != removeValue)) {
          idoc.addField(field, value);
        }
        Some(idoc);
      } else {
        None;
      }
    }
  }

  def noop(query : String, server : CommonsHttpSolrServer) {
    modifyDocuments (query, server) { (doc)=>
      Some(doc2InputDoc(doc)) 
    }
  }
}
