package org.cdlib.was.ngIndexer;

import java.lang.{Object=>JObject}

import java.util.{Collection=>JCollection}

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.{SolrDocument, SolrInputDocument};

import org.archive.net.UURIFactory;

import org.cdlib.was.ngIndexer.SolrFields._;

import scala.collection.JavaConversions.collectionAsScalaIterable;

object SolrDocumentModifier extends Logger {

  /**
   * Turn an existing SolrDocument into a SolrInputDocument suitable
   * for sending back to solr.
   */
  def doc2InputDoc (doc : SolrDocument) : SolrInputDocument = {
    val idoc = new SolrInputDocument();
    for (fieldName <- doc.getFieldNames) {
      if (MULTI_VALUED_FIELDS.contains(fieldName)) {
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
                      (f : (SolrInputDocument) => Option[SolrInputDocument]) {
    val q = new SolrQuery;
    q.setQuery(query);
    val coll = new SolrDocumentCollection(server, q);
    for (doc <- coll) {
      val idoc = doc2InputDoc(doc);
      f(idoc) match {
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
    modifyDocuments (query, server) { (idoc)=>
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
    modifyDocuments (query, server) { (idoc)=>
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
    modifyDocuments (query, server) { (idoc)=>
      Some(idoc);
    }
  }

  /**
   * Update the boost in a document.
   */
  def updateDocBoost (doc : SolrInputDocument,
                      boost : Float) {
    doc.setDocumentBoost(boost);
    doc.addField(BOOST_FIELD, boost);
  }
    
  /**
   * Update the url & digest fields in a document.
   */
  def updateDocMain (doc : SolrInputDocument, 
                     url : String,
                     digest : String) {
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;
    doc.addField(ID_FIELD, "%s.%s".format(uuri.toString, digest));
    doc.addField(DIGEST_FIELD, digest);
    doc.addField(HOST_FIELD, host);
    doc.addField(SITE_FIELD, host);
    doc.addField(URL_FIELD, url, 1.0f);
    doc.addField(URLFP_FIELD, UriUtils.fingerprint(uuri));
    doc.addField(CANONICALURL_FIELD, uuri.toString, 1.0f);
  }

  def updateMimeTypes (doc      : SolrInputDocument,
                       detected : ContentType,
                       supplied : ContentType) {
    doc.addField(MEDIA_TYPE_SUP_FIELD, supplied.mediaTypeString, 1.0f);
    supplied.charset.map(str=>doc.addField(CHARSET_SUP_FIELD, str, 1.0f));

    doc.addField(MEDIA_TYPE_DET_FIELD, detected.mediaTypeString);
    detected.charset.map(str=>doc.addField(CHARSET_DET_FIELD, str, 1.0f))
  }

  def makeDocument (rec : IndexArchiveRecord,
                    parseResult : MyParseResult) : Option[SolrInputDocument] = {
    val doc = new SolrInputDocument;
    updateDocBoost(doc, 1.0f);
    if (rec.getDigestStr.isEmpty) {
      return None;
    } else {
      updateDocMain(doc, rec.getUrl, rec.getDigestStr.get);
      doc.addField(DATE_FIELD, rec.getDate, 1.0f);
      parseResult.content.map(str=>doc.addField(CONTENT_FIELD, str, 1.0f));
      updateMimeTypes(doc, 
                      parseResult,
                      rec);
      parseResult.title.map(str=>doc.addField(TITLE_FIELD, str, 1.0f));
      doc.addField(CONTENT_LENGTH_FIELD, rec.getLength, 1.0f);
      return Some(doc);
    }
  }

  /**
   * Merge two documents into one, presuming they have the same id.
   * Multi-value fields are appended.
   */
  def mergeDocs (a : SolrInputDocument, b : SolrInputDocument) : SolrInputDocument = {
    val retval = new SolrInputDocument;
    if (a.getFieldValue(ID_FIELD) != b.getFieldValue(ID_FIELD)) {
      throw new Exception;
    } else {
      /* identical fields */
      for (fieldName <- SINGLE_VALUED_FIELDS) {
        retval.setField(fieldName, a.getFieldValue(fieldName));
      }
      /* fields to merge */
      for (fieldName <- MULTI_VALUED_FIELDS) {
        def emptyIfNull(xs : JCollection[JObject]) : List[JObject] = xs match {
          case null => List();
          case seq  => seq.toList;
        }
        val values = (emptyIfNull(a.getFieldValues(fieldName)) ++
                      emptyIfNull(b.getFieldValues(fieldName))).distinct;
        for (value <- values) {
          retval.addField(fieldName, value);
        }
      }
    }
    return retval;
  }
}
