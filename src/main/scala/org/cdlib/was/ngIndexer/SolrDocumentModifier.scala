package org.cdlib.was.ngIndexer;

import java.lang.{Object=>JObject}

import java.util.{Collection=>JCollection}

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.{SolrDocument, SolrInputDocument};

import org.archive.net.{UURI,UURIFactory};

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
        if (values != null)
          for (value <- values) 
            idoc.addField(fieldName, value);
      } else {
        idoc.addField(fieldName, doc.getFirstValue(fieldName));
      }
    }
    return idoc;
  }

  /**
   * Remove a single value from a document's field.
   */
  def removeFieldValue (doc : SolrInputDocument, key : String, value : Any) {
    val oldValues = doc.getFieldValues(key);
    doc.removeField(key);
    for (value <- oldValues.filter(_==value))
      doc.addField(key, value);
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
        for (value <- oldfield.filter(_ != removeValue))
          idoc.addField(field, value);
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
    
  def updateFields(doc : SolrInputDocument,
                   fields : Pair[String, Any]*) {
    for (field <- fields) {
      field._2 match {
        case null | None   => ();
        case Some(s)       => doc.addField(field._1, s);
        case s             => doc.addField(field._1, s);
      }
    }
  }
          
  /**
   * Update the url-based fields in a document.
   */
  def updateDocUrls (doc : SolrInputDocument, 
                     url : String) {
    val uuri = UURIFactory.getInstance(url);
    val host = uuri.getHost;
    updateFields(doc,
                 HOST_FIELD         -> host,
                 SITE_FIELD         -> host,
                 URL_FIELD          -> url,
                 URLFP_FIELD        -> UriUtils.fingerprint(uuri),
                 CANONICALURL_FIELD -> uuri.toString);
  }

  /**
   * Update the content type fields in a document.
   *
   * @parameter detected The content type as parsed.
   * @parameter supplied The content type as supplied by the server.
   */
  def updateContentType (doc      : SolrInputDocument,
                         detected : ContentType,
                         supplied : ContentType) {
    updateFields(doc,
                 MEDIA_TYPE_SUP_FIELD -> supplied.mediaTypeString,
                 CHARSET_SUP_FIELD    -> supplied.charset,
                 MEDIA_TYPE_DET_FIELD -> detected.mediaTypeString,
                 CHARSET_DET_FIELD    -> detected.charset);
  }

  def makeDocument (rec : IndexArchiveRecord,
                    parseResult : MyParseResult) : Option[SolrInputDocument] = {
    val doc = new SolrInputDocument;
    if (rec.getDigestStr.isEmpty) {
      return None;
    } else {
      /* set the fields */
      val uuri = UURIFactory.getInstance(rec.getUrl);
      val digest = rec.getDigestStr;
      updateFields(doc,
                   ID_FIELD             -> "%s.%s".format(uuri.toString, digest.getOrElse("-")),
                   DIGEST_FIELD         -> digest,
                   DATE_FIELD           -> rec.getDate,
                   CONTENT_FIELD        -> parseResult.content,
                   TITLE_FIELD          -> parseResult.title,
                   CONTENT_LENGTH_FIELD -> rec.getLength);
      updateDocBoost(doc, 1.0f);
      updateDocUrls(doc, rec.getUrl);
      updateContentType(doc, parseResult, rec);
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
      for (fieldName <- SINGLE_VALUED_FIELDS)
        retval.setField(fieldName, a.getFieldValue(fieldName));
      /* fields to merge */
      for (fieldName <- MULTI_VALUED_FIELDS) {
        def emptyIfNull(xs : JCollection[JObject]) : List[JObject] = xs match {
          case null => List();
          case seq  => seq.toList;
        }
        val values = (emptyIfNull(a.getFieldValues(fieldName)) ++
                      emptyIfNull(b.getFieldValues(fieldName))).distinct;
        for (value <- values)
          retval.addField(fieldName, value);
      }
    }
    return retval;
  }
}
