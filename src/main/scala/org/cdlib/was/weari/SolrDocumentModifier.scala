/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.lang.{Object=>JObject}

import java.util.{Collection=>JCollection}

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.{SolrDocument, SolrInputDocument};

import org.apache.solr.client.solrj.util.ClientUtils.toSolrInputDocument;

import org.cdlib.was.weari.SolrFields._;

import scala.collection.JavaConversions.collectionAsScalaIterable;

object SolrDocumentModifier {
  /**
   * Remove a single value from a document's field.
   */
  def removeFieldValue (doc : SolrInputDocument, key : String, value : Any) {
    val oldValues = doc.getFieldValues(key);
    doc.removeField(key);
    for (value <- oldValues.filter(_==value))
      doc.addField(key, value);
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
        case null | None    => ();
        case Some(s)        => doc.addField(field._1, s);
        case seq : Traversable[Any] =>
          for (v <- seq) doc.addField(field._1, v);
        case s              => doc.addField(field._1, s);
      }
    }
  }

  def updateFields(doc : SolrInputDocument,
                   fields : Map[String,Any]) {
    updateFields(doc, fields.toSeq : _*);
  }

  def makeDoc(fields : Pair[String, Any]*) : SolrInputDocument = {
    var doc = new SolrInputDocument;
    updateFields(doc, fields : _*);
    return doc;
  }

  /**
   * Update the content type fields in a document.
   *
   * @parameter detected The content type as parsed.
   * @parameter supplied The content type as supplied by the server.
   */
  def updateContentType (doc      : SolrInputDocument,
                         detected : Option[ContentType],
                         supplied : ContentType) {
    updateFields(doc,
                 MEDIA_TYPE_GROUP_DET_FIELD -> detected.flatMap(_.mediaTypeGroup),
                 MEDIA_TYPE_SUP_FIELD       -> supplied.mediaType,
                 CHARSET_SUP_FIELD          -> supplied.charset,
                 MEDIA_TYPE_DET_FIELD       -> detected.map(_.mediaType),
                 CHARSET_DET_FIELD          -> detected.flatMap(_.charset));
  }

  def shouldIndexContentType (contentType : ContentType) : Boolean = {
    /* Right now we index everything except audio, video, image, js, & css */
    contentType.top match {
      case "audio" | "video" | "image" => false;
      case "text" => contentType.sub match {
        case "javascript" | "css" => false;
        case _ => true;
      }
      case "application" => contentType.sub match {
        case "zip" => false;
        case _     => true;
      }
    }
  }
}
