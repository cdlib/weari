/* Copyright (c) 2009-2012 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.common.SolrInputDocument;

import scala.collection.JavaConversions.collectionAsScalaIterable;

object SolrDocumentModifier {
  /**
   * Add a set of fields to a SolrInputDocument.
   * If field value is None or null, do not add.
   * If field value is Some(x), add x.
   * If field value is Traversable[Any], add each value.
   * Otherwise just add it.
   */
  def addFields(doc : SolrInputDocument,
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

  /**
   * Make a new SolrInputDocument with the fields provided.
   * See documentation on addFields for field processing
   */
  def makeDoc(fields : Pair[String, Any]*) : SolrInputDocument = {
    var doc = new SolrInputDocument;
    addFields(doc, fields : _*);
    return doc;
  }
}
