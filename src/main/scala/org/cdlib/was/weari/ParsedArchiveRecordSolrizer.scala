/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.common.SolrInputDocument;

import org.archive.net.UURIFactory;

import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateDocUrls,updateContentType,updateFields};

object ParsedArchiveRecordSolrizer {
  
  private def getContent (rec : ParsedArchiveRecord) : Option[String] = 
    if (shouldIndexContentType(rec.suppliedContentType) ||
        shouldIndexContentType(rec.detectedContentType.getOrElse(ContentType.DEFAULT)))
          rec.content
    else None;

  def convert (rec : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = new SolrInputDocument;
    /* set the fields */
    val uuri = UURIFactory.getInstance(rec.url);
    updateFields(doc,
                 ARCNAME_FIELD        -> rec.filename,
                 ID_FIELD             -> "%s.%s".format(uuri.toString, 
                                                        rec.digest.getOrElse("-")),
                 DIGEST_FIELD         -> rec.digest,
                 DATE_FIELD           -> rec.date,
                 TITLE_FIELD          -> rec.title,
                 CONTENT_LENGTH_FIELD -> rec.length,
                 CONTENT_FIELD        -> getContent(rec));
    updateDocBoost(doc, 1.0f);
    updateDocUrls(doc, rec.url);
    updateContentType(doc, rec.detectedContentType, rec.suppliedContentType);
    return doc;
  }
}
