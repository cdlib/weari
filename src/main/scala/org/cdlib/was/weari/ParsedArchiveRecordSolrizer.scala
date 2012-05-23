/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateContentType,updateFields};

object ParsedArchiveRecordSolrizer {
  
  private def getContent (rec : ParsedArchiveRecord) : Option[String] = 
    if (shouldIndexContentType(rec.suppliedContentType) ||
        shouldIndexContentType(rec.detectedContentType.getOrElse(ContentType.DEFAULT)))
          rec.content
    else None;

  def convert (rec : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = new SolrInputDocument;
    /* set the fields */
    updateFields(doc,
                 ARCNAME_FIELD        -> rec.filename,
                 ID_FIELD             -> "%s.%s".format(rec.canonicalUrl,
                                                        rec.digest.getOrElse("-")),
                 HOST_FIELD           -> rec.canonicalHost,
                 CANONICALURL_FIELD   -> rec.canonicalUrl,
                 URL_FIELD            -> rec.url,
                 URLFP_FIELD          -> rec.urlFingerprint,
                 DIGEST_FIELD         -> rec.digest,
                 DATE_FIELD           -> rec.date,
                 TITLE_FIELD          -> rec.title,
                 CONTENT_LENGTH_FIELD -> rec.length,
                 CONTENT_FIELD        -> getContent(rec));
    updateDocBoost(doc, 1.0f);
    updateContentType(doc, rec.detectedContentType, rec.suppliedContentType);
    return doc;
  }
}
