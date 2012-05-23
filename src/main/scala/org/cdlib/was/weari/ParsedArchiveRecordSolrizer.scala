/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateFields};

object ParsedArchiveRecordSolrizer {
  
  private def getContent (rec : ParsedArchiveRecord) : Option[String] = 
    if (shouldIndexContentType(rec.suppliedContentType) ||
        shouldIndexContentType(rec.detectedContentType.getOrElse(ContentType.DEFAULT)))
          rec.content
    else None;

  def convert (rec : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = new SolrInputDocument;
    /* set the fields */
    val detected = rec.detectedContentType;
    val supplied = rec.suppliedContentType;
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
                 CONTENT_FIELD        -> getContent(rec),
                 MEDIA_TYPE_GROUP_DET_FIELD -> detected.flatMap(_.mediaTypeGroup),
                 MEDIA_TYPE_SUP_FIELD       -> supplied.mediaType,
                 CHARSET_SUP_FIELD          -> supplied.charset,
                 MEDIA_TYPE_DET_FIELD       -> detected.map(_.mediaType),
                 CHARSET_DET_FIELD          -> detected.flatMap(_.charset));

    updateDocBoost(doc, 1.0f);
    return doc;
  }
}
