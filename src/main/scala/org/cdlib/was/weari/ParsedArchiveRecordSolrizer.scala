/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.updateFields;

object ParsedArchiveRecordSolrizer {
  
  private def getContent (rec : ParsedArchiveRecord) : Option[String] = 
    if (shouldIndexContentType(rec.suppliedContentType) ||
        shouldIndexContentType(rec.detectedContentType.getOrElse(ContentType.DEFAULT)))
          rec.content
    else None;

  private def shouldIndexContentType (contentType : ContentType) : Boolean = {
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

  def convert (rec : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = new SolrInputDocument;
    /* set the fields */
    val detected = rec.detectedContentType;
    val supplied = rec.suppliedContentType;
    val boost = 1.0f;

    updateFields(doc,
                 BOOST_FIELD                -> boost,
                 ARCNAME_FIELD              -> rec.filename,
                 ID_FIELD                   -> "%s.%s".format(rec.canonicalUrl,
                                                        rec.digest.getOrElse("-")),
                 HOST_FIELD                 -> rec.canonicalHost,
                 CANONICALURL_FIELD         -> rec.canonicalUrl,
                 URL_FIELD                  -> rec.url,
                 URLFP_FIELD                -> rec.urlFingerprint,
                 DIGEST_FIELD               -> rec.digest,
                 DATE_FIELD                 -> rec.date,
                 TITLE_FIELD                -> rec.title,
                 CONTENT_LENGTH_FIELD       -> rec.length,
                 CONTENT_FIELD              -> getContent(rec),
                 MEDIA_TYPE_GROUP_DET_FIELD -> detected.flatMap(_.mediaTypeGroup),
                 MEDIA_TYPE_SUP_FIELD       -> supplied.mediaType,
                 CHARSET_SUP_FIELD          -> supplied.charset,
                 MEDIA_TYPE_DET_FIELD       -> detected.map(_.mediaType),
                 CHARSET_DET_FIELD          -> detected.flatMap(_.charset));
    doc.setDocumentBoost(boost);
    return doc;
  }
}
