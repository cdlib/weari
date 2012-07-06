/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrUtils.addFields;

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

    addFields(doc,
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
