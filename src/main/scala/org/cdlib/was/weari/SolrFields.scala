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

import org.apache.solr.common.{SolrDocument,SolrInputDocument};

object SolrFields {
  val ARCNAME_FIELD        = "arcname";
  val BOOST_FIELD          = "boost";
  val CANONICALURL_FIELD   = "canonicalurl";
  val CHARSET_DET_FIELD    = "charsetdet";
  val CHARSET_SUP_FIELD    = "charsetsup";
  val CONTENT_FIELD        = "content";
  val CONTENT_LENGTH_FIELD = "contentlength";
  val DATE_FIELD           = "date";
  val MEDIA_TYPE_DET_FIELD = "mediatypedet";
  val MEDIA_TYPE_GROUP_DET_FIELD = "mediatypegroupdet";
  val MEDIA_TYPE_SUP_FIELD = "mediatypesup";
  val DIGEST_FIELD         = "digest";
  val HOST_FIELD           = "host";
  val HOST_TOKENIZED_FIELD = "hostt";
  val ID_FIELD             = "id";
  val INSTITUTION_FIELD    = "institution";
  val JOB_FIELD            = "job";
  val PROJECT_FIELD        = "project";
  val SERVER_FIELD         = "server";
  val SPECIFICATION_FIELD  = "specification";
  val TAG_FIELD            = "tag";
  val TITLE_FIELD          = "title";
  val TYPE_FIELD           = "type";
  val URLFP_FIELD          = "urlfp";
  val URL_FIELD            = "url";
  val URL_TOKENIZED_FIELD  = "urlt";

  /* fields which have a single value */
  val SINGLE_VALUED_FIELDS = 
      List(CANONICALURL_FIELD,
           CHARSET_DET_FIELD,
           CHARSET_SUP_FIELD,
           CONTENT_FIELD,
           CONTENT_LENGTH_FIELD,
           DIGEST_FIELD,
           HOST_FIELD,
           HOST_TOKENIZED_FIELD,
           ID_FIELD, 
           MEDIA_TYPE_DET_FIELD,
           MEDIA_TYPE_GROUP_DET_FIELD,
           MEDIA_TYPE_SUP_FIELD,
           PROJECT_FIELD,
           SPECIFICATION_FIELD,
           TITLE_FIELD,
           URLFP_FIELD,
           URL_FIELD,
           URL_TOKENIZED_FIELD);

  val MULTI_VALUED_FIELDS =
    List(ARCNAME_FIELD,
         DATE_FIELD,
         JOB_FIELD,
         TAG_FIELD);

  /**
   * Return the ID field in a solr document.
   */
  def getId (doc : SolrInputDocument) : String = 
    doc.getFieldValue(ID_FIELD).asInstanceOf[String];

  /**
   * Return the ID field in a solr document.
   */
  def getId (doc : SolrDocument) : String = 
    doc.getFieldValue(ID_FIELD).asInstanceOf[String];

}
