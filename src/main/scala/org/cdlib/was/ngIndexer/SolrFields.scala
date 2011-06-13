package org.cdlib.was.ngIndexer;

object SolrFields {
  val ARCNAME_FIELD        = "arcname";
  val BOOST_FIELD          = "boost";
  val CANONICALURL_FIELD   = "canonicalurl";
  val CHARSET_FIELD        = "charset";
  val CONTENT_FIELD        = "content";
  val CONTENT_LENGTH_FIELD = "contentLength";
  val DATE_FIELD           = "date";
  val DIGEST_FIELD         = "digest";
  val HTTP_TOP_TYPE_FIELD  = "httpTopType";
  val HTTP_TYPE_FIELD      = "httpType";
  val HOST_FIELD           = "host";
  val ID_FIELD             = "id";
  val JOB_FIELD            = "job";
  val PROJECT_FIELD        = "project";
  val SERVER_FIELD         = "server";
  val SITE_FIELD           = "site";
  val SPECIFICATION_FIELD  = "specification";
  val TAG_FIELD            = "tag";
  val TITLE_FIELD          = "title";
  val TOP_TYPE_FIELD       = "topType";
  val TYPE_FIELD           = "type";
  val URLFP_FIELD          = "urlfp";
  val URL_FIELD            = "url";

  /* fields which have a single value */
  val SINGLE_VALUED_FIELDS = 
      List(CANONICALURL_FIELD,
           CONTENT_FIELD,
           CONTENT_LENGTH_FIELD,
           DIGEST_FIELD,
           HOST_FIELD,
           ID_FIELD, 
           SITE_FIELD,
           TAG_FIELD,
           TITLE_FIELD,
           TYPE_FIELD,
           URLFP_FIELD,
           URL_FIELD);

  val MULTI_VALUED_FIELDS =
    List(ARCNAME_FIELD,
         DATE_FIELD,
         JOB_FIELD,
         PROJECT_FIELD,
         SPECIFICATION_FIELD);
}
