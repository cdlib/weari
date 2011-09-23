package org.cdlib.was.ngIndexer;

import org.archive.net.UURIFactory;
import java.io.{BufferedWriter,File,FileOutputStream,OutputStreamWriter,Writer};

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.util.ClientUtils;

import org.cdlib.was.ngIndexer.SolrFields._;
import org.cdlib.was.ngIndexer.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateDocUrls,updateContentType,updateFields};

class IndexResource (var rec : IndexArchiveRecord,
                     var parseResult : MyParseResult) {
  val filename = rec.getFilename;
  val digest = rec.getDigestStr;
  val url = rec.getUrl
  val date = rec.getDate;
  val title = parseResult.title;
  val length = rec.getLength;
  val content = if (shouldIndexContentType(rec)) {
    Some(parseResult.content);
  } else { 
    None;
  }
  /* make lightweight copies of these for later */
  val suppliedContentType = new ContentTypeImpl(rec);
  val detectedContentType = new ContentTypeImpl(parseResult);

  /* throw away the refs */
  rec = null;
  parseResult = null;

  def makeDocument : Option[SolrInputDocument] = {
    if (digest.isEmpty) {
      return None;
    } else {
      val doc = new SolrInputDocument;
      /* set the fields */
      val uuri = UURIFactory.getInstance(url);
      updateFields(doc,
                   ARCNAME_FIELD        -> filename,
                   ID_FIELD             -> "%s.%s".format(uuri.toString, digest.getOrElse("-")),
                   DIGEST_FIELD         -> digest,
                   DATE_FIELD           -> date,
                   TITLE_FIELD          -> title,
                   CONTENT_LENGTH_FIELD -> length,
                   CONTENT_FIELD        -> content);
      updateDocBoost(doc, 1.0f);
      updateDocUrls(doc, url);
      updateContentType(doc, detectedContentType, suppliedContentType);
      return Some(doc);
    }
  }
}
