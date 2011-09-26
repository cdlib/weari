/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.util.Date;

import net.liftweb.json._;
import net.liftweb.json.Serialization.write;

import org.archive.net.UURIFactory;
import java.io.{BufferedWriter,File,FileOutputStream,OutputStreamWriter,Writer};

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.util.ClientUtils;

import org.cdlib.was.ngIndexer.SolrFields._;
import org.cdlib.was.ngIndexer.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateDocUrls,updateContentType,updateFields};

class IndexResource (
  val filename : String,
  val digest : String,
  val url : String,
  val date : Date,
  val title : String,
  val length : Long,
  val content : Option[String],
  val suppliedContentType : ContentType,
  val detectedContentType : ContentType) {
  
  /* for json */
  implicit val formats = Serialization.formats(NoTypeHints);

  private def toMap = 
    Map("filename" -> filename,
        "digest"   -> digest,
        "url"      -> url,
        "date"     -> date,
        "title"    -> title,
        "length"   -> length,
        "content"  -> content.getOrElse(""));

  def toJson : String = {
    return write(toMap);
  }

  def writeJson (w : Writer) {
    write(toMap, w);
  }

  def toDocument : SolrInputDocument = {
    val doc = new SolrInputDocument;
    /* set the fields */
    val uuri = UURIFactory.getInstance(url);
    updateFields(doc,
                 ARCNAME_FIELD        -> filename,
                 ID_FIELD             -> "%s.%s".format(uuri.toString, digest),
                 DIGEST_FIELD         -> digest,
                 DATE_FIELD           -> date,
                 TITLE_FIELD          -> title,
                 CONTENT_LENGTH_FIELD -> length,
                 CONTENT_FIELD        -> content);
    updateDocBoost(doc, 1.0f);
    updateDocUrls(doc, url);
    updateContentType(doc, detectedContentType, suppliedContentType);
    return doc;
  }
}

object IndexResource {
  def apply(rec : IndexArchiveRecord,
            parseResult : MyParseResult) : IndexResource = {
    new IndexResource(filename = rec.getFilename,
                      digest = rec.getDigestStr.getOrElse("-"),
                      url = rec.getUrl,
                      date = rec.getDate,
                      title = parseResult.title.getOrElse(""),
                      length = rec.getLength,
                      content = if (shouldIndexContentType(rec)) {
                        parseResult.content;
                      } else { 
                        None;
                      },
                      suppliedContentType = new ContentTypeImpl(rec),
                      detectedContentType = new ContentTypeImpl(parseResult));

  }
}
