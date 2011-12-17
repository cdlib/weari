/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.util.Date;

import org.archive.net.UURIFactory;
import java.io.{File,InputStreamReader,Writer};

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.util.ClientUtils;

import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateDocUrls,updateContentType,updateFields};

/**
 * A class representing a WASArchiveRecord that has been parsed.
 */
case class ParsedArchiveRecord (
  /* being a case class makes this easy to serialize as JSON */
  val filename : String,
  val digest : Option[String],
  val url : String,
  val date : Date,
  val title : Option[String],
  val length : Long,
  val content : Option[String],
  val suppliedContentType : ContentType,
  val detectedContentType : Option[ContentType],
  val outlinks : Seq[Long]) extends WASArchiveRecord {

  def getFilename = filename;
  def getDigestStr = digest;
  def getUrl = url;
  def getDate = date;
  def getLength = length;
  def getStatusCode = 200;
  def isHttpResponse = true;
  def getContentType = suppliedContentType;

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

object ParsedArchiveRecord {
  def apply(rec : WASArchiveRecord) : ParsedArchiveRecord =
    apply(rec, None, None, None, Seq[Long]());

  def apply(rec : WASArchiveRecord,
            content  : Option[String],
            detectedContentType : Option[ContentType],
            title    : Option[String],
            outlinks : Seq[Long]) = {
    val suppliedContentType = rec.getContentType;
    new ParsedArchiveRecord(filename = rec.getFilename,
                            digest = rec.getDigestStr,
                            url = rec.getUrl,
                            date = rec.getDate,
                            title = title,
                            length = rec.getLength,
                            content = if (shouldIndexContentType(suppliedContentType) ||
                                          shouldIndexContentType(detectedContentType.getOrElse(ContentType.DEFAULT))) {
                              content;
                            } else { 
                              None;
                            },
                            suppliedContentType = suppliedContentType,
                            detectedContentType = detectedContentType,
                            outlinks = outlinks);
  }
}
