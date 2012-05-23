/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Date;

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

  @JsonIgnore
  lazy val canonicalUrl = UriUtils.canonicalize(url);
  @JsonIgnore
  lazy val urlFingerprint = UriUtils.fingerprint(this.canonicalUrl);
  @JsonIgnore
  lazy val canonicalHost = UriUtils.string2uuri(canonicalUrl).getHost;
}

object ParsedArchiveRecord {
  def apply(rec : WASArchiveRecord) : ParsedArchiveRecord =
    apply(rec, None, None, None, Seq[Long]());

  def apply(rec : WASArchiveRecord,
            content : Option[String],
            detectedContentType : Option[ContentType],
            title : Option[String],
            outlinks : Seq[Long]) = {
    val suppliedContentType = rec.getContentType;
    new ParsedArchiveRecord(filename = rec.getFilename,
                            digest = rec.getDigestStr,
                            url = rec.getUrl,
                            date = rec.getDate,
                            title = title,
                            length = rec.getLength,
                            content = content,
                            suppliedContentType = suppliedContentType,
                            detectedContentType = detectedContentType,
                            outlinks = outlinks);
  }
}
