/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io.{File,FileReader};
import java.util.Date;

import net.liftweb.json.{DefaultFormats,JsonParser,NoTypeHints,Serialization}
import net.liftweb.json.JsonAST.JValue;

import org.archive.net.UURIFactory;
import java.io.{BufferedWriter,File,FileOutputStream,OutputStreamWriter,Writer};

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.util.ClientUtils;

import org.cdlib.was.ngIndexer.SolrFields._;
import org.cdlib.was.ngIndexer.SolrDocumentModifier.{shouldIndexContentType,updateDocBoost,updateDocUrls,updateContentType,updateFields};

/**
 * A class representing a WASArchiveRecord that has been parsed.
 */
case class ParsedArchiveRecord (
  val filename : String,
  val digest : String,
  val url : String,
  val date : Date,
  val title : String,
  val length : Long,
  val content : Option[String],
  val suppliedContentType : ContentType,
  val detectedContentType : ContentType) extends WASArchiveRecord {

  def getFilename = filename;
  def getDigestStr = Some(digest);
  def getUrl = url;
  def getDate = date;
  def getLength = length;
  def getStatusCode = 200;
  def isHttpResponse = true;
  def getContentType = suppliedContentType;

  private def toMap = {
    var m = Map("filename" -> filename,
                "digest"   -> digest,
                "url"      -> url,
                "date"     -> date,
                "suppliedContentType" -> suppliedContentType,
                "detectedContentType" -> detectedContentType,
                "title"    -> title,
                "length"   -> length);
    if (content.isDefined) { m += ("content") -> content.get }
    m;
  }

  /* for json */
  implicit val formats = DefaultFormats;

  def toJson : String = {
    return Serialization.write(toMap);
  }

  def writeJson (w : Writer) {
    Serialization.write(toMap, w);
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

object ParsedArchiveRecord {
  /* for json */
  implicit val formats = DefaultFormats;

  def fromJson (j : JValue) = j.extract[ParsedArchiveRecord];

  def fromJson (in : String) : ParsedArchiveRecord = 
    fromJson(JsonParser.parse(in));
  
  def parse (file : File) : ParsedArchiveRecord =
    fromJson(JsonParser.parse(new FileReader(file), true));

  def apply(rec : WASArchiveRecord,
            parseResult : MyParseResult) : ParsedArchiveRecord = {
    new ParsedArchiveRecord(filename = rec.getFilename,
                      digest = rec.getDigestStr.getOrElse("-"),
                      url = rec.getUrl,
                      date = rec.getDate,
                      title = parseResult.title.getOrElse(""),
                      length = rec.getLength,
                      content = if (shouldIndexContentType(rec.getContentType)) {
                        parseResult.content;
                      } else { 
                        None;
                      },
                      suppliedContentType = rec.getContentType,
                      detectedContentType = parseResult.contentType);

  }
}
