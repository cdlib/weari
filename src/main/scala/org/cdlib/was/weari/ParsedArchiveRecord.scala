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

import java.io.InputStream;

import java.util.Date;

import org.json4s.FieldSerializer;
import org.json4s.jackson.Serialization;
import org.json4s.NoTypeHints;

import com.typesafe.scalalogging.slf4j.Logging;

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
  val isRevisit : Option[Boolean],
  val outlinks : Seq[Long]) extends WASArchiveRecord with JsonSerializer {

  def getFilename = filename;
  def getDigest = digest;
  def getUrl = url;
  def getDate = date;
  def getLength = length;
  def getStatusCode = 200;
  def isHttpResponse = true;
  def getContentType = suppliedContentType;

  lazy val canonicalUrl = UriUtils.canonicalize(url);
  lazy val urlFingerprint = UriUtils.fingerprint(this.canonicalUrl);
  lazy val canonicalHost = UriUtils.string2handyUrl(canonicalUrl).getHost;
}

object ParsedArchiveRecord extends JsonDeserializer[ParsedArchiveRecord] with Logging {
  /* necessary for JsonDeserializer trait due to type erasure */
  override val jsonType = manifest[ParsedArchiveRecord];

  def apply(rec : WASArchiveRecord) : ParsedArchiveRecord =
    apply(rec, None, None, None, Seq[Long]());

  def apply(rec : WASArchiveRecord,
            content : Option[String],
            detectedContentType : Option[ContentType],
            title : Option[String],
            outlinks : Seq[Long]) = {
    val suppliedContentType = rec.getContentType;
    new ParsedArchiveRecord(filename = rec.getFilename,
                            digest = rec.getDigest,
                            url = rec.getUrl,
                            date = rec.getDate,
                            title = title,
                            length = rec.getLength,
                            content = content,
                            isRevisit = rec.isRevisit,
                            suppliedContentType = suppliedContentType,
                            detectedContentType = detectedContentType,
                            outlinks = outlinks);
  }
}

object ParsedArchiveRecordSeq extends JsonDeserializer[Seq[ParsedArchiveRecord]] with Logging {
  override val jsonType = manifest[Seq[ParsedArchiveRecord]];
}
