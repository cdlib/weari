package org.cdlib.was.weari;

import org.json4s._;
import org.json4s.jackson.JsonMethods.parse;
import org.json4s.Extraction.extract;
import org.json4s.jackson.Serialization;
import org.json4s.jackson.Serialization.{read, write};

import java.io.InputStream;

import org.joda.time.{ DateTime, DateTimeZone };

/**
  * Serialize dates to integers.
  */
case object TimestampSerializer extends CustomSerializer[DateTime](format => (
  {
    case JInt(i) => new DateTime(i.longValue, DateTimeZone.UTC);
    case JNull => null;
  },
  { 
    case d : DateTime => JInt(d.getMillis);
  }
))

trait JsonDeserializer[T] {
  implicit val jsonType: Manifest[T];

  implicit val formats = DefaultFormats + TimestampSerializer

  def deserializeJson(value: InputStream) : T = 
    extract(parse(value, false));

  def deserializeJson(value: String) : T = {
    read[T](value);
  }
}

trait JsonSerializer {
  implicit val formats = DefaultFormats + TimestampSerializer

  def writeJson(writer : java.io.Writer) {
    writer.write(this.toJsonString);
  }

  def toJsonString: String = write(this);
}
