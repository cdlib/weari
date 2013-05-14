package org.cdlib.was.weari;

import org.json4s._;
import org.json4s.jackson.JsonMethods.parse;
import org.json4s.Extraction.extract;
import org.json4s.jackson.Serialization;
import org.json4s.jackson.Serialization.{read, write};

import java.io.InputStream;

trait JsonDeserializer[T] {
  implicit val jsonType: Manifest[T];

  implicit val formats = Serialization.formats(NoTypeHints);

  def deserializeJson(value: InputStream) : T = 
    extract(parse(value, false));

  def deserializeJson(value: String) : T = {
    read[T](value);
  }
}

trait JsonSerializer {
  implicit val formats = Serialization.formats(NoTypeHints);

  def writeJson(writer : java.io.Writer) {
    writer.write(this.toJsonString);
  }

  def toJsonString: String = write(this);
}