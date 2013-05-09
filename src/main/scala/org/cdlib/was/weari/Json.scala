package org.cdlib.was.weari;

import com.fasterxml.jackson.annotation._;
import com.fasterxml.jackson.core.`type`.TypeReference;
import com.fasterxml.jackson.databind.{Module, ObjectMapper};
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.lang.reflect.{Type, ParameterizedType}
import java.io.InputStream;

trait JsonIO {
  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
}

trait JsonDeserializer[T] extends JsonIO {
  implicit val jsonType: TypeReference[T];

  def deserializeJson(value: InputStream) : T = {
    try {
      objectMapper.readValue(value, jsonType);
    } finally {
      if (value != null) value.close;
    }
  }

  def deserializeJson(value: String) : T = {
    val is = new java.io.ByteArrayInputStream(value.getBytes());
    objectMapper.readValue(is, jsonType);
  }
}

trait JsonSerializer extends JsonIO {
  def writeJson(writer : java.io.Writer, value : ParsedArchiveRecord) {
    objectMapper.writeValue(writer, value);
  }

  def toJsonString: String = {
    import java.io.StringWriter
    val writer = new StringWriter()
    objectMapper.writeValue(writer, this)
    writer.toString
  }
}