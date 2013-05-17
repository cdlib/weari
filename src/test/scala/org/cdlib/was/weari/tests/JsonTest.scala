/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.cdlib.was.weari._;

import org.joda.time._;
import org.json4s._;

import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};

case class ValTestCaseClass(val intVal: Int, val strVal: String)
  extends JsonSerializer;

object ValTestCaseClassJson extends JsonDeserializer[ValTestCaseClass] {
  override val jsonType = manifest[ValTestCaseClass];
}

case class DateCaseClass(val date : DateTime) 
  extends JsonSerializer;

object DateCaseClassJson extends JsonDeserializer[DateCaseClass] {
  override val jsonType = manifest[DateCaseClass];
}

class JsonTest extends FeatureSpec {

  feature ("JSON") {
    scenario ("serialize JSON") {
      val foo = ValTestCaseClass(1, "foo");
      val fooJson = foo.toJsonString;
      assert((fooJson == """{"intVal":1,"strVal":"foo"}""")
        || (fooJson == """{"strVal":"foo","intVal":1}"""));
    }

    scenario("deserialize JSON") {
      val s = "{\"strVal\":\"foo\",\"intVal\":1}";
      val o1 = ValTestCaseClassJson.deserializeJson(s);
      val o2 = ValTestCaseClass(1,"foo");
      assert(o1 === o2);
    }

    scenario("de/serialize joda to json") {
      val o = DateCaseClass(new DateTime(1368771502525L, DateTimeZone.UTC));
      assert(o.toJsonString === """{"date":1368771502525}""");
      assert(o === DateCaseClassJson.deserializeJson(o.toJsonString));
    }
  }
}
