/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.cdlib.was.weari._;

import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};

case class ValTestCaseClass(val intVal: Int, val strVal: String);
//    extends JsonSerializer

object ValTestCaseClassJson extends JsonDeserializer[ValTestCaseClass] {
  override val jsonType = manifest[ValTestCaseClass];
}

class JsonTest extends FeatureSpec {

  feature ("JSON") {
    scenario ("serialize JSON") {
      // val foo = ValTestCaseClass(1, "foo");
      // assert((foo.toJsonString == "{\"intVal\":1,\"strVal\":\"foo\"}")
      //   || (foo.toJsonString == "{\"strVal\":\"foo\",\"intVal\":1}"))
    }

    scenario("deserialize JSON") {
      val s = "{\"strVal\":\"foo\",\"intVal\":1}";
      val o1 = ValTestCaseClassJson.deserializeJson(s);
      val o2 = ValTestCaseClass(1,"foo");
      assert(o1 === o2);
    }
  }
}
