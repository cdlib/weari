/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.zip.GZIPInputStream;

import org.cdlib.was.weari._;

import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};

class JsonTest extends FeatureSpec {
  val cl = classOf[WarcSpec].getClassLoader;
  val config = new Config {};
  val jsonName = "test.warc.gz.json.gz";
  val weari = new Weari(config);

  feature ("JSON") {
    scenario ("we can parse a JSON file") {
      val records = ParsedArchiveRecordSeq.deserializeJson(new GZIPInputStream(cl.getResourceAsStream(jsonName)));
      assert(records.length === 1);
      val record = records(0);
      assert (record.getUrl === "http://www.actorsequity.org/home.asp");
    }
  }
}
