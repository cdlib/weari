/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.zip.GZIPInputStream;

import org.cdlib.was.weari._;

import org.scalatest.FeatureSpec;

class ParsedArchiveRecordTest extends FeatureSpec {
  val cl = classOf[ParsedArchiveRecordTest].getClassLoader;
  val config = new Config {};
  val jsonName = "test.warc.gz.json.gz";
  val weari = new Weari(config);

  feature ("ParsedArchiveRecord") {
    scenario ("can parse a JSON record") {
      val json = """{
"filename" : "CDL-20120529171053-00000-dp02-00025441.warc.gz",
"digest" : "64K7WUNMRDPUIYDMOV3Q3MUPVVK4F2T2",
"url" : "http://www.actorsequity.org/home.asp",
"date" : 1338311458000,
"title" : "Actors' Equity - Representing American Actors and Stage Managers in the Theatre",
"length" : 17086,
"content" : "Actors' Equity",
"suppliedContentType" : {"top" : "text","sub" : "html"},
"detectedContentType" : {"top" : "text","sub" : "html"},
"isRevisit" : false,
"outlinks" : [79601217693245147,204326234557181145,1230741338106938321,1662501621584289054,2088645020136017605,2255180460618605737,2373626944810009567,2570039139248822298,2922266654371116943,2924928916408419859,3256781520671374234,3321800361530713097,3439289050292423006,3901523628611711254,4504313446340626241,5052459132275693637,5121218988486157979,5548141020175383775,5727575041380438605,6853472079381973454,6855134712471976639,6989764569923718381,7686021261761412938,8743216438960871154,8776533273734997468,8928615062711307732]
}"""
      val record = ParsedArchiveRecord.deserializeJson(json);
      assert (record.getUrl === "http://www.actorsequity.org/home.asp");
    }

    scenario ("we can parse a JSON file") {
      val records = ParsedArchiveRecordSeq.deserializeJson(new GZIPInputStream(cl.getResourceAsStream(jsonName)));
      assert(records.length === 1);
      val record = records(0);
      assert (record.getUrl === "http://www.actorsequity.org/home.asp");
    }
  }
}
