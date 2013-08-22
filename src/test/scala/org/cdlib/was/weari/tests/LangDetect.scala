/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.{ FileSystem, Path };

import org.apache.pig.{ ExecType, PigServer };
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.{ PigContext };
import org.apache.pig.impl.util.PropertiesUtil;

import scala.collection.JavaConversions.asScalaIterator;


import org.cdlib.was.weari._;

import org.scalatest.FeatureSpec;

import com.typesafe.config.ConfigFactory;

class LangDetectTest extends FeatureSpec {
  val cl = classOf[LangDetectTest].getClassLoader;
  val config = new Config(ConfigFactory.load("test"));

  def testLanguageDetection (lang : String) {
    val w = new Weari(config);
    val pigServer = w.pigUtil.mkPigServer;
    val path = w.pigUtil.writeInputStreamToTempFile(cl.getResourceAsStream("%s.txt".format(lang)));
    w.pigUtil.setupClasspath(pigServer);

    pigServer.registerQuery("DATA = LOAD '%s' USING TextLoader();".format(path));
    pigServer.registerQuery("LANG = FOREACH DATA GENERATE org.cdlib.was.weari.pig.LANGDETECT($0);");
    val it = pigServer.openIterator("LANG");
    assert(it.next().get(0) === lang);
  }

  feature ("language detection") {
    scenario ("works for french") {
      testLanguageDetection("fr");
    }

    scenario ("works for german") {
      testLanguageDetection("de");
    }

    scenario ("works for English") {
      testLanguageDetection("en");
    }
  }
}
