/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.cdlib.was.weari._;

import org.scalatest.GivenWhenThen;

import com.typesafe.config.ConfigFactory;

class ParseTest extends FeatureSpec {
  val cl = classOf[ParseTest].getClassLoader;
  val config = new Config(ConfigFactory.load("test"));
  val arcName = "IAH-20080430204825-00000-blackbook.arc.gz";

  feature ("pig parser") {
    scenario ("can parse an ARC file") {
      val w = new Weari(config);
      w.deleteParse(arcName);
      w.parseArcs (List(cl.getResource(arcName).toString));
      assert (w.isArcParsed(arcName));
      val recs = w.pigUtil.readJson(w.pigUtil.getPath(arcName));
      assert (recs(0).url === "http://www.archive.org/robots.txt");
      assert (recs(0).detectedContentType === Some(ContentType("text", "plain", None)));
    }
  }
}
