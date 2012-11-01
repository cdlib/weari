/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.cdlib.was.weari._;

import org.scalatest.{ FeatureSpec, GivenWhenThen };

import com.typesafe.config.ConfigFactory;

class ParseTest extends FeatureSpec {
  val cl = classOf[ParseTest].getClassLoader;
  val config = new Config(ConfigFactory.load("test"));
  val arcname = "IAH-20080430204825-00000-blackbook.arc.gz";
  var arcpath = cl.getResource(arcname).toString;
  val warcname = "IAH-20080430204825-00000-blackbook.warc.gz";
  var warcpath = cl.getResource(warcname).toString;
  val w = new Weari(config);

  feature ("pig parser") {
    scenario ("can parse an ARC file") {
      w.deleteParse(arcname);
      w.parseArcs(List(arcpath))
      assert (w.isArcParsed(arcname));
      val recs = w.pigUtil.readJson(w.pigUtil.getPath(arcname));
      assert (recs(0).url === "http://www.archive.org/robots.txt");
      assert (recs(0).detectedContentType === Some(ContentType("text", "plain", None)));
    }

    scenario ("can parse a WARC file") {
      w.deleteParse(warcname);
      w.parseArcs(List(warcpath));
      assert (w.isArcParsed(warcname));
      val recs = w.pigUtil.readJson(w.pigUtil.getPath(arcname));
      assert (recs(0).url === "http://www.archive.org/robots.txt");
      assert (recs(0).detectedContentType === Some(ContentType("text", "plain", None)));
    }
  }
}
