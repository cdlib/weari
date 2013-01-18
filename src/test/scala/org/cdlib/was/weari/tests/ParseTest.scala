/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.{ FileSystem, Path };

import org.cdlib.was.weari._;

import org.scalatest.{ FeatureSpec, GivenWhenThen, Ignore };

import com.typesafe.config.ConfigFactory;

class ParseTest extends FeatureSpec {
  val cl = classOf[ParseTest].getClassLoader;
  val config = new Config(ConfigFactory.load("test"));
  val arcname = "IAH-20080430204825-00000-blackbook.arc.gz";
  val arcpath = cl.getResource(arcname).toString;
  val warcname = "IAH-20080430204825-00000-blackbook.warc.gz";
  val warcpath = cl.getResource(warcname).toString;
  val emptyarcname = "CDL-20121105000015-00000-oriole.ucop.edu-00343531.arc.gz";
  val emptyarcpath = cl.getResource(emptyarcname).toString;

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
    
    scenario ("an empty ARC file should be parsed") {
      w.deleteParse(emptyarcname);
      w.parseArcs(List(emptyarcpath));
      assert (w.isArcParsed(emptyarcname));
      val recs = w.pigUtil.readJson(w.pigUtil.getPath(emptyarcname));
      assert (recs.size === 0);
    }
    
    scenario ("cleans up after itself") {
      val hadoopConfig = new Configuration();
      val fs = FileSystem.get(new URI("file:///"), hadoopConfig);
      val tmpDir = new Path("%s/tmp".format(config.jsonBaseDir));
      assert (fs.listStatus(tmpDir).size === 0);
    }
  }
}
