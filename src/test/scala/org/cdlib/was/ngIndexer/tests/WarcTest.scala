/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.cdlib.was.ngIndexer._;

import org.junit.runner.RunWith;

import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};
import org.scalatest.junit.JUnitRunner;

@RunWith(classOf[JUnitRunner])
class WarcSpec extends FeatureSpec {
  val cl = classOf[WarcSpec].getClassLoader;

  feature ("We can read a WARC file.") {
    scenario ("We are reading a WARC file.") {
      val arcName = "IAH-20080430204825-00000-blackbook.warc.gz";
      val is = cl.getResourceAsStream(arcName);
      Utility.eachRecord (is, arcName) { (rec)=>
        if (rec.getUrl == "http://www.archive.org/images/logoc.jpg") {
          assert (rec.getDigestStr === Some("UZY6ND6CCHXETFVJD2MSS7ZENMWF7KQ2"));
        }
      }
    }
  }
}
