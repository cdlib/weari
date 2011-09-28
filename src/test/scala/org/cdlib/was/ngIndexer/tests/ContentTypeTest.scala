/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.junit.runner.RunWith;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.scalatest.junit.JUnitRunner;

import org.cdlib.was.ngIndexer._;

@RunWith(classOf[JUnitRunner])
class ContentTypeSpec extends FeatureSpec {
  feature ("We can parse Content-Type headers.") {
    scenario ("text/plain") {
      val str = "text/plain";
      ContentType.parse(str) match {
        case Some(ContentType("text", "plain", None)) => assert(true);
        case _ => assert(false);
      }
    }

    scenario ("text/plain; charset=utf-8") {
      val str = "text/plain; charset=utf-8";
      ContentType.parse(str) match {
        case Some(ContentType("text", "plain", Some("utf-8"))) => assert(true);
        case _ => assert(false);
      }
    }
  }
}
