/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.scalatest.junit.JUnitRunner;

import org.cdlib.was.weari._;

import org.cdlib.was.weari.MediaTypeGroup.groupWrapper;

class ContentTypeSpec extends FeatureSpec {
  feature ("We can parse Content-Type headers.") {
    scenario ("text/plain") {
      ContentType.parse("text/plain") match {
        case Some(ContentType("text", "plain", None)) => assert(true);
        case _ => assert(false);
      }
    }

    scenario ("text/plain; charset=utf-8") {
      ContentType.parse("text/plain; charset=utf-8") match {
        case Some(ContentType("text", "plain", Some("utf-8"))) => assert(true);
        case _ => assert(false);
      }
    }

    scenario ("bad line") {
      assert (ContentType.parse("xxx").isEmpty);
    }
  }
  
  feature ("Media Type Groups") {
    scenario ("media type groups") {
      assert(ContentType("text", "html", None).mediaTypeGroup === Some("html"));
      assert(ContentType("image", "XXXXX", None).mediaTypeGroup === Some("image"));
      assert(ContentType("video", "XXXXX", None).mediaTypeGroup === Some("video"));
      assert(ContentType("audio", "XXXXX", None).mediaTypeGroup === Some("audio"));
      assert(ContentType("application", "pdf", None).mediaTypeGroup === Some("pdf"));
      assert(ContentType("application", "zip", None).mediaTypeGroup === Some("compressed"));
      assert(ContentType("application", "XXXX", None).mediaTypeGroup === None);
    }
  }
}
