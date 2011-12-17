/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import com.codahale.jerkson.Json;

import org.junit.runner.RunWith;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.scalatest.junit.JUnitRunner;

import org.cdlib.was.weari._;

@RunWith(classOf[JUnitRunner])
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
  
  feature ("We can serialize to JSON") {
    scenario ("round trip text/plain") {
      val ct = ContentType("text", "plain", None);
      assert (Json.parse[ContentType](Json.generate(ct)) == ct);
    }

    scenario ("round trip text/plain with charset") {
      val ct = ContentType("text", "plain", Some("utf-8"));
      assert (Json.parse[ContentType](Json.generate(ct)) == ct);
    }

    scenario ("round trip application/pdf") {
      val ct = ContentType("application", "pdf", None);
      assert (Json.parse[ContentType](Json.generate(ct)) == ct);
    }
  }
}
