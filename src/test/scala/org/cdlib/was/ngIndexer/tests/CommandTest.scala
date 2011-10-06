/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.cdlib.was.ngIndexer._;

import org.junit.runner.RunWith;

import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};
import org.scalatest.junit.JUnitRunner;

@RunWith(classOf[JUnitRunner])
class CommandSpec extends FeatureSpec {
  feature ("We can parse JSON commands.") {
    scenario ("Parse command") {
      val str = """ [ { "command" : "parse", "uri" : "http://example.org/ARC.arc.gz", "jsonpath" : "/tmp/json" } ] """;
      Command.parse(str) match {
        case (p : ParseCommand) :: Nil => assert (p.uri == "http://example.org/ARC.arc.gz");
        case x @ _ => {
          System.err.println(x);
          assert(false);
        }
      }
    }
  }
}
