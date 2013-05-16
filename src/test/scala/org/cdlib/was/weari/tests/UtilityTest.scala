/* (c) 2009-2013 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.cdlib.was.weari._;

import org.scalatest.{ FeatureSpec, GivenWhenThen, Ignore };

import com.typesafe.config.ConfigFactory;

import org.joda.time.{ DateTime, DateTimeZone };

class UtilityTest extends FeatureSpec {
  val d = new DateTime(1368725942000L, DateTimeZone.UTC);
  
  feature ("date/string conversions") {
    scenario ("can convert a date to a string") {
      assert(Utility.date2string(d) === "2013-05-16T17:39:02Z");
    }
    scenario("can convert a w3cdtf string to a date") {
      assert(Utility.string2date("2013-05-16T17:39:02Z") === d);
    }
    scenario("can convert a 14 digit string to a date") {
      assert(Utility.string2date("20130516173902") === d);
    }
  }
}
