/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.UUID;

import org.junit.runner.RunWith;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.scalatest.junit.JUnitRunner;

import org.cdlib.was.weari._;

@RunWith(classOf[JUnitRunner])
class QuickIdFilterSpec extends FeatureSpec {
  feature ("Quick id filters") {
    scenario ("should work without a solr server") {
      val filter = new QuickIdFilter(null, null);
      for (n <- 1.until(9999999)) {
        val uuid = UUID.randomUUID.toString;
        filter.add(uuid);
        assert(filter.contains(uuid));
      }
    }
  }
}
