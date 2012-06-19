/* (c) 2009-2012 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.UUID;

import org.scalatest._;
import org.scalatest.matchers._;

import org.cdlib.was.weari.Config;

class ConfigSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {
  describe("config system") {
    it("should load") {
      val config = new Config;
    }
    
    it("should have the right defaults") {
      val config = new Config;
      assert(config.queueSize === 1000);
    }

    it("should be overridden by an application.conf in the classpath") {
      val config = new Config;
      assert(config.threadCount === 10);
    }
  }
}
