/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.File;

import com.typesafe.config.ConfigFactory;

class Config {
  val confRoot = ConfigFactory.load();
  confRoot.checkValid(ConfigFactory.defaultReference(), "weari");

  val conf = confRoot.getConfig("weari");
  val threadCount = conf.getInt("threadCount");
  val queueSize = conf.getInt("queueSize");
  val jsonBaseDir = conf.getString("jsonBaseDir");
  val trackCommitThreshold = conf.getInt("trackCommitThreshold");
}
