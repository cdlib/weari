/* Copyright (c) 2009-2012 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.File;

import com.typesafe.config.ConfigFactory;

class Config {
  val confRoot = ConfigFactory.load();
  confRoot.checkValid(ConfigFactory.defaultReference(), "weari");

  val conf = confRoot.getConfig("weari");

  /* number of threads to use for ConcurrentUpdateSolrServer */
  val threadCount = conf.getInt("threadCount");

  /* size of queue for ConcurrentUpdateSolrServer */
  val queueSize = conf.getInt("queueSize");

  val jsonBaseDir = conf.getString("jsonBaseDir");

  /* threshold of tracked merged documents at which we should commit */
  val trackCommitThreshold = conf.getInt("trackCommitThreshold");

  /* maximum number of id queries to send to the server at once (id:A OR id:B ... )*/
  val maxIdQuerySize = conf.getInt("maxIdQuerySize");

  /* size of groups to send to the batch merge at once */
  val batchMergeGroupSize = conf.getInt("batchMergeGroupSize")
}
