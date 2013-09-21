/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari;

import java.io.File;

import com.typesafe.config.{ ConfigFactory, Config => TSConfig };

class Config (confRoot : TSConfig) {
  def this() = this (ConfigFactory.load());

  confRoot.checkValid(ConfigFactory.defaultReference(), "weari");

  val conf = confRoot.getConfig("weari");

  val port = conf.getInt("port");

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
  
  /* number of docs to load at once */
  val numDocsPerRequest = conf.getInt("numDocsPerRequest");
  
  /* number of doc ids to load at once */
  val numDocIdsPerRequest = conf.getInt("numDocIdsPerRequest");

  val commitBetweenArcs = conf.getBoolean("commitBetweenArcs");

  val commitThreshold = conf.getInt("commitThreshold");

  val useHadoop = conf.getBoolean("useHadoop");

  val useRealTimeGet = conf.getBoolean("useRealTimeGet");

  val useAtomicUpdates = conf.getBoolean("useAtomicUpdates");

  val batchArcParseSize = conf.getInt("batchArcParseSize");

  val solrServer = conf.getString("solrServer");

  val solrZkHost = conf.getString("solrZkHost");

  val solrCollection = conf.getString("solrCollection");
}
