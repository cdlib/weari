package org.cdlib.was.ngIndexer;

import org.cdlib.ssconf.SSConfig;

trait Config extends SSConfig {
  val indexer : Value[SolrIndexer] = new Value[SolrIndexer];
}
    
