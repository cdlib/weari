package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.{CommonsHttpSolrServer,StreamingUpdateSolrServer}
import org.apache.solr.common.SolrInputDocument;

/** A class for handling a distributed solr system.
  * Uses a consistent hash of servers.
  */

class SolrDistributedServer (servers : Seq[Pair[String,Int]]) {
  val ring = new ConsistentHashRing[CommonsHttpSolrServer];

  for ((url, level) <- servers) {
    val server = new StreamingUpdateSolrServer (url, 50, 5);
    ring.addServer(url, server, level);
  }
  
  def add (doc : SolrInputDocument) {
    val id = doc.getField(SolrIndexer.ID_FIELD).getValue.asInstanceOf[String];
    val server = ring.getServerFor(id);
    /* we need to store the server this is indexed on */
    doc.addField(SolrIndexer.SERVER_FIELD, server.getBaseURL);
    server.add(doc);
  }

  def commit {
    for (server <- ring.getServers) {
      server.commit;
    }
  }
}
