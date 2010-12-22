package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.impl.{CommonsHttpSolrServer,StreamingUpdateSolrServer};
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

/** A class for handling a distributed solr system.
  *
  * Uses a consistent hash of servers.
  */

class SolrDistributedServer (servers : Seq[Tuple3[String,String,Int]]) {
  val ring = new ConsistentHashRing[CommonsHttpSolrServer];
  var serverList = List[CommonsHttpSolrServer]();

  for ((id, url, level) <- servers) {
    val server = new StreamingUpdateSolrServer (url, 50, 5);
    ring.addServer(id, server, level);
    serverList = server :: serverList;
  }
  
  def add (doc : SolrInputDocument) {
    val id = doc.getField(SolrIndexer.ID_FIELD).getValue.asInstanceOf[String];
    val server = ring.getServerFor(id);
    /* we need to store the server this is indexed on */
    doc.addField(SolrIndexer.SERVER_FIELD, server.getBaseURL);
    server.add(doc);
  }

  def commit {
    for (server <- serverList) {
      server.commit;
    }
  }

  def getShards : String = 
    return serverList.map(_.getBaseURL).map(_.substring(7)).mkString("",",","");

  def query (q : ModifiableSolrParams) : QueryResponse = {
    q.set("shards", getShards);
    return serverList.first.query(q);
  }
}
