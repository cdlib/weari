package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.{CommonsHttpSolrServer,StreamingUpdateSolrServer};
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.{ModifiableSolrParams,SolrParams};

/** A class for handling a distributed solr system.
  *
  * Uses a consistent hash of servers.
  */
class SolrDistributedServer (serverInit : Seq[Tuple3[String,String,Int]],
                             queueSize : Int = 50,
                             queueRunners : Int = 5) {
  val ring = new ConsistentHashRing[CommonsHttpSolrServer];
  var servers = scala.collection.mutable.Map[String,CommonsHttpSolrServer]();

  for ((id, url, level) <- serverInit) {
    val server = new StreamingUpdateSolrServer (url, queueSize, queueRunners);
    ring.addServer(id, server, level);
    servers += (server.getBaseURL -> server);
  }
  
  def add (doc : SolrInputDocument) {
    val id = doc.getFieldValue(SolrProcessor.ID_FIELD).asInstanceOf[String];
    val server = ring.getServerFor(id);
    /* we need to store the server this is indexed on */
    doc.addField(SolrProcessor.SERVER_FIELD, server.getBaseURL);
    server.add(doc);
  }

  def commit {
    for (server <- servers.values) server.commit;
  }

  /** Gets the string to use when submitting a shards query param
    */
  def getShardsValue : String = 
    return servers.values.map(_.getBaseURL).map(_.substring(7)).mkString("",",","");

  def query (q : SolrParams) : QueryResponse = {
    val q2 = new ModifiableSolrParams(q);
    q2.set("shards", getShardsValue);
    if (q2.get("qt") == "/terms") {
      q2.set("shards.qt", "/terms");
    }
    return servers.values.head.query(q2);
  }

  def getById(id : String) : Option[SolrDocument] = {
    val q = new SolrQuery;
    q.setQuery("id:\"%s\"".format(id));
    try {
      return Some((new SolrDocumentCollection(this, q)).head);
    } catch {
      case ex : NoSuchElementException => {
        return None;
      }
    }
  }

  def deleteById(id : String) {
    // val result = getById(id).get;
    // val serverName = result.getFirstValue(SolrProcessor.SERVER_FIELD).asInstanceOf[String];
    // val server = servers.get(serverName).get;
    // server.deleteById(id);
    // server.commit;
  }
}
