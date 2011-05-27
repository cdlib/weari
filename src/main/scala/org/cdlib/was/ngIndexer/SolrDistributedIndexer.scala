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
                             queueSize : Int = 1000,
                             queueRunners : Int = 3,
                             commitThreshold : Int = 10000) extends Logger {
  val commitLock = new Object;
  var commitCounter = 0;
  
  val ring = new ConsistentHashRing[CommonsHttpSolrServer];
  var servers = scala.collection.mutable.Map[String,CommonsHttpSolrServer]();

  for ((id, url, level) <- serverInit) {
    val server = new StreamingUpdateSolrServer (url, queueSize, queueRunners);
    ring.addServer(id, server, level);
    servers += (server.getBaseURL -> server);
  }
  
  def add (doc : SolrInputDocument, key : String) {
    val server = ring.getServerFor(key);
    server.add(doc);
    commitCounter = commitCounter + 1;
  }

  /** Send commit to servers. */
  def commit {
    commitLock.synchronized {
      logger.info("Commiting.");
      for (server <- servers.values) server.commit;
    }
  }

  /** Commit if threshold met. */
  def maybeCommit {
    if (commitCounter > commitThreshold) {
      commitLock.synchronized {
        /* somebody else may have committed while we were waiting, check */
        if (commitCounter > commitThreshold) {
          commitCounter = 0;
          commit;
        }
      }
    }
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
}
