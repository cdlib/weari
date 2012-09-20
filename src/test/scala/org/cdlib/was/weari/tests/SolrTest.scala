/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import org.scalatest._;
import org.scalatest.matchers._;

import org.cdlib.was.weari._;

class SolrTest extends FunSpec with BeforeAndAfter with ShouldMatchers with RequiresRunningContentApi {
  describe("solr") {
    it("should not return any results to start with") {
      val server = new HttpSolrServer("http://localhost:8700/solr");
      val docs = new SolrDocumentCollection(server, new SolrQuery("*:*").setRows(10));
      assert (docs.size === 0);
    }

    it("should index properly") {
      val config = new Config {};
      val weari = new Weari(config);
      weari.index("http://localhost:8700/solr", "*:*", List("CDL-20070613180402-00006-ingest1.arc.gz"), "", Map[String,Seq[String]]());
    }
  }
}
