/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import org.scalatest._;
import org.scalatest.matchers._;

import org.cdlib.was.weari._;

class SolrTest extends FunSpec with BeforeAndAfter with ShouldMatchers {
  describe("solr") {
    it("should not return any results to start with") {
      val server = new HttpSolrServer("http://localhost:8983/solr");
      val docs = new SolrDocumentCollection(server, new SolrQuery("*:*").setRows(10));
      assert (docs.size === 0);
    }
  }
}
