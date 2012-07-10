/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import org.scalatest._;
import org.scalatest.matchers._;

import org.cdlib.was.weari._;

class SolrTest extends FunSpec with BeforeAndAfter with ShouldMatchers {
  describe("weari") {
    it("should parse arcs") {
      val config = new Config {};
      val weari = new Weari(config);
      weari.parseArcsLocal(List[String]("http://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.arc.gz"));
    }
  }
}
