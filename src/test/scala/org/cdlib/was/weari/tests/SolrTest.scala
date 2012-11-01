/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import org.scalatest._;
import org.scalatest.matchers._;

import org.cdlib.was.weari._;

import com.typesafe.config.ConfigFactory;

class SolrTest extends FunSpec with ShouldMatchers {
  val cl = classOf[ParseTest].getClassLoader;
  val config = new Config(ConfigFactory.load("test"));
  val w = new Weari(config);
  val arcname = "IAH-20080430204825-00000-blackbook.arc.gz";
  var arcpath = cl.getResource(arcname).toString;
  val server = new HttpSolrServer("http://localhost:8983/solr");

  describe("solr") {
    it("should not return any results to start with") {
      val docs = new SolrDocumentCollection(server, new SolrQuery("*:*").setRows(10));
      assert (docs.size === 0);
    }
    
    it("should index properly") {
      val config = new Config;
      if (!w.isArcParsed(arcname)) {
        w.parseArcs(List(arcpath));
      }
      w.index("http://localhost:8983/solr", "*:*", 
              List(arcname), "", Map[String,Seq[String]]());
    }
    
    it("should have indexed images") {
      val docs = new SolrDocumentCollection(server, new SolrQuery("mediatypegroupdet:image").setRows(10));
      assert (docs.size === 55);
    }

    it("should be able to remove arcs") {
      w.remove("http://localhost:8983/solr", List(arcname));
      assert (0 === new SolrDocumentCollection(server, new SolrQuery("*:*")).size);
    }
  }
}
