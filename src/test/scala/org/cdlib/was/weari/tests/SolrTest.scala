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
  val solrurl = "http://localhost:8983/solr";
  val server = new HttpSolrServer(solrurl);

  def mkSearch(query : String) = 
      new SolrDocumentCollection(server, new SolrQuery(query));

  def assertSearchSize(query : String, size : Int) =
    assert(size === mkSearch(query).size);

  describe("solr") {
    it("should not return any results to start with") {
      assertSearchSize("*:*", 0);
    }
    
    it("should index properly") {
      val config = new Config;
      if (!w.isArcParsed(arcname)) {
        w.parseArcs(List(arcpath));
      }
      w.index(solrurl, "*:*", List(arcname), "", Map[String,Seq[String]]());
    }
    
    it("should have indexed images") {
      assertSearchSize("mediatypegroupdet:image", 55);
    }

    it("can set some tags") {
      assertSearchSize("tag:hello", 0);
      w.setFields(solrurl, "arcname:%s".format(arcname), Map("tag"->List("hello", "world")));
      assertSearchSize("tag:hello", 214);
      assertSearchSize("tag:world", 214);
    }
      
    it("should be able to remove arcs") {
      w.remove(solrurl, List(arcname));
      assertSearchSize("*:*", 0);
    }
  }
}
