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

  describe("basic indexing") {
    ignore("should not return any results to start with") {
      assertSearchSize("*:*", 0);
    }
    
    ignore("should index properly") {
      if (!w.isArcParsed(arcname)) {
        w.parseArcs(List(arcpath));
      }
      w.index(List(arcname), "");
    }

    ignore("reindexing should work") {
      val docsa = mkSearch("*:*").toList.sortBy(_.getFirstValue("id").asInstanceOf[String])
      w.index(List(arcname), "");
      val docsb = mkSearch("*:*").toList.sortBy(_.getFirstValue("id").asInstanceOf[String])
      for ((a, b) <- docsa.zip(docsb);
           field <- List("id", "date", "content", "url", "canonicalurl", "title", "mediatypedet", "mediatypesup")) {
             assert (a.getFieldValue(field) === b.getFieldValue(field));
           }
    }

    ignore("should have indexed images") {
      assertSearchSize("mediatypegroupdet:image", 55);
    }

    ignore("can set some tags") {
      assertSearchSize("tag:hello", 0);
      w.setFields("arcname:%s".format(arcname), Map("tag"->List("hello", "world")));
      assertSearchSize("tag:hello", 214);
      assertSearchSize("tag:world", 214);
      assertSearchSize("tag:\"hello world\"", 0);
    }
      
    ignore("should be able to remove arcs") {
      w.remove(List(arcname));
      assertSearchSize("*:*", 0);
    }

    ignore("threaded indexing should work") {
      val threads = for (i <- (1 to 10))
                    yield {
                      val t = new Thread {
                        override def run {
                          val is = i.toString;
                          assert(w.isLocked === false);
                          w.index(List(arcname), is, Map("job"->List(is)));
                          assert(w.isLocked === false);
                        }
                      }
                      t.start;
                      t;
                    }
      threads.map(_.join);
      
      for (i <- (1 to 10)) {
        assertSearchSize("job:%s".format(i.toString), 214);
      }
      assertSearchSize("*:*", 214 * 10);
      w.remove(List(arcname));
    }

    ignore("threaded indexing should lock on the same extraId") {
      val threads = for (i <- (1 to 2))
                    yield {
                      val t = new Thread {
                        val is = i.toString;
                        override def run {
                          w.index(List(arcname), "XXX");
                        }
                      }
                      t;
                    }
      assert(w.isLocked === false);
      threads.map(_.start);
      /* this depends on the threads not being finished, which should be true */
      assert(w.isLocked === true);
      threads.map(_.join);
      assert(w.isLocked === false);
      w.remove(List(arcname));
    }
  }
}
