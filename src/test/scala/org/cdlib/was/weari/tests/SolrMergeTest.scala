/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import org.scalatest._;
import org.scalatest.matchers._;

import org.cdlib.was.weari._;

import com.typesafe.config.ConfigFactory;

class SolrMergeTest extends FunSpec with ShouldMatchers with BeforeAndAfter {
  val cl = classOf[ParseTest].getClassLoader;
  val config = new Config(ConfigFactory.load("test"));
  val w = new Weari(config);
  val solrurl = "http://localhost:8983/solr";
  val server = new HttpSolrServer(solrurl);
  val mergearc1 = "CDL-20100505172641-00000-kandinsky.cdlib.org-00018489.warc.gz";
  val mergearc2 = "CDL-20120529064748-00000-dp02-00025431.warc.gz";
  val mergearc3 = "CDL-20120529071722-00000-dp02-00025434.warc.gz";
  var mergearcpath1 = cl.getResource(mergearc1).toString;
  var mergearcpath2 = cl.getResource(mergearc2).toString;
  var mergearcpath3 = cl.getResource(mergearc3).toString;

  def mkSearch(query : String) = 
    new SolrDocumentCollection(server, new SolrQuery(query));

  def assertSearchSize(query : String, size : Int) =
    assert(size === mkSearch(query).size);

  def testResults (query : String, size : Int , dateSize : Map[String, Int]) {
    val results = mkSearch(query);
    assert(size === results.size);
    for (doc <- mkSearch(query)) {
      val id = doc.getFieldValue("id").asInstanceOf[String];
      val dates = doc.getFieldValues("date");
      assert (dates.size === dateSize.getOrElse(id, -1), "Bad date size for %s".format(id));
    }
  }

  before {
    if (!w.isArcParsed(mergearc1)) {
      w.parseArcs(List(mergearcpath1));
    }
    if (!w.isArcParsed(mergearc2)) {
      w.parseArcs(List(mergearcpath2));
    }
    if (!w.isArcParsed(mergearc3)) {
      w.parseArcs(List(mergearcpath3));
    }
    w.remove(solrurl, List(mergearc1, mergearc2, mergearc3));
  }
  
  after {
    w.remove(solrurl, List(mergearc1, mergearc2, mergearc3));
  }

  describe("merging") {
    ignore("should work") {
      assertSearchSize("*:*", 0);
      w.index(solrurl, "*:*", List(mergearc1), "XXX");
      testResults("*:*", 3,
        Map("http://gales.cdlib.org/robots.txt.MNSXZO35OCDMK2YM2TS4NGM3W2BWMSDI.XXX" -> 1,
            "http://gales.cdlib.org/.GNQD4SRUO7VSBGHTDQUO4AIWDG2PJ74M.XXX"-> 1,          
            "http://gales.cdlib.org/b-traven3.jpeg.ZKCMBC3DSMM3RYW4KFRJCQJZFR6G3C4J.XXX" -> 1))
      w.index(solrurl, "*:*", List(mergearc2), "XXX");
      /* one changed file, one file the same as before, one file (robots) missing */
      testResults("*:*", 4,
        Map("http://gales.cdlib.org/robots.txt.MNSXZO35OCDMK2YM2TS4NGM3W2BWMSDI.XXX" -> 1,
            "http://gales.cdlib.org/.GNQD4SRUO7VSBGHTDQUO4AIWDG2PJ74M.XXX"-> 1,          
            "http://gales.cdlib.org/.R4OI4U63VX5OM5NYDZPUECTERBGWOCLD.XXX" -> 1,
            "http://gales.cdlib.org/b-traven3.jpeg.ZKCMBC3DSMM3RYW4KFRJCQJZFR6G3C4J.XXX" -> 2))
    }
    
    ignore("should work with de-duplicated arcs") {
      assertSearchSize("*:*", 0);
      w.index(solrurl, "*:*", List(mergearc1, mergearc2, mergearc3), "XXX");
      testResults("*:*", 4,
        Map("http://gales.cdlib.org/robots.txt.MNSXZO35OCDMK2YM2TS4NGM3W2BWMSDI.XXX" -> 1,
            "http://gales.cdlib.org/.GNQD4SRUO7VSBGHTDQUO4AIWDG2PJ74M.XXX"-> 1,          
            "http://gales.cdlib.org/.R4OI4U63VX5OM5NYDZPUECTERBGWOCLD.XXX" -> 2,
            "http://gales.cdlib.org/b-traven3.jpeg.ZKCMBC3DSMM3RYW4KFRJCQJZFR6G3C4J.XXX" -> 3))
    }

    ignore("should unmerge successfully") {
      w.index(solrurl, "*:*", List(mergearc1, mergearc2, mergearc3), "XXX");
      w.remove(solrurl, List(mergearc3));
      testResults("*:*", 4,
        Map("http://gales.cdlib.org/robots.txt.MNSXZO35OCDMK2YM2TS4NGM3W2BWMSDI.XXX" -> 1,
            "http://gales.cdlib.org/.GNQD4SRUO7VSBGHTDQUO4AIWDG2PJ74M.XXX"-> 1,          
            "http://gales.cdlib.org/.R4OI4U63VX5OM5NYDZPUECTERBGWOCLD.XXX" -> 1,
            "http://gales.cdlib.org/b-traven3.jpeg.ZKCMBC3DSMM3RYW4KFRJCQJZFR6G3C4J.XXX" -> 2))
      w.remove(solrurl, List(mergearc2));
      testResults("*:*", 3,
        Map("http://gales.cdlib.org/robots.txt.MNSXZO35OCDMK2YM2TS4NGM3W2BWMSDI.XXX" -> 1,
            "http://gales.cdlib.org/.GNQD4SRUO7VSBGHTDQUO4AIWDG2PJ74M.XXX"-> 1,          
            "http://gales.cdlib.org/b-traven3.jpeg.ZKCMBC3DSMM3RYW4KFRJCQJZFR6G3C4J.XXX" -> 1))
      w.remove(solrurl, List(mergearc1));
      assertSearchSize("*:*", 0);
    }
    
    ignore("should do threaded merge properly") {
      val threads = 
        for (i <- (1 to 10))
        yield {
          new Thread {
            val is = i.toString;
            override def run {
              w.index(solrurl, "job:%s".format(is), List(mergearc1), is, Map("job"->List(is)));
              w.index(solrurl, "job:%s".format(is), List(mergearc2), is, Map("job"->List(is)));
              w.index(solrurl, "job:%s".format(is), List(mergearc3), is, Map("job"->List(is)));
            }
          }
        }
      threads.map(_.start);
      threads.map(_.join);
      for (i <- (1 to 10)) {
        testResults("job:%d".format(i), 4,
                    Map("http://gales.cdlib.org/robots.txt.MNSXZO35OCDMK2YM2TS4NGM3W2BWMSDI.%d".format(i) -> 1,
                        "http://gales.cdlib.org/.GNQD4SRUO7VSBGHTDQUO4AIWDG2PJ74M.%d".format(i)-> 1,          
                        "http://gales.cdlib.org/.R4OI4U63VX5OM5NYDZPUECTERBGWOCLD.%d".format(i) -> 2,
                        "http://gales.cdlib.org/b-traven3.jpeg.ZKCMBC3DSMM3RYW4KFRJCQJZFR6G3C4J.%d".format(i) -> 3))
      }
    }
  }
}
