/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import net.liftweb.json.parse;

import org.apache.solr.common.SolrInputDocument;

import org.archive.io.{ArchiveReaderFactory,ArchiveRecord};

import org.cdlib.was.ngIndexer._;

import org.junit.runner.RunWith;


import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};
import org.scalatest.junit.JUnitRunner;

import scala.collection.mutable.HashMap;

@RunWith(classOf[JUnitRunner])
class WarcSpec extends FeatureSpec {
  val cl = classOf[WarcSpec].getClassLoader;
  val config = new Config {};
  val indexer = new SolrIndexer(config);
  val warcName = "IAH-20080430204825-00000-blackbook.warc.gz";
  val arcName = "IAH-20080430204825-00000-blackbook.arc.gz";
  
  feature ("We can read a WARC file.") {
    scenario ("(W)ARC files should return the same data.") {
      val arcData = new HashMap[String, String];
      val warcData = new HashMap[String, String];

      Utility.eachRecord (cl.getResourceAsStream(warcName), warcName) { (rec)=>
        if (rec.isHttpResponse) {
          indexer.mkIndexResource(rec).map { res =>
            warcData += (rec.getUrl -> res.toJson);
          }
        }
      }
      Utility.eachRecord (cl.getResourceAsStream(arcName), arcName) { (rec)=>
        indexer.mkIndexResource(rec).map { res =>
          arcData += (rec.getUrl -> res.toJson)
        }
      }
      for ((k,v) <- arcData) {
        val j1 = parse(v);
        val j2 = parse(warcData.get(k).getOrElse(""));
        for (field <- List("digest", "url", "date", "title", "content")) {
          assert((j1 \ field) === (j2 \ field));
        }
      }
    }
  }
}
