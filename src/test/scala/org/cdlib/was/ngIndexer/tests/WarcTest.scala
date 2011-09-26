/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

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

  feature ("We can read a WARC file.") {
    scenario ("(W)ARC files should return the same data.") {
      val warcName = "IAH-20080430204825-00000-blackbook.warc.gz";
      val arcName = "IAH-20080430204825-00000-blackbook.arc.gz";
      val arcData = new HashMap[String, SolrInputDocument];
      val warcData = new HashMap[String, SolrInputDocument];

      Utility.eachRecord (cl.getResourceAsStream(warcName), warcName) { (rec)=>
        if (rec.isHttpResponse) {
          indexer.mkIndexResource(rec, rec, config).map(res=>res.toDocument.map(doc=>warcData += (rec.getUrl -> doc)));
        }
      }
      Utility.eachRecord (cl.getResourceAsStream(arcName), arcName) { (rec)=>
        indexer.mkIndexResource(rec, rec, config).map(res=>res.toDocument.map(doc=>arcData += (rec.getUrl -> doc)));
      }
/*      for ((k,v) <- arcData) {
        assert(v === warcData.getOrElse(k, ""));
      } */
    }
  }
}
