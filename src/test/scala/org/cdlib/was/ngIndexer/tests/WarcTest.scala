/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import com.codahale.jerkson.Json;

import java.io.{File,FileInputStream,FileOutputStream,InputStreamReader};

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
  val indexer = new SolrIndexer;
  val warcName = "IAH-20080430204825-00000-blackbook.warc.gz";
  val arcName = "IAH-20080430204825-00000-blackbook.arc.gz";

  feature ("We can read a WARC file.") {
    scenario ("(W)ARC files should return the same data.") {
      val arcData = new HashMap[String, String];
      val warcData = new HashMap[String, String];

      for (rec <- ArchiveReaderFactoryWrapper.get(warcName, cl.getResourceAsStream(warcName))) {
        if (rec.isHttpResponse) {
          indexer.parseArchiveRecord(rec).map { res =>
            warcData += (rec.getUrl -> Json.generate(res));
          }
        }
      }
      for (rec <- ArchiveReaderFactoryWrapper.get(arcName, cl.getResourceAsStream(arcName))) {
        indexer.parseArchiveRecord(rec).map { res =>
          arcData += (rec.getUrl -> Json.generate(res))
        }
      }
      for ((k,v) <- arcData) {
        val j1 = Json.parse[ParsedArchiveRecord](v);
        val j2 = Json.parse[ParsedArchiveRecord](warcData.get(k).get);
        assert(j1.digest == j2.digest);
        assert(j1.url == j2.url);
        assert(j1.date == j2.date);
        assert(j1.title == j2.title);
        assert(j1.content == j2.content);
      }
    }
    
    scenario ("We can round trip JSON.") {
      for (rec <- ArchiveReaderFactoryWrapper.get (arcName, cl.getResourceAsStream(arcName))) {
        indexer.parseArchiveRecord(rec).map { res =>
          assert (Json.parse[ParsedArchiveRecord](Json.generate(res)) == res);
        }
      }
    }
    
    scenario ("can serialize an arc to JSON") {
      val tmpfile = File.createTempFile("ng-indexer", ".json.gz");
      indexer.arc2json(cl.getResourceAsStream(arcName), arcName, tmpfile);

      /* and read it */
      indexer.json2records(tmpfile);
    }
  }
}
