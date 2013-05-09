/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.io.{File,FileInputStream,FileOutputStream,InputStreamReader};

import org.apache.solr.common.SolrInputDocument;

import org.archive.io.{ArchiveReaderFactory,ArchiveRecord};

import org.cdlib.was.weari._;

import org.scalatest.{FeatureSpec,GivenWhenThen,Ignore};

import scala.collection.mutable.HashMap;

class WarcSpec extends FeatureSpec {
  val cl = classOf[WarcSpec].getClassLoader;
  val config = new Config {};
  val parser = new MyParser;
  val warcName = "IAH-20080430204825-00000-blackbook.warc.gz";
  val arcName = "IAH-20080430204825-00000-blackbook.arc.gz";

  feature ("We can read a WARC file.") {
    scenario ("(W)ARC files should return the same data.") {
      val arcData = new HashMap[String, String];
      val warcData = new HashMap[String, String];

      for (rec <- ArchiveReaderFactoryWrapper.get(warcName, cl.getResourceAsStream(warcName))) {
        if (rec.isHttpResponse) {
          parser.safeParse(rec).map { res =>
            warcData += (rec.getUrl -> res.toJsonString);
          }
        }
      }
      for (rec <- ArchiveReaderFactoryWrapper.get(arcName, cl.getResourceAsStream(arcName))) {
        parser.safeParse(rec).map { res =>
          arcData += (rec.getUrl -> res.toJsonString)
        }
      }
      for ((k,v) <- arcData) {
        val j1 = ParsedArchiveRecord.deserializeJson(v);
        val j2 = ParsedArchiveRecord.deserializeJson(warcData.get(k).get);
        assert(j1.getDigest == j2.getDigest);
        assert(j1.getUrl == j2.getUrl);
        assert(j1.getDate == j2.getDate);
        assert(j1.title == j2.title);
        assert(j1.content == j2.content);
        assert(j1.canonicalUrl == j2.canonicalUrl);
        assert(j1.canonicalHost == j2.canonicalHost);
        assert(j1.urlFingerprint == j2.urlFingerprint);
      }
    }
    
    scenario ("We can round trip JSON.") {
      for (rec <- ArchiveReaderFactoryWrapper.get (arcName, cl.getResourceAsStream(arcName))) {
        parser.safeParse(rec).map { res =>
          assert (ParsedArchiveRecord.deserializeJson(res.toJsonString) == res);
        }
      }
    }

    // scenario ("can serialize an arc to JSON") {
    //   val tmpfile = File.createTempFile("ng-indexer", ".json.gz");
    //   indexer.arc2json(cl.getResourceAsStream(arcName), arcName, tmpfile);

    //   /* and read it */
    //   indexer.json2records(tmpfile);
    // }
  }
}
