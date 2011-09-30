/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import java.io.{File,FileOutputStream};

import net.liftweb.json._;
import net.liftweb.json.Serialization.{read, write};
import net.liftweb.json.{DefaultFormats,Serialization};

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
  implicit val formats = DefaultFormats;

  feature ("We can read a WARC file.") {
    scenario ("(W)ARC files should return the same data.") {
      val arcData = new HashMap[String, String];
      val warcData = new HashMap[String, String];

      Utility.eachRecord (cl.getResourceAsStream(warcName), warcName) { (rec)=>
        if (rec.isHttpResponse) {
          indexer.parseArchiveRecord(rec).map { res =>
            warcData += (rec.getUrl -> Serialization.write(res));
          }
        }
      }
      Utility.eachRecord (cl.getResourceAsStream(arcName), arcName) { (rec)=>
        indexer.parseArchiveRecord(rec).map { res =>
          arcData += (rec.getUrl -> Serialization.write(res))
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
    
    scenario ("We can round trip JSON.") {
      Utility.eachRecord (cl.getResourceAsStream(arcName), arcName) { (rec)=>
        indexer.parseArchiveRecord(rec).map { res =>
          assert (parse(Serialization.write(res)).extract[ParsedArchiveRecord] == res);
        }
      }
    }
    
    scenario ("can serialize an arc to JSON") {
      val tmpfile = File.createTempFile("ng-indexer", "json");
      val os = new FileOutputStream(tmpfile);
      indexer.parseToJson(cl.getResourceAsStream(arcName), arcName, os);
    }
  }
}
