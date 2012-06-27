/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.UUID;

import org.scalatest._;
import org.scalatest.matchers._;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.makeDoc;

import scala.collection.JavaConversions.mapAsScalaMap;

class MergeManagerSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {
  /**
   * Convert a SolrInputDocument into something that we can compare.
   */
  def doc2map (doc : SolrInputDocument) : 
      scala.collection.Map[java.lang.String,java.lang.Object] = 
    mapAsScalaMap(doc).mapValues(_.getValue)

  var manager : MergeManager = null;
  val config = new Config();

  /* reset for every run */
  before {
    manager = new MergeManager(config, "*:*", null);
  }

  val adoc = makeDoc(ID_FIELD -> "abc",
                     ARCNAME_FIELD -> "ARC-A.arc.gz",
                     CONTENT_FIELD -> "hello world");
  val bdoc = makeDoc(ID_FIELD -> "abc",
                     ARCNAME_FIELD -> "ARC-B.arc.gz",
                     CONTENT_FIELD -> "hello world");
  val merged = makeDoc(ID_FIELD -> "abc",
                       ARCNAME_FIELD -> "ARC-A.arc.gz",
                       ARCNAME_FIELD -> "ARC-B.arc.gz",
                       CONTENT_FIELD -> "hello world");

  def assertDocsEqual(a : SolrInputDocument, b : SolrInputDocument) {
    assert(doc2map(a) === doc2map(b));
  }

  describe ("Sample documents") {
    it("should not be equal") {
      assert(doc2map(adoc) != doc2map(bdoc));
    }
  }

  describe ("Merge manager") {
    it("should merge docs successfully") {
      assertDocsEqual(merged, manager.mergeDocs(adoc, bdoc));
    }

    it("should return an equal document on first merge") {
      assertDocsEqual(manager.merge(adoc), adoc);
    }

    it("should merge with an empty content field") {
      val doc = makeDoc(ID_FIELD -> "abc",
                        ARCNAME_FIELD -> "ARC-B.arc.gz");
      manager.merge(adoc);
      assertDocsEqual(manager.merge(doc), merged);
    }

    it("should yield a merged document on second merge") {
      manager.merge(adoc);
      assertDocsEqual(manager.merge(bdoc), merged);
    }

    it("should get the correct field value when the first or second doc lacks the field") {
      assert (Some("hello world") === 
        manager.getSingleFieldValue("field",
                                    makeDoc("field" -> "hello world"),
                                    makeDoc()));
      assert (Some("hello world") === 
        manager.getSingleFieldValue("field", 
                                    makeDoc(),
                                    makeDoc("field" -> "hello world")));
    }

    it("should merge fields in order") {
      assert (Seq("foo", "bar") ===
        manager.mergeFieldValues("field",
                                 makeDoc("field" -> "foo"),
                                 makeDoc("field" -> "bar")));
    }

    it("should unmerge fields successfully") {
      assert (Seq("bar", "baz") ===
        manager.unmergeFieldValues("field",
                                   makeDoc("field" -> Seq("foo", "bar", "baz")),
                                   makeDoc("field" -> "foo")));
    }

    it("should load a document successfully") {
      manager.loadDoc(adoc);
      assert(doc2map(manager.merge(bdoc)) === doc2map(merged));
    }                 

    it("should load a document successfully AFTER it has been merged") {
      manager.merge(adoc);
      manager.loadDoc(bdoc);
      assert(manager.getDocById("abc").map(doc2map(_)) === Some(doc2map(merged)));
    }

    it("should mark a merged doc as a potential merge") {
      manager.merge(adoc);
      assert(manager.isPotentialMerge("abc") === true);
    }

    it("should merge empty string content correctly") {
      val doc = makeDoc(ID_FIELD -> "abc",
                        ARCNAME_FIELD -> "ARC-B.arc.gz",
                        CONTENT_FIELD -> "");
      manager.merge(adoc);
      assert(doc2map(manager.merge(doc)) === doc2map(merged));
    }
  }

  describe("unmergeDocs") {
    it("should work") {
      val unmerged = manager.unmergeDocs(merged, bdoc);
      assert(doc2map(unmerged.get) === doc2map(makeDoc(ID_FIELD -> "abc",
                                                       ARCNAME_FIELD -> "ARC-A.arc.gz",
                                                       CONTENT_FIELD -> "hello world")));
    }
  }

  describe("Unmerging") {
    it("should happen successfully") {
      manager.merge(adoc);
      manager.merge(bdoc);
      manager.unmerge(bdoc);
      assert(manager.getDocById("abc").map(doc2map(_)) === Some(doc2map(adoc)));
    }

    it("should happen successfully in any order") {
      manager.merge(adoc);
      manager.merge(bdoc);
      manager.unmerge(adoc);
      assertDocsEqual(manager.getDocById("abc").get, bdoc);
    }

    it("should return None when unmerging the final doc") {
      manager.merge(adoc);
      manager.merge(bdoc);
      manager.unmerge(adoc);
      assert(manager.unmerge(bdoc) === None);
    }
  }
}
