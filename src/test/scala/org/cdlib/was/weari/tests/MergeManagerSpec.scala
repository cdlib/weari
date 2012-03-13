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

  /* reset for every run */
  before {
    manager = new MergeManager("*:*", null);
  }

  val adoc = makeDoc(ID_FIELD -> "abc",
                     ARCNAME_FIELD -> "ARC-A.arc.gz");
  val bdoc = makeDoc(ID_FIELD -> "abc",
                     ARCNAME_FIELD -> "ARC-B.arc.gz");
  val merged = makeDoc(ID_FIELD -> "abc",
                       ARCNAME_FIELD -> "ARC-A.arc.gz",
                       ARCNAME_FIELD -> "ARC-B.arc.gz");

  describe ("Sample documents") {
    it("should not be equal") {
      assert(doc2map(adoc) != doc2map(bdoc));
    }
  }

  describe ("Merge manager") {
    it("should merge docs successfully") {
      assert(doc2map(merged) === doc2map(manager.mergeDocs(adoc, bdoc)));
    }

    it("should return an equal document on first merge") {
      assert(doc2map(manager.merge(adoc)) === doc2map(adoc));
    }

    it("should a merged document on second merge") {
      manager.merge(adoc);
      assert(doc2map(manager.merge(bdoc)) === doc2map(merged));
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
                                   makeDoc("field" -> "foo"),
                                   makeDoc("field" -> Seq("foo", "bar", "baz"))));
    }

    // it("reset should work") {
    //   manager.merge(adoc);
    //   manager.reset;
    //   assert(doc2map(manager.merge(bdoc)) === doc2map(bdoc));
    // }
  }
}
