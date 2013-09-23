/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.UUID;

import org.scalatest._;
import org.scalatest.matchers._;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrUtils.{ makeDoc, record2inputDocument };

import org.joda.time.DateTime;

class SolrUtilsTest extends FunSpec with BeforeAndAfter { 
  describe ("record2inputDocument") {
    it("should generate the right input document") {
      val rec = new ParsedArchiveRecord(filename = "ARC-A.arc.gz",
                                        digest = Some("ABCDEFGH"),
                                        url = "http://example.org/",
                                        date = new DateTime(),
                                        title = Some("title"),
                                          length = 10,
                                        content = Some("hello world"),
                                        isRevisit = None,
                                        suppliedContentType = ContentType.forceParse("text/html"),
                                        detectedContentType = ContentType.parse("text/plain"),
                                        outlinks = Seq[Long]());
      
      val newdoc = record2inputDocument(rec, Map("fielda"->"vala"), "extraId");
      assert (newdoc.getFieldValue(URL_FIELD) === "http://example.org/");
      assert (newdoc.getFieldValue(ARCNAME_FIELD) === "ARC-A.arc.gz");
      assert (newdoc.getFieldValue(DIGEST_FIELD) === "ABCDEFGH");
      assert (newdoc.getFieldValue(CONTENT_LENGTH_FIELD) === 10);
      assert (newdoc.getFieldValue(TITLE_FIELD) === "title")
      assert (newdoc.getFieldValue(MEDIA_TYPE_DET_FIELD) === "text/plain")
      assert (newdoc.getFieldValue(MEDIA_TYPE_SUP_FIELD) === "text/html")
      assert (newdoc.getFieldValue("fielda") === "vala");
    }
  }

  // describe("removeMerge") {
  //   val a = makeDoc(ID_FIELD -> "abc",
  //                   ARCNAME_FIELD -> "ARC-A.arc.gz",
  //                   JOB_FIELD -> List("A"),
  //                   CONTENT_FIELD -> "hello world");
  //   val b = makeDoc(ID_FIELD -> "abc",
  //                   ARCNAME_FIELD -> "ARC-B.arc.gz",
  //                   JOB_FIELD -> List("B"),
  //                   CONTENT_FIELD -> "hello world");
  //   val m = makeDoc(ID_FIELD -> "abc",
  //                   ARCNAME_FIELD -> List("ARC-A.arc.gz", "ARC-B.arc.gz"),
  //                   JOB_FIELD -> List("A", "B"),
  //                   CONTENT_FIELD -> "hello world");
  //   it("should work") {
  //     assertDocsEqual(SolrUtils.removeMerge(ARCNAME_FIELD, "ARC-A.arc.gz", toSolrDocument(m)).get, b);
  //   }

  //   it("should return None when the last doc is removed") {
  //     val tmp = SolrUtil.removeMerge(ARCNAME_FIELD, "ARC-A.arc.gz", toSolrDocument(merged)).get;
  //     assert(SolrUtil.removeMerge(ARCNAME_FIELD, "ARC-B.arc.gz", toSolrDocument(tmp)) === None);
  //   }
  // }
}
