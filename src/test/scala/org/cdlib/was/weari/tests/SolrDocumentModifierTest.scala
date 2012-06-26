/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.{ Date, UUID };

import org.scalatest._;
import org.scalatest.matchers._;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.{ makeDoc, record2inputDocument };

//import scala.collection.JavaConversions.mapAsScalaMap;

class SolrDocumentModifierTest extends FunSpec with BeforeAndAfter with ShouldMatchers {
  describe ("record2inputDocument") {
    it("should generate the right input document") {
      val rec = new ParsedArchiveRecord(filename = "ARC-A.arc.gz",
                                        digest = Some("ABCDEFGH"),
                                        url = "http://example.org/",
                                        date = new Date(),
                                        title = Some("title"),
                                          length = 10,
                                        content = Some("hello world"),
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
}
