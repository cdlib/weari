/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.weari.tests;

import java.util.UUID;

import org.scalatest._;
import org.scalatest.matchers._;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier._;

class SolrDocumentModifierSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {
  describe ("Canonicalizer") {
    it("should remove www") {
      assert(canonicalize("http://www.example.org") === canonicalize("http://example.org"))
    }

    it("should lowercase") {
      assert(canonicalize("http://www.example.org/path") === canonicalize("http://www.example.org/PATH"))
    }

    it("should order querys") {
      assert(canonicalize("http://www.example.org/?x=1&y=2") === canonicalize("http://www.example.org/?y=2&x=1"))
    }
  }
}
