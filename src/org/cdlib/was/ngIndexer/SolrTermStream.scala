package org.cdlib.was.ngIndexer;

import org.apache.solr.common._;
import org.apache.solr.client.solrj._;
import scala.collection.mutable.ArrayBuffer;

class SolrTermStream(val server : SolrServer,
                     val field : String,
                     var cache : RandomAccessSeq.MutableProjection[String],
                     var pos : String)
extends Stream[String] {
  
  def this(server : SolrServer, field : String) = this(server, field, new ArrayBuffer[String]().drop(0), "");

  override def hasDefiniteSize = false;

  def addDefinedElems(buf : StringBuilder, prefix : String) : StringBuilder = {
    if (head.isEmpty) { return buf; }
    else {
      val buf1 = buf.append(prefix).append(this.head);
      if (tail.isEmpty) { return buf1.append(")"); }
      else { return buf1.append(", ?)"); }
    }
  }
  
  def fillCache {
    if (cache.isEmpty) {
      var newCache = new ArrayBuffer[String]();
      val q = new SolrQuery().setTerms(true).setTermsSortString("index").
        addTermsField(field).setTermsLimit(10000).setQueryType("/terms").setTermsLower(pos);
      val results = server.query(q).getTermsResponse.getTerms(field);
      for (i <- new Range(0, results.size, 1)) {
        newCache.append(results.get(i).getTerm);
      }
      if (pos == "") {
        cache = newCache.drop(0);
      } else {
        // drop the first one, which is a dup of our last
        cache = newCache.drop(1);
      }
    }
  }

  def head = { 
    if (cache.isEmpty) fillCache;
    if (cache.isEmpty) throw new Predef.NoSuchElementException();
    cache(0);
  }

  def tail = new SolrTermStream(server, field, cache.drop(1), cache(0));
  
  override def isEmpty = {
    if (cache.isEmpty) fillCache;
    cache.isEmpty;
  }
}

