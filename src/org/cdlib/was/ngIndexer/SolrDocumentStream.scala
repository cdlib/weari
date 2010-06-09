package org.cdlib.was.ngIndexer;

import org.apache.solr.common._;
import org.apache.solr.client.solrj._;
import scala.collection.mutable.ArrayBuffer;

class SolrDocumentStream(val server : SolrServer,
                         val q : SolrQuery,
                         var cache : RandomAccessSeq.MutableProjection[SolrDocument],
                         var pos : Int,
                         var _length : Int)
extends Stream.Definite[SolrDocument] {
  
  def this(server : SolrServer, q : SolrQuery) = this(server, q, new ArrayBuffer[SolrDocument]().drop(0), 0, -1);

  def addDefinedElems(buf : StringBuilder, prefix : String) : StringBuilder = {
    val buf1 = buf.append(prefix).append(this.head);
    if (tail.isEmpty) return buf1.append(")");
    else return buf1.append(", ?)");
  }
  
  override def length : Int = {
    if (_length == -1) {
      _length = server.query(q).getResults.getNumFound.asInstanceOf[Int];
    }
    return _length;
  }

  def fillCache {
    if (cache.isEmpty) {
      var newCache = new ArrayBuffer[SolrDocument]();
      val results = server.query(q.getCopy.setStart(pos)).getResults;
      for (i <- new Range(0, results.size, 1)) {
        newCache.append(results.get(i));
      }
      pos = (results.getStart.asInstanceOf[Int] + results.size.asInstanceOf[Int]);
      cache = newCache.drop(0);
    }
  }

  def head = { 
    if (cache.isEmpty) fillCache;
    if (cache.isEmpty) throw new Predef.NoSuchElementException();
    cache(0);
  }

  def tail = new SolrDocumentStream(server, q, cache.drop(1), pos, length);
  
  override def isEmpty = (pos >= (length - 1));
}

