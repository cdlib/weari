/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari.solr;

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;

class SolrDocumentCollection(server : SolrServer, q : SolrQuery)
  extends Iterable[SolrDocument] {

  /* you don't want to call this. */
  def apply (idx : Int) = this.iterator.toSeq(idx);

  override def toString = 
    iterator.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty)");

  def iterator = new CachingIterator[SolrDocument]() {
    var pos = 0;

    var _length = -1;
    override def length = {
      /* if we haven't filled the cache yet, force it to be filled */
      if (_length == -1) this.peek;
      _length;
    }

    def fillCache {
      val results = server.query(q.getCopy.setStart(pos)).getResults;
      this._length = results.getNumFound.asInstanceOf[Int];
      for (i <- new Range(0, results.size, 1)) {
        cache += results.get(i);
      }
      pos = (results.getStart.asInstanceOf[Int] + results.size.asInstanceOf[Int]);
    }
  }    
}
