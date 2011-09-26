/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;

class SolrDocumentCollection(server : SolrServer, q : SolrQuery)
  extends Iterable[SolrDocument] {

  /* you don't want to call this. */
  def apply (idx : Int) = this.iterator.toSeq(idx);

  lazy val length : Int =
    server.query(q.getCopy.setRows(0)).getResults.getNumFound.asInstanceOf[Int];

  override def toString = 
    iterator.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty)");

  def iterator = new CachingIterator[SolrDocument]() {
    var pos = 0;

    def fillCache {
      val results = server.query(q.getCopy.setStart(pos)).getResults;
      for (i <- new Range(0, results.size, 1)) {
        cache += results.get(i);
      }
      pos = (results.getStart.asInstanceOf[Int] + results.size.asInstanceOf[Int]);
    }
  }    
}
