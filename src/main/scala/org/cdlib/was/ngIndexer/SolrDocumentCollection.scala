package org.cdlib.was.ngIndexer;

import org.apache.solr.common._;
import org.apache.solr.client.solrj._;

class SolrDocumentCollection(val server : SolrServer,
                             val q : SolrQuery)
extends Iterable[SolrDocument] {

  /* you don't want to call this. */
  def apply (idx : Int) = this.iterator.toSeq(idx);

  lazy val length : Int =
    server.query(q.getCopy.setRows(0)).getResults.getNumFound.asInstanceOf[Int];

  override def toString = 
    iterator.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty)");

  def iterator = new MyIterator[SolrDocument]() {
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
