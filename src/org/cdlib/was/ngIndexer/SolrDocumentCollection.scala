package org.cdlib.was.ngIndexer;

import org.apache.solr.common._;
import org.apache.solr.client.solrj._;
import scala.collection.mutable.ArrayBuffer;

class SolrDocumentCollection(val server : SolrServer,
                             val q : SolrQuery)
extends Collection[SolrDocument] {
  override lazy val size : Int =
    server.query(q.getCopy.setRows(0)).getResults.getNumFound.asInstanceOf[Int];

  override def toString = 
    elements.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty)");

  def elements = new MyIterator[SolrDocument]() {
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
