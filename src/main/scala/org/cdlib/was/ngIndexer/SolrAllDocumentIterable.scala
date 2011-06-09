package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;

class SolrAllDocumentIterable(server : SolrServer, field : String, fieldVals : Seq[String])
extends Iterable[SolrDocument] {
  
  override def toString = 
    iterator.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty)");

  def iterator = new CachingIterator[SolrDocument]() {
    val atOnce = 100;
    val rowsAtOnce = 1000;
    var nextUrlPos = 0;

    def fillCache {
      val lastVal = fieldVals(scala.math.min(nextUrlPos + atOnce, fieldVals.length));
      val q = new SolrQuery().
        setQuery("%s:[%s TO %s]".format(field, fieldVals(nextUrlPos), lastVal))
        .setSortField(field, SolrQuery.ORDER.asc).setRows(rowsAtOnce);
      for (d <- new SolrDocumentCollection(server, q)) {
        cache += d;
      }
      nextUrlPos += (atOnce + 1);
    }   
  }
}
