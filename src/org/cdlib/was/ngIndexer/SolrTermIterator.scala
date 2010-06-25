package org.cdlib.was.ngIndexer;

import org.apache.solr.common._;
import org.apache.solr.client.solrj._;
import scala.collection.mutable.ArrayBuffer;

class SolrTermIterable(val server : SolrServer,
                         val field : String)
extends Iterable[String] {
  
  override def toString = 
    elements.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty");

  def elements = new MyIterator[String]() {
    var lowerLimit : String = "";

    def fillCache {
      val q = new SolrQuery().setTerms(true).setTermsSortString("index").
        addTermsField(field).setTermsLimit(100000).setQueryType("/terms").
        setTermsLower(lowerLimit);
      val results = server.query(q).getTermsResponse.getTerms(field);
      for (i <- new Range(0, results.size, 1)) {
        cache += results.get(i).getTerm;
      }
      if (lowerLimit != "") {
        // drop the first one, which is a dup of our last
        cache.trimStart(1);
      }
      if (cache.length > 0) lowerLimit = cache(cache.length-1);
    }
  }
}
