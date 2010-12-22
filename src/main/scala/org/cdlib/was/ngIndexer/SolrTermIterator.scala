package org.cdlib.was.ngIndexer;

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};

class SolrTermIterable(val server : SolrServer,
                         val field : String)
extends Iterable[String] {
  
  override def toString = 
    iterator.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty");

  override def iterator = new MyIterator[String]() {
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
