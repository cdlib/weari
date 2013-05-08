/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari;

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;

/**
 * Iterable which is used to fetch terms from solr index. Lazy loads
 * the terms.
 *
 */
class SolrTermIterable(server : SolrServer, field : String, queryString : String)
  extends Iterable[String] {

  def this(server : SolrServer, field : String) = this(server, field, "*:*");

  override def toString = 
    iterator.peek.map(el=>"(%s, ...)".format(el)).getOrElse("(empty)");

  override def iterator = new CachingIterator[String]() {
    var lowerLimit : String = "";

    def fillCache {
      val q = new SolrQuery(queryString).setTerms(true).setTermsSortString("index").
        addTermsField(field).setTermsLimit(100000).setRequestHandler("/terms").
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
