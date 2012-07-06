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
