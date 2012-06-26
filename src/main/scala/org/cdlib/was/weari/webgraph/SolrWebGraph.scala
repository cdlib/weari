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

package org.cdlib.was.weari.webgraph;

import it.unimi.dsi.webgraph._;
import java.io._;
import org.apache.solr.client.solrj._;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common._;
import org.cdlib.was.weari._;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions.collectionAsScalaIterable;
import scala.collection.mutable.ArrayBuffer;

class SolrWebGraph (url : String) extends WebGraph {
  val logger = LoggerFactory.getLogger(classOf[SolrWebGraph]);

  val server = new HttpSolrServer(url);

  def addLink (link : Outlink) = ();
  def addLinks (links : Seq[Outlink]) = ();

  val urlsSize = 3500000;
  lazy val urls : Seq[String] = {
    val terms = new solr.SolrTermIterable(server, SolrFields.CANONICALURL_FIELD);
    var newUrls = new ArrayBuffer[String]() { ensureSize(urlsSize); };
    val it = terms.iterator;
    while (it.hasNext) { newUrls += it.next; }
    newUrls;
  }

  def fingerprints(i : Int) = UriUtils.fingerprint(urls(i));

  def numNodes = urls.length;

  def writeUrls (f : File) {
    val os = new FileOutputStream(f);
    val pw = new PrintWriter(os);
    val it = nodeIterator;
    while (it.hasNext) {
      it.next;
      pw.println(it.url);
    }
    pw.close;
  }

  def store (basename : String) {
    val isg = new MyImmutableSequentialGraph(this);
    try { 
      ImmutableGraph.store(classOf[BVGraph], isg, basename);
      writeUrls(new File("%s.urls".format(basename)));
    } catch { 
      case ex: java.lang.IllegalArgumentException => {
        logger.error("Caught exception storing.",ex);
      }
    }
  }

  override def nodeIterator = new MyNodeIterator();

  class MyNodeIterator extends NodeIterator {
    var position = -1;
    
    var outlinksCache : Option[Seq[Int]] = None;
    
    def url = urls(position);

    val docIterable = 
      new solr.SolrAllDocumentIterable(server, SolrFields.CANONICALURL_FIELD, urls);
    var docIt = docIterable.iterator;

    def checkUrl(d : SolrDocument) = 
      (d.getFieldValue(SolrFields.CANONICALURL_FIELD).asInstanceOf[String] == url)

    def hasNextDocument = docIt.peek.map(checkUrl(_)).getOrElse(false);
      
    def nextDocument : SolrDocument = docIt.next;
    
    def currentOutlinks : Option[Seq[Int]] = {
      if (outlinksCache.isEmpty) {
        var outlinkFps = new ArrayBuffer[Long]();
        while (hasNextDocument) {
          val doc = nextDocument;
          val fields = doc.getFieldValues("outlinks");
          if (fields != null) {
            for (j <- fields) {
              outlinkFps += j.asInstanceOf[Long];
            }
          }
        }
        outlinksCache = Some(outlinkFps.map(fp2id(_)).filter(n=>n < numNodes).
                             toList.sortWith((a,b)=>(a < b)).distinct);
      }
      return outlinksCache;
    }

    override def outdegree : Int = currentOutlinks.map(_.length).getOrElse(0);
    
    override def hasNext : Boolean = (position < numNodes - 1);
    
    override def successorArray : Array[Int] = currentOutlinks match {
      case None    => new Array[Int](0);
      case Some(o) => o.toArray.asInstanceOf[Array[Int]];
    }
      
    override def nextInt : Int = next.asInstanceOf[Int];
    
    override def next : java.lang.Integer = {
      if (!hasNext) {
        throw new NoSuchElementException();
      } else {
        outlinksCache = None;
        position += 1;
        return fp2id(fingerprints(position));
      }
    }
  }
}
