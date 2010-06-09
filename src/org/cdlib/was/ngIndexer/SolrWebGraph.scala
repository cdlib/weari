package org.cdlib.was.ngIndexer;

import it.unimi.dsi.webgraph._;
import org.apache.solr.client.solrj._;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common._;
import scala.collection.jcl.BufferWrapper;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.jcl.MutableIterator.Wrapper;
import java.io._;

class SolrWebGraph (url : String) extends WebGraph {
  override def nodeIterator = new MyNodeIterator();
  val server = new CommonsHttpSolrServer(url);

  def javaList2Seq[T](javaList : java.util.List[T]) : Seq[T] =
    new BufferWrapper[T]() { def underlying = javaList; }

  def javaIteratorToScalaIterator[A](it : java.util.Iterator[A]) = new Wrapper(it)

  def addLink (link : Outlink) = ();
  def addLinks (links : Seq[Outlink]) = ();

  var fpcache : Option[Seq[Long]] = None;

  def getFingerPrints : Seq[Long] = {
    if (fpcache.isEmpty) {
      val q = new SolrQuery().setTerms(true).addTermsField(solrIndexer.URLFP_FIELD).setTermsLimit(-1).setQueryType("/terms")
      val resp = server.query(q);
      fpcache = Some(javaList2Seq(resp.getTermsResponse.getTerms(solrIndexer.URLFP_FIELD)).map(_.getTerm.toLong));
    }
    return fpcache.getOrElse(null);
  }

  def numNodes = getFingerPrints.length;

  def writeUrls (f : File) {
    val os = new FileOutputStream(f);
    val pw = new PrintWriter(os);
    val it1 = nodeIterator;
    val it = nodeIterator;
    while (it.hasNext) {
      it.next;
      pw.println(it.getUrl);
    }
    pw.close;
  }

  class MyNodeIterator extends NodeIterator {
    var position = -1;
    
    var outlinksCache : Option[Seq[Int]] = None;
        
    def getSearchResults = {
      val resp = server.query(new SolrQuery().setRows(1000).setQuery("%s:%d".format(solrIndexer.URLFP_FIELD, getFingerPrints(position))));
      resp.getResults;
    }

    def getUrl = getSearchResults.get(0).getFieldValue("url");
    
    def currentOutlinks : Option[Seq[Int]] = {
      if (outlinksCache.isEmpty) {
        val results = getSearchResults;
        var outlinkFps = new ArrayBuffer[Long]();
        for (i <- new Range(0, results.getNumFound.asInstanceOf[Int], 1)) {
          val fields = results.get(i).getFieldValues("outlinks");
          if (fields != null) {
            for (j <- javaIteratorToScalaIterator(fields.iterator)) {
              outlinkFps += j.asInstanceOf[Long];
            }
          }
        }
        outlinksCache = Some(outlinkFps.map(fp2id(_)).filter(n=>n < numNodes).toList.sort((a,b)=>(a < b)).removeDuplicates);
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
        return fp2id(getFingerPrints(position));
      }
    }
  }
}
