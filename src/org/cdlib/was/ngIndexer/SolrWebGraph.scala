package org.cdlib.was.ngIndexer;

import it.unimi.dsi.webgraph._;
import org.apache.solr.client.solrj._;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common._;
import scala.collection.mutable.ArrayBuffer;
import java.io._;
import org.archive.net.UURIFactory;
import Utility.{javaIteratorToScalaIterator,javaList2Seq};

class SolrWebGraph (url : String) extends WebGraph {
  val server = new CommonsHttpSolrServer(url);

  def addLink (link : Outlink) = ();
  def addLinks (links : Seq[Outlink]) = ();

  lazy val urls : Seq[String] = {
    val newUrls = new ArrayBuffer[String]();
    newUrls ++= new SolrTermStream(server, solrIndexer.CANONICALURL_FIELD);
    newUrls;
  }

  lazy val fingerprints = urls.map(url=>UriUtils.fingerprint(UURIFactory.getInstance(url)));
  
  def fingerprints(i : Int) = UriUtils.fingerprint(UURIFactory.getInstance(urls(i)));

  def numNodes = urls.length;

  def writeUrls (f : File) {
    val os = new FileOutputStream(f);
    val pw = new PrintWriter(os);
    val it1 = nodeIterator;
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
      case ex: java.lang.IllegalArgumentException => ex.printStackTrace(System.err);
    }
  }

  def documents (i : Int) : SolrDocumentStream = {
    val q = new SolrQuery().setQuery("%s:\"%s\"".format(solrIndexer.CANONICALURL_FIELD, urls(i)));
    return new SolrDocumentStream(server, q);
  }

  override def nodeIterator = new MyNodeIterator();

  class MyNodeIterator extends NodeIterator {
    var position = -1;
    
    var outlinksCache : Option[Seq[Int]] = None;
    
    def url = urls(position);
          
    def currentOutlinks : Option[Seq[Int]] = {
      if (outlinksCache.isEmpty) {
        var outlinkFps = new ArrayBuffer[Long]();
        for (doc <- documents(position)) {
          val fields = doc.getFieldValues("outlinks");
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
        return fp2id(fingerprints(position));
      }
    }
  }
}
