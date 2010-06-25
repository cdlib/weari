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

  val urlsSize = 3500000;
  lazy val urls : Seq[String] = {
    val terms = new SolrTermIterable(server, solrIndexer.CANONICALURL_FIELD);
    var newUrls = new ArrayBuffer[String]() { ensureSize(urlsSize); };
    val it = terms.elements;
    while (it.hasNext) { newUrls += it.next; }
    newUrls;
  }

  lazy val fingerprints = urls.map(url=>UriUtils.fingerprint(UURIFactory.getInstance(url)));
  
  def fingerprints(i : Int) = UriUtils.fingerprint(UURIFactory.getInstance(urls(i)));

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
      case ex: java.lang.IllegalArgumentException => ex.printStackTrace(System.err);
    }
  }

  override def nodeIterator = new MyNodeIterator();

  class MyNodeIterator extends NodeIterator {
    var position = -1;
    
    var outlinksCache : Option[Seq[Int]] = None;
    
    def url = urls(position);

    var docIt = (new SolrAllDocumentIterable(server, solrIndexer.CANONICALURL_FIELD, urls)).
      elements;

    def checkUrl(d : SolrDocument) = 
      (d.getFieldValue(solrIndexer.CANONICALURL_FIELD).asInstanceOf[String] == url)

    def hasNextDocument = docIt.peek.map(checkUrl(_)).getOrElse(false);
      
    def nextDocument : SolrDocument = docIt.next;
    
    def currentOutlinks : Option[Seq[Int]] = {
      if (outlinksCache.isEmpty) {
        var outlinkFps = new ArrayBuffer[Long]();
        while (hasNextDocument) {
          val doc = nextDocument;
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
