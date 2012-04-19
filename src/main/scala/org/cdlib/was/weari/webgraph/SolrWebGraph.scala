/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari.webgraph;

import it.unimi.dsi.webgraph._;
import java.io._;
import org.apache.solr.client.solrj._;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common._;
import org.archive.net.UURIFactory;
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
