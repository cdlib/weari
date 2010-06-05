package org.cdlib.was.ngIndexer;

import com.shorrockin.cascal.model;
import com.shorrockin.cascal.session._;
import com.shorrockin.cascal.utils.Conversions._;
import it.unimi.dsi.webgraph._;
import java.io._;
import java.lang.UnsupportedOperationException;
import java.util.Date;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.archive.net.UURI;
import org.archive.util.ArchiveUtils;
import scala.collection.mutable.HashMap;
import org.archive.net.UURIFactory;

class CassandraWebGraph extends WebGraph {
  val hosts  = Host("localhost", 9160, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 1000L, 6, 2)
  val pool   = new SessionPool(hosts, params, Consistency.One)  

  override def toString = "WebGraph";

  def borrow[T] (f : (Session) => T) : T = {
    var retval : T = null.asInstanceOf[T];
    pool.borrow { session =>
      retval = f(session);
    }
    return retval;
  }

  def addLink (link : Outlink) {
    this.addLinks(List(link));
  }

  var numNodesFinished = false;
  var numNodesCount = 0;
  var knownUrlCounter = 0;
  var knownUrls = new HashMap[Long, Int]();
  
  def fp2id (l : Long) : Int = {
    knownUrls.get(l) match {
      case Some(i) => return i;
      case None    => {
        knownUrls.update(l, knownUrlCounter);
        knownUrlCounter = knownUrlCounter + 1;
        return knownUrlCounter - 1;
      }
    }
  }

  def writeIds (f : File) {
    val fos = new FileOutputStream(f);
    for ((k,v) <- knownUrls) {
      lookupFp(k).map(url=>fos.write("%d %s\n".format(v, url)));
    }
    fos.close;
  }

  val columnFamily = "WebGraph" \\ "Outlinks";

  def numNodes : Int = {
    if (numNodesFinished) {
      return numNodesCount;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /* init node numbers so BVGraph gets +1 for each next node */
  def init {
    val it = this.nodeIterator;
    while (it.hasNext) it.next;
  }

  init;

  def lookupFp (fp : Long) : Option[UURI] = 
    lookupFp(UriUtils.fp2string(fp));
  
  def lookupFp (fpstring : String) : Option[UURI] = {
    borrow { session=>
      session.get("WebGraph" \ "Urls" \ fpstring \ "url").map(x=>UURIFactory.getInstance(x.value));
    }
  }  
    
  /* adds a URL to our collection, if necessary, returns its fingerprint */
  def addUrl (url : UURI) : Long = {
    val fp = UriUtils.fingerprint(url);
    val fpstring = UriUtils.fp2string(fp);
    pool.borrow { session =>
      val key = "WebGraph" \ "Urls" \ fpstring;
      def insertUrl {
        val encoded = UriUtils.encodeUrl(url);
        session.insert(key \ ("url", encoded));
      }
      try {
        if (session.get(key \ "url").isEmpty) insertUrl;
      } catch {
        case ex : TApplicationException => {
          /* ok, whatever, just insert it anyhow */
          insertUrl;
        }
      }
    }
    return fp;
  }

  def addLinks (links : Seq[Outlink]) {
    var done = false;
    var retryTimes = 0;

    while (!done) {
      try {
        val inserts = links.map((l)=>{
          Insert(columnFamily \ 
                 UriUtils.fp2string(addUrl(l.from)) \ 
                 UriUtils.fpdate2string(addUrl(l.to), l.date) \
                 ("text", l.text));
        });
        if (inserts.size > 0) {
          pool.borrow { session =>
            session.batch(inserts);
          }
        }
        done = true;
      } catch {
        case ex : TTransportException => {
          if (retryTimes > 3) {
            throw ex; // too many errors
          } else {
            retryTimes = retryTimes + 1;
            Thread.sleep(5);
          }
        }
      }
    }
  }

  override def nodeIterator = new MyNodeIterator();

  class MyNodeIterator
    extends NodeIterator {
      var iteratorDone = false;
      var currentKey : Option[String] = None;
      var currentLong : Option[Long] = None;
      var nextKey : Option[String] = None;
      var itCount = 0;
      
      /* TODO speed up by caching this puppy */
      def currentVal : Option[Map[model.SuperColumn,Seq[model.Column[model.SuperColumn]]]] = {
        var retval : Option[Map[model.SuperColumn,Seq[model.Column[model.SuperColumn]]]] = None;
        pool.borrow { session=>
          retval = currentKey.map(key=>{ session.list(columnFamily \ key) });
        }
        return retval;
      }
      
      // override def outdegree : Int = {
      //   borrow { session =>
      //     currentKey.map(key=>session.count(columnFamily \ key)).getOrElse(-1);
      //   }
      // }
      
      override def outdegree : Int =
        successorArray.length;

      def bytesort (a : String, b : String) = 
        (-1 == FBUtilities.compareByteArrays(a.getBytes("ASCII"), b.getBytes("ASCII")));

      def getNextKeyFromRange (start : String) : Option[String] = {
        val range = KeyRange(start, "~~~~~~~~", 2); /* ~ is 0x7f, top ascii char, should sort last */
        var retval : Option[String] = None;
        pool.borrow { session =>
          val res = session.list("WebGraph" \ "Urls", range);
          val sortedKeys = res.keySet.toList.sort((a,b)=>{
            bytesort(a.value, b.value)
          });
          /* find the first where start != key */
          retval = sortedKeys.map(_.value).find(_ != start);
        }
        return retval;
      }
    
      override def hasNext : Boolean = {
        if (!iteratorDone && nextKey.isEmpty) {
          nextKey = getNextKeyFromRange(currentKey.getOrElse(""));
          if (nextKey.isEmpty) {
            iteratorDone = true;
            numNodesFinished = true;
            numNodesCount = itCount;
          }
        }
        return !iteratorDone;
      }
      
      override def successorArray : Array[Int] = currentVal match {
        case None    => return new Array[Int](0);
        case Some(v) => {
          val ids = for (k <- v.keySet)
                    yield fp2id(UriUtils.decodeFp(k.value.take(8)));
          return ids.toList.removeDuplicates.sort((a,b)=>a < b).toArray[Int];
        }
      }
      
      override def nextInt : Int = next.asInstanceOf[Int];
      
      override def next : java.lang.Integer = {
        if (!hasNext) {
          throw new NoSuchElementException();
        } else {
          itCount = itCount + 1;
          val newCurrentLong = nextKey.map(UriUtils.string2fp(_));
          currentKey = nextKey;
          nextKey = None;
          currentLong = newCurrentLong;
          return fp2id(currentLong.getOrElse(0L));
        }
      }
    }
}

object CassandraWebGraph {
  def import2bv (name : String) {
    val g = new MyImmutableSequentialGraph(new CassandraWebGraph());
    try { 
      ImmutableGraph.store(classOf[BVGraph], g, name);
    } catch { 
      case ex: java.lang.IllegalArgumentException => ex.printStackTrace(System.err);
    }
  }

  def import2ascii (name : String) {
    val g = new MyImmutableSequentialGraph(new CassandraWebGraph());
    try { 
      ASCIIGraph.store(g, name);
    } catch { 
      case ex: java.lang.IllegalArgumentException => ex.printStackTrace(System.err);
    }
  }

  def readAscii (name : String) {
    val ag = ASCIIGraph.loadSequential(name);
    val it = ag.nodeIterator;
    while (it.hasNext) {
      System.err.println("reading...");
      it.next;
    }
  }

  def main (args : Array[String]) {
    val g = new CassandraWebGraph();
    val it = g.nodeIterator;
    var i = 0;
    while (it.hasNext) {
      var u = it.next;
      if (u != i) throw new RuntimeException("node seq error %d != %d.".format(i, u));
      i = i + 1;
    }
    val numNodes = g.numNodes;
    if (numNodes != i) throw new RuntimeException("num nodes error.");
    val it2 = g.nodeIterator;
    while (it2.hasNext) {
      it2.next;
      if (it.outdegree > 0) {
        val a = it.successorArray;
        if (a.length != it.outdegree) { System.err.println("Bad outdegree.") };
        for (j <- a) {
          if (j > numNodes) { System.err.println("Bad successor %d.".format(j)); }
        }
      }
    }
  }
}
