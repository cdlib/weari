package org.cdlib.was.ngIndexer;

import com.shorrockin.cascal.session._;
import com.shorrockin.cascal.utils.Conversions._;
import com.shorrockin.cascal.model;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TApplicationException;
import it.unimi.dsi.webgraph._;
import java.lang.UnsupportedOperationException;
import org.archive.net.UURI;
import org.archive.util.ArchiveUtils;
import java.util.Date;
import scala.collection.mutable.HashMap;

class CassandraWebGraph extends ImmutableSequentialGraph with WebGraph {
  val hosts  = Host("localhost", 9160, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 1000L, 6, 2)
  val pool   = new SessionPool(hosts, params, Consistency.One)  

  val leastFp    = UriUtils.long2bytearray(0);
  val greatestFp = UriUtils.long2bytearray(0xffffffffffffffffL);

  override def toString = "WebGraph";
  
  def addLink (link : Outlink) {
    this.addLinks(List(link));
  }

  var numNodesFinished = false;
  var n = 0;
  var knownUrls = new HashMap[Long, Int]();

  def fp2id (l : Long) : Int = {
    knownUrls.get(l) match {
      case Some(i) => return i;
      case None    => {
        knownUrls.update(l, n);
        n = n + 1;
        return n - 1;
      }
    }
  }
  
  val columnFamily = "WebGraph" \\ "Outlinks";

  def numNodes : Int = {
    if (numNodesFinished) {
      return n;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /* adds a URL to our collection, if necessary, returns its fingerprint */
  def addUrl (url : UURI) : Long = {
    val fp = UriUtils.fingerprint(url);
    val fpbytes = UriUtils.encodeFp(fp);
    pool.borrow { session =>
      val key = "WebGraph" \ "Urls" \ fpbytes;
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
          val fromfp = addUrl(l.from);
          val tofp = addUrl(l.to);
          val fpdate = UriUtils.encodeFpDate(tofp, l.date);
          val i = Insert(columnFamily \ UriUtils.encodeFp(fromfp) \ fpdate \ ("text", l.text));
          i;
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
      var position = 0L;
      var currentKey : Option[model.Key[model.SuperColumn, Map[model.SuperColumn, Seq[model.Column[model.SuperColumn]]]]] = None;
      var currentVal : Option[Map[model.SuperColumn,Seq[model.Column[model.SuperColumn]]]] = None;
      var nextKey : Option[model.Key[model.SuperColumn, Map[model.SuperColumn, Seq[model.Column[model.SuperColumn]]]]] = None;
      var nextVal : Option[Map[model.SuperColumn,Seq[model.Column[model.SuperColumn]]]] = None;
      
      override def outdegree = {
        currentVal match {
          case None    => -1;
          case Some(v) => v.size;
        }
      }
      
      def fillNext {
        if (nextKey.isEmpty) {
          pool.borrow { session =>
            val res = session.list(columnFamily, KeyRange(UriUtils.encodeFp(position+1), greatestFp, 1));
            res.keys.find(a=>true) match { /* find the first one */
              case None => System.err.println("Could not get key");
              case Some(key) => {
                if (UriUtils.decodeFp(key.value) == position) {
                  /* we have reached the end */
                  nextKey = None;
                  nextVal = None;
                } else {
                  nextKey = Some(key);
                  nextVal = res.get(key);
                }
              }
            }
          }
        }
      }
    
      override def hasNext : Boolean = {
        fillNext;
        if (nextKey.isEmpty) {
          numNodesFinished = true;
          return false;
        } else { return true; }
      }
      
      override def successorArray : Array[Int] = currentVal match {
        case None    => return new Array[Int](0);
        case Some(v) => {
          val ids = for (k <- v.keySet)
                    yield fp2id(UriUtils.decodeFp(k.value.take(8)));
          return ids.toList.toArray[Int];
        }
      }
      
      override def nextInt : Int = next.asInstanceOf[Int];
      
      override def next : java.lang.Integer = {
        if (!hasNext) {
          throw new NoSuchElementException();
        } else {
          currentKey = nextKey;
          currentVal = nextVal;
          nextKey = None;
          nextVal = None;
          position = UriUtils.decodeFp(currentKey.getOrElse(null).key.value.getBytes);
          return fp2id(position);
        }
      }
    }
}
