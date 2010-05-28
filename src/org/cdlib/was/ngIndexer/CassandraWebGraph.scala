package org.cdlib.was.ngIndexer;

import com.shorrockin.cascal.session._;
import com.shorrockin.cascal.utils.Conversions._;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TApplicationException;
import it.unimi.dsi.webgraph._;
import java.lang.UnsupportedOperationException;
import org.archive.net.UURI;
import org.archive.util.ArchiveUtils;
import java.util.Date;

class CassandraWebGraph extends WebGraph { // with ImmutableSequentialGraph {
  val hosts  = Host("localhost", 9160, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
  val pool   = new SessionPool(hosts, params, Consistency.One)  

  def addLink (link : Outlink) {
    this.addLinks(List(link));
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
        val columnFamily = "WebGraph" \\ "Outlinks";
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

  // def numNodes : Int = {
  //   pool.borrow { session =>
  //     session.count(ColumnContainer("WebGraph" \\ "Outlinks"));
  //   }
  //}

  // val leastFp    = UriUtils.long2bytearray(0);
  // val greatestFp = UriUtils.long2bytearray(0xffffffffffffffffL);

  // def list {
  //   val family = "WebGraph" \ "Outlinks";
  //   pool.borrow { session => 
  //     for (l <- session.list(family, Range(leastFp, greatestFp, 100))) {
  //       System.out.println(l);
  //     }
  //   }
  // }

  // //def copy : ImmutableSequentialGraph = {
  // //return new CassandraWebGraph();
  // //}

  // class MyNodeIterator (var i)
  //   extends ArcLabelledNodeIterator {
    
  //   def hasNext : Boolean = {
  //   }
    
  //   def nextInt {
  //     if (!this.hasNext) throw new NoSuchElementException();
      
  //     def outdegree : int = {
  //     return 0;
  //   }
    
  //   override def successors :  = {
  // def nodeIterator : NodeIterator = {
    
}
