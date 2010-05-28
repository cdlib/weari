package org.cdlib.was.ngIndexer;

import com.shorrockin.cascal.session._;
import com.shorrockin.cascal.utils.Conversions._;
import org.apache.thrift.transport.TTransportException;
import it.unimi.dsi.webgraph._;
import java.lang.UnsupportedOperationException;

class CassandraWebGraph extends WebGraph { // with ImmutableSequentialGraph {
  val hosts  = Host("localhost", 9160, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
  val pool   = new SessionPool(hosts, params, Consistency.One)  

  def addLink (link : Outlink) {
    this.addLinks(List(link));
  }
  
  def addLinks (links : Seq[Outlink]) {
    var done = false;
    var retryTimes = 0;
    //val added = new java.util.HashSet[String]();
    while (!done) {
      try {
        val key = "WebGraph" \\ "Outlinks";
        val inserts = for (l <- links)
                      yield Insert(key \ l.from \ "%s_%s".format(l.to, l.date) \ ("text", l.text));
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

  def list {
    val family = "WebGraph" \ "Outlinks";
    pool.borrow { session => 
      for (l <- session.list(family, KeyRange("h", "i", 100))) {
        System.out.println(l);
      }
    }
  }

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
