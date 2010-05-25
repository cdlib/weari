package org.cdlib.was.ngIndexer;

import com.shorrockin.cascal.session._;
import com.shorrockin.cascal.utils.Conversions._;
import org.apache.thrift.transport.TTransportException;

class CassandraWebGraph extends WebGraph {
  val hosts  = Host("localhost", 9160, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
  val pool   = new SessionPool(hosts, params, Consistency.One)  

  def addLink (link : Outlink) {
    this.addLinks(List(link));
  }
  
  def addLinks (links : Seq[Outlink]) {
    var done = false;
    var retry = false; // true if retrying
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
          if (retry) {
            throw ex; // two errors in a row!
          } else {
            retry = true;
            Thread.sleep(2);
          }
        }
      }
    }
  }
}
