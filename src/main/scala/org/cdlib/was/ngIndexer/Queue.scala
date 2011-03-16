package org.cdlib.was.ngIndexer;

import org.cdlib.mrt.queue.{DistributedQueue,Item};
import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};

/** Helper class to retry ZK operations */

class Queue(hosts : String, path : String) {

  val session = new DefaultZkSessionManager(hosts, 10000);

  def q : DistributedQueue = new DistributedQueue(session.getZooKeeper, path, null);

  def submit (bytes : Array[Byte]) { retry { q.submit(bytes); } }

  def consume : Item = { retry { q.consume; } }

  def requeue (item : Item) { retry { q.requeue(item); } }

  def complete (id : String) { retry { q.complete(id); } }
  
  def cleanup (b : Byte) { retry { q.cleanup(b); } }

  private def retry[T] (what: => T) : T = {
    var i = 0;
    while (i < 3) {
      try {
        return what;
      } catch {
        case ex : KeeperException.ConnectionLossException =>
          i = i + 1;
          if (i >= 3) throw ex;
      }
    }
    /* should be unreachable */
    return null.asInstanceOf[T];
  }

  def close = { session.closeSession; }
}
