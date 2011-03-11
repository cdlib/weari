package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.recipes.queue.DistributedQueue;
import org.apache.zookeeper.recipes.queue.Item;
import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};

/** Helper class to retry ZK operations */

class Queue(zooKeeperHosts : String, path : String) {
  val session = new DefaultZkSessionManager(zooKeeperHosts, 10000);

  def q : DistributedQueue = new DistributedQueue(session.getZooKeeper, path, null);

  def submit (bytes : Array[Byte]) { retry { q.submit(bytes); } }

  def consume : Item = { retry { q.consume; } }

  def requeue (id : String) { retry { q.requeue(id); } }

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
}
