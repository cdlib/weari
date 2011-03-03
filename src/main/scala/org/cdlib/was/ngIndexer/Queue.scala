package org.cdlib.was.ngIndexer;
import org.apache.zookeeper.recipes.queue.DistributedQueue;
import org.apache.zookeeper.recipes.queue.Item;
import org.apache.zookeeper.{KeeperException, ZooKeeper};

/** Helper class to retry ZK operations */

class Queue(zookeeperHosts : String, path : String) {
  var zookeeper = new ZooKeeper(zookeeperHosts, 10000,
                                new DistributedQueue.Ignorer());

  val q = new DistributedQueue(zookeeper, path, null);

  val reconnectSync = new Object;

  def reconnect {
    reconnectSync.synchronized {
      zookeeper = new ZooKeeper(zookeeperHosts, 10000,
                                new DistributedQueue.Ignorer());
    }
  }
  
  def maybeReconnect {
    if (zookeeper.getState == ZooKeeper.States.CLOSED) {
      reconnect;
    }
  }

  def submit (bytes : Array[Byte]) = {
    retry {
      q.submit(bytes);
    }
  }

  def consume : Item = {
    retry {
      q.consume;
    }
  }

  def requeue (id : String) {
    retry {
      q.requeue(id);
    }
  }

  def complete (id : String) {
    retry {
      q.complete(id);
    }
  }
  
  def cleanup (b : Byte) {
    retry {
      q.cleanup(b);
    }
  }

  def retry[T] (what: => T) : T = {
    var i = 0;
    while (i < 3) {
      try {
        maybeReconnect;
        return what;
      } catch {
        case ex : KeeperException.SessionExpiredException =>
          /* Q: why do we need this? I would think maybeReconnect */
          /* would catch it */
          reconnect;
          i = i + 1;
          if (i >= 3) throw ex;
        case ex : KeeperException.ConnectionLossException =>
          reconnect;
          i = i + 1;
          if (i >= 3) throw ex;
      }
    }
    /* should be unreachable */
    return null.asInstanceOf[T];
  }
}
