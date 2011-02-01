package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.recipes.queue.DistributedQueue;
import org.apache.zookeeper.recipes.queue.Item;
import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.cdlib.ssconf.Configurator;

import sun.misc.{Signal, SignalHandler};

/** Generic work queue processor
 */
abstract class QueueProcessor (zooKeeperHosts : String, path : String, workers : Int) {
  var finished = false;
  var zookeeper = new ZooKeeper(zooKeeperHosts, 10000, 
                                new DistributedQueue.Ignorer());
  val q = new DistributedQueue(zookeeper, path, null);

  def handler (item : Item) : Unit;

  class Worker extends Thread {
    override def run {
      while (!finished) {
        try {
          val next = q.consume();
          if (next == null) {
            Thread.sleep(100);
          } else {
            handler(next);
          }
        } catch {
          case ex : NoSuchElementException => ();
          case ex : KeeperException => ();
        }
      }
    }
  }

  class Reconnect extends Thread {
    override def run {
      while (!finished) {
        for (n <- 1 to 60) {
          if (n == 0 && zookeeper.getState == ZooKeeper.States.CLOSED) {
              zookeeper = new ZooKeeper(zooKeeperHosts, 10000, 
                                        new DistributedQueue.Ignorer());
          }
          if (finished) return;
          Thread.sleep(1000);
        }
      }
    }
  }

  class Cleanup extends Thread {
    override def run {
      while (!finished) {
        for (n <- 1 to 3600) {
          /* every hour, clean up completed items */
          if (n == 1) {
            try { 
              q.cleanup(Item.COMPLETED);
            } catch {
              case ex : Exception => 
                System.err.println("Caught exception cleaning up.");
            }
          }
          if (finished) return;
          Thread.sleep(1000);
        }
      }
    }
  }

  def start {
    val threads = for (n <- 1 to workers) {
      (new Worker).start;
    }
    (new Cleanup).start;
    (new Reconnect).start;
  }
  
  def finish {
    finished = true;
  }
}
