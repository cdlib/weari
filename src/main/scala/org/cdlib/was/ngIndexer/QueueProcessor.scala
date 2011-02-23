package org.cdlib.was.ngIndexer;

import java.util.NoSuchElementException;
import org.apache.zookeeper.recipes.queue.DistributedQueue;
import org.apache.zookeeper.recipes.queue.Item;
import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.cdlib.ssconf.Configurator;

import org.slf4j.LoggerFactory

import sun.misc.{Signal, SignalHandler};

trait QueueItemHandler {
  def handle (item : Item) : Boolean;
}

trait QueueItemHandlerFactory {
  def mkHandler (zooKeeper : ZooKeeper) : QueueItemHandler;
}

/** Generic work queue processor
 */
class QueueProcessor (zooKeeperHosts : String, path : String, workers : Int, handlerFactory : QueueItemHandlerFactory) {
  var finished = false;
  var zookeeper = new ZooKeeper(zooKeeperHosts, 10000, 
                                new DistributedQueue.Ignorer());
  val q = new DistributedQueue(zookeeper, path, null);
  val logger = LoggerFactory.getLogger(classOf[QueueProcessor]);

  class Worker (handler : QueueItemHandler) extends Thread {    
    override def run {
      while (!finished) {
        try {
          reconnect;
          val next = q.consume;
          if (next == null) {
            Thread.sleep(100);
          } else {
            if (handler.handle(next)) {
              /* if it returns true, consider this finished */
              q.complete(next.getId);
            } else {
              q.requeue(next.getId);
            }
          }
        } catch {
          case ex : NoSuchElementException => ();
          case ex : KeeperException => ();
        }
      }
    }
  }

  val reconnectSync = new Object;
  def reconnect {
    reconnectSync.synchronized {
      if (zookeeper.getState == ZooKeeper.States.CLOSED) {
        zookeeper = new ZooKeeper(zooKeeperHosts, 10000, 
                                  new DistributedQueue.Ignorer());
      }
    }
  }

  class Cleanup extends Thread {
    override def run {
      while (!finished) {
        for (n <- 1 to 3600) {
          /* every hour, clean up completed items */
          if (n == 3600) {
            try { 
              reconnect;
              q.cleanup(Item.COMPLETED);
            } catch {
              case ex : NoSuchElementException => ();
              case ex : Exception => 
                logger.error("Caught exception {} cleaning up.", ex);
            }
          }
          if (finished) return;
          Thread.sleep(1000);
        }
      }
    }
  }

  def start {
    for (n <- 1 to workers)
      (new Worker(handlerFactory.mkHandler(zookeeper))).start;
    (new Cleanup).start;
  }
  
  def finish {
    finished = true;
  }
}
