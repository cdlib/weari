package org.cdlib.was.ngIndexer;

import java.util.NoSuchElementException;
import org.apache.zookeeper.recipes.queue.DistributedQueue;
import org.apache.zookeeper.recipes.queue.Item;
import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.cdlib.ssconf.Configurator;

import sun.misc.{Signal, SignalHandler};

/**
 * Subclass this to write handlers for items from the queue.
 * 
 */
trait QueueItemHandler {
  /**
   * @returns true if the item was handled sucessfully.
   */
  def handle (item : Item) : Boolean;
}

/**
 * Subclass to create a factory to create items from the queue.
 *
 * Factory is used to allow users to define variables in the factory
 * scope which will be shared by workers.
 */
trait QueueItemHandlerFactory {
  def mkHandler : QueueItemHandler;
}

/**
 * A generic work queue processor
 */
class QueueProcessor (zooKeeperHosts : String, path : String, workers : Int, handlerFactory : QueueItemHandlerFactory) extends Logger {
  var finished = false;
  val q = new Queue(zooKeeperHosts, path);

  class Worker (handler : QueueItemHandler) extends Thread {    
    override def run {
      while (!finished) {
        try {
          val next = q.consume;
          if (next == null) {
            Thread.sleep(1000);
          } else {
            if (handler.handle(next)) {
              /* if it returns true, consider this finished */
              q.complete(next.getId);
            } else {
              q.requeue(next.getId);
            }
          }
        } catch {
          case ex : Exception =>
            logger.error("Caught exception {} processing item.", ex);
        }
      }
    }
  }

  class Cleanup extends Thread {
    val MS_BTWN_CLEANUP = 3600000; // 1 hour
    override def run {
      var lastCleanup = System.currentTimeMillis;
      while (!finished) {
        if (System.currentTimeMillis < (lastCleanup + MS_BTWN_CLEANUP)) {
          Thread.sleep(1000);
        } else {
          try { 
            q.cleanup(Item.COMPLETED);
          } catch {
            case ex : NoSuchElementException => ();
            case ex : Exception => 
              logger.error("Caught exception {} cleaning up.", ex);
          } finally {
            lastCleanup = System.currentTimeMillis;
          }
        }
      }
    }
  }

  def start {
    for (n <- 1 to workers)
      (new Worker(handlerFactory.mkHandler)).start;
    (new Cleanup).start;
  }
  
  def finish {
    finished = true;
  }
}
