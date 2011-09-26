/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.cdlib.mrt.queue.Item;

import org.cdlib.ssconf.Configurator;

import sun.misc.{Signal, SignalHandler};

/**
 * Subclass this to write handlers for items from the queue.
 * 
 */
trait QueueItemHandler {
  /**
   * @return true if the item was handled sucessfully.
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
class QueueProcessor (hosts : String, path : String, workers : Int, handlerFactory : QueueItemHandlerFactory) extends Logger {
  var finished = false;

  class Worker (handler : QueueItemHandler) extends Thread ("QueueWorker") {    
    override def run {
      val q = new Queue(hosts, path);
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
              q.requeue(next);
            }
          }
        } catch {
          case ex : Exception =>
            logger.error("Caught exception {} processing item.", ex);
        }
      }
      q.close;
    }
  }

  class Cleanup extends Thread {
    val MS_BTWN_CLEANUP = 3600000; // 1 hour
    override def run {
      var lastCleanup = System.currentTimeMillis;
      val q = new Queue(hosts, path);
      while (!finished) {
        if (System.currentTimeMillis < (lastCleanup + MS_BTWN_CLEANUP)) {
          Thread.sleep(1000);
        } else {
          try { 
            q.cleanup(Item.COMPLETED);
          } catch {
            case ex : Exception => 
              logger.error("Caught exception {} cleaning up.", ex);
          } finally {
            lastCleanup = System.currentTimeMillis;
          }
        }
      }
      q.close;
    }
  }

  var threads = List[Thread]();

  def start {
    for (n <- 1 to workers) {
      val thread = new Worker(handlerFactory.mkHandler);
      thread.start;
      threads = thread :: threads;
    }
    val cleanupThread = new Cleanup;
    cleanupThread.start;
    threads = cleanupThread :: threads;
  }
  
  def finish {
    finished = true;
  }
}
