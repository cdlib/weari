/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.cdlib.was.ngIndexer._;

import org.apache.zookeeper.recipes.queue.DistributedQueue;
import org.apache.zookeeper.recipes.queue.Item;
import org.apache.zookeeper.{KeeperException, ZooKeeper};

class QueueTest extends FeatureSpec {
  val zkhosts = "localhost:2181"
  val q = new Queue(zkhosts, "/test");
  feature ("We need to ensure that our queue is robust.") {
    scenario ("Ensure that we can enqueue an item.") {
      // q.zookeeper = new ZooKeeper(zkhosts, 10,
      //                             new DistributedQueue.Ignorer());
      for (i <- 1 to 10) {
        q.submit(Array[Byte](0, 1, 2, 3));
        Thread.sleep(50);
        val item = q.consume;
        q.complete(item.getId);
      }
      //causes exception
      //      q.cleanup(Item.COMPLETED);
    }
  }
}
