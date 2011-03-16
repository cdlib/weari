/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.cdlib.was.ngIndexer._;

import org.cdlib.mrt.queue.Item;

import org.apache.zookeeper.{KeeperException, ZooKeeper};

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};

class QueueTest extends FeatureSpec {
  val zkhosts = "localhost:2181"
  feature ("We need to ensure that our queue is robust.") {
    scenario ("Ensure that we can enqueue an item.") {
      val q = new Queue(zkhosts, "/test");
      for (i <- 1 to 10) {
        q.submit(Array[Byte](0, 1, 2, 3));
        Thread.sleep(50);
        val item = q.consume;
        q.complete(item.getId);
      }
      q.cleanup(Item.COMPLETED);
      q.close;
    }
    
    scenario ("Reqeueing") {
      val q = new Queue(zkhosts, "/test");
      q.cleanup(Item.COMPLETED);
      q.cleanup(Item.PENDING);
      q.submit("1".getBytes("UTF-8"));
      q.submit("2".getBytes("UTF-8"));
      val item0 = q.consume;
      assert (new String(item0.getData) == "1");
      q.requeue(item0);
      val item1 = q.consume;
      assert (new String(item1.getData) == "2");
      q.complete(item1.getId);
      q.complete(q.consume.getId);
      q.close;
    }
  }
}
