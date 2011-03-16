package org.cdlib.was.ngIndexer;

import org.cdlib.mrt.queue.{DistributedQueue,Item};
import org.apache.zookeeper.ZooKeeper;

object QueueClient {
  def main (args : Array[String]) {
    if (args.size < 2) {
      System.err.println("Usage: ... ZOOKEEEPER_HOSTS QUEUE_NAME");
      System.exit(1);
    }
    var zookeeper = new ZooKeeper(args(0), 10000, 
                                  new DistributedQueue.Ignorer());
    val q = new DistributedQueue(zookeeper, args(1), null);
    var nextCommand = readLine();
    while (nextCommand != null) {
      q.submit(nextCommand.getBytes("UTF-8"));
      nextCommand = readLine();
    }
  }
}
