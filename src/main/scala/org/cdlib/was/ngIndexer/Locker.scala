package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.{KeeperException,ZooKeeper};

import org.apache.zookeeper.recipes.queue.DistributedQueue;

import org.apache.zookeeper.recipes.lock.WriteLock;

class Locker (zooKeeperHosts : String, lockRoot : String) {
  var zookeeper = new ZooKeeper(zooKeeperHosts, 10000, 
                                new DistributedQueue.Ignorer());
  val reconnectSync = new Object;

  /* init locking */
  try {
    if (zookeeper.exists(lockRoot, false) == null) {
      zookeeper.create(lockRoot, Array[Byte](),
                       Ids.OPEN_ACL_UNSAFE,
                       CreateMode.PERSISTENT);
    }
  } catch {
    /* probably should do something here, but it is not clear why we
       lose our connection */
    case ex : KeeperException.ConnectionLossException => ();
  }

  def obtainLock[T] (lockName : String) (proc: => T) : T = {
    var l = new WriteLock(zookeeper, "%s/%s".format(lockRoot, lockName), null);
    if (!l.lock) { while (!l.isOwner) { Thread.sleep(100); } }
    val retval = proc;
    l.unlock;
    return retval;
  }  

  def tryToObtainLock[T] (lockName : String) (proc: => T) (orElse: => T) : T = {
    var l = new WriteLock(zookeeper, "%s/%s".format(lockRoot, lockName), null);
    val retval = if (l.lock) proc; else orElse;
    l.unlock;
    return retval;
  }    
}
