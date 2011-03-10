package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.{KeeperException,ZooKeeper};

import org.apache.zookeeper.recipes.queue.DistributedQueue;

import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};
import org.menagerie.locks.ReentrantZkLock;

class Locker (zooKeeperHosts : String, lockRoot : String) {
  val session = new DefaultZkSessionManager(zooKeeperHosts, 10000);
  
  if (session.getZooKeeper.exists(lockRoot, false) == null) {
    session.getZooKeeper.create(lockRoot, Array[Byte](),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
  }

  def obtainLock[T] (lockName : String) (proc: => T) : T = {
    val l = new ReentrantZkLock("%s/%s".format(lockRoot, lockName), session);
    l.lock;
    val retval = proc;
    l.unlock;
    return retval;
  }  

  def tryToObtainLock[T] (lockName : String) (proc: => T) (otherwise: => T) : T = {
    val l = new ReentrantZkLock("%s/%s".format(lockRoot, lockName), session);
    return if (l.tryLock) proc; else otherwise;
  }
  
  def finish { session.closeSession; }

}
