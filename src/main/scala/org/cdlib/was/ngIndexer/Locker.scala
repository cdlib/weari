package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.{KeeperException,ZooKeeper};

import org.apache.zookeeper.recipes.queue.DistributedQueue;

import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};

import org.menagerie.locks.ReentrantZkLock;
import org.menagerie.ZkUtils;

class Locker (zkhosts : String) extends ZkRetry {
  var lockers = scala.collection.mutable.Map[String, ReentrantZkLock]();
  val session = new DefaultZkSessionManager(zkhosts, 100000);
  val sync = new Object;

  def getLock (lockName : String) : ReentrantZkLock = {
    lockers.get(lockName) match {
      case Some(lock) => lock;
      case None => {
        sync.synchronized {
          val lock = new ReentrantZkLock(lockName, session);
          lockers.put(lockName, lock);
          lock;
        }
      }
    }
  }

  def obtainLock[T] (lockName : String) (proc: => T) : T = {
    val l = getLock(lockName);
    l.lock;
    val retval = proc;
    l.unlock;
    return retval;
  }

  def tryToObtainLock[T] (lockName : String) (proc: => T) (otherwise: => T) : T = {
    val l = getLock(lockName);
    val retval = if (l.tryLock) proc; else otherwise;
    return retval;
  }
  
  def finish { session.closeSession; }
}
