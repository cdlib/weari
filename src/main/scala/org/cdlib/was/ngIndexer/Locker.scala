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

class Locker (zkhosts : String) extends Retry {
  private def mkLockDir (session : ZkSessionManager, path : String) {
    ZkUtils.safeCreate(session.getZooKeeper, path, 
                       Array[Byte](),
                       Ids.OPEN_ACL_UNSAFE,
                       CreateMode.PERSISTENT);
  }

  def obtainLock[T] (lockName : String) (proc: => T) : T = {
    val session = new DefaultZkSessionManager(zkhosts, 10000);
    mkLockDir(session, lockName);
    val l = new ReentrantZkLock(lockName, session);
    l.lock;
    val retval = proc;
    l.unlock;
    session.closeSession;
    return retval;
  }  

  def tryToObtainLock[T] (lockName : String) (proc: => T) (otherwise: => T) : T = {
    val session = new DefaultZkSessionManager(zkhosts, 10000);
    mkLockDir(session, lockName);
    val l = new ReentrantZkLock(lockName, session);
    val retval = if (l.tryLock) proc; else otherwise;
    session.closeSession;
    return retval;
  }
}
