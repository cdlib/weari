/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.cdlib.was.ngIndexer._;

import org.apache.zookeeper.{KeeperException,ZooKeeper};

import org.apache.zookeeper.recipes.queue.DistributedQueue;

import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import java.lang.Math.{random,round};

class LockerTest extends FeatureSpec {
  val zkhosts = "localhost:2181";
  val lockname = "/testLocker";

  feature ("Testing that ZooKeeper locking actually works.") {
    
    scenario ("Ensure that we can lock.") {
      val locker = new Locker(zkhosts);
      locker.obtainLock (lockname) {
        assert(true);
      }
      locker.finish;
    }

    scenario ("Ensure that we can try to get a lock.") {
      val locker = new Locker(zkhosts);
      locker.tryToObtainLock (lockname) {
        assert(true);
      } /* otherwise */ {
        assert(false);
      }
      locker.finish;
    }

    scenario ("Ensure that when we have a lock, others cannot.") {
      val locker = new Locker(zkhosts);
      locker.obtainLock (lockname) {
        assert(true);
        val t = new Thread {
          override def run {
            locker.tryToObtainLock (lockname) {
              assert(false);
            } /* otherwise */ {
              assert(true);
            }
          }
        }
        t.start;
        t.join;
      }
      locker.finish;
    }

    scenario("Ensure against deadlocks.") {
      val locker = new Locker(zkhosts);
      val threads = (0 to 50).map { (i) =>
        new Thread {
          override def run {
            locker.obtainLock(lockname) {
              assert(true);
            }
            System.err.println("finished %d".format(i));
          }
        }
      }
      threads.map{t=>t.start}
      threads.map{t=>t.join}
      locker.finish;
    }
  }
}
