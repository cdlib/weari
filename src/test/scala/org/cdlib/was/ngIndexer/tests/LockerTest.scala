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
  val locker = new Locker(zkhosts);

  feature ("Testing that ZooKeeper locking actually works.") {
    
    scenario ("Ensure that we can lock.") {
      locker.obtainLock (lockname) {
        assert(true);
      }
    }

    scenario ("Ensure that we can try to get a lock.") {
      locker.tryToObtainLock (lockname) {
        assert(true);
      } /* otherwise */ {
        assert(false);
      }
    }

    scenario ("Ensure that when we have a lock, others cannot.") {
      locker.obtainLock (lockname) {
        assert(true);
        locker.tryToObtainLock (lockname) {
          assert(false);
        } /* otherwise */ {
          assert(true);
        }
      }
    }

    scenario("Ensure against deadlocks.") {
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
    }
  }
}
