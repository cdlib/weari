/* (c) 2009-2010 Regents of the University of California */

package org.cdlib.was.ngIndexer.tests;

import org.scalatest.{FeatureSpec,GivenWhenThen};
import org.cdlib.was.ngIndexer._;

class ConsistentHashRingSpec extends FeatureSpec with GivenWhenThen {
  feature ("We need to order longs according to their bits.") {
    scenario ("Check ring ordering.") {
      val ring = new ConsistentHashRing[String];
      val one     =  1L; 0x00000
      val zero    =  0L;
      val largest = -1L;
      for (i <- 1.to(62)) {
        then("%s should be larger than no bits set".format(UriUtils.long2string(one << i)));
        assert (ring.compare(one << i, zero) == 1);

        then("%s should be less than all bits set".format(UriUtils.long2string(one << i)));
        assert (ring.compare(one << i, largest) == -1);

        then("%s should be less than all bits set".
             format(UriUtils.long2string(largest >>> (64-i))));
        assert (ring.compare(largest >>> (64-i), largest) == -1);

        then("%s should be larger than %s".format(UriUtils.long2string(one << i),
                                                  UriUtils.long2string(one << (i-1))));
        assert (ring.compare(one << (i+1), one << i) == 1);
        then("%s should be less than %s".format(UriUtils.long2string(largest >>> (64-i)),
                                                UriUtils.long2string(one << i)));
        assert (ring.compare(largest >>> (64-i), one << i) == -1);
      }
    }
  }
  
  feature ("We need to distribute items evenly.") {
    scenario ("We have two servers") {
      val rand = new java.util.Random;
      val ring = new ConsistentHashRing[Int];
      ring.addServer(1);
      ring.addServer(2);
      ring.addServer(3);
      val buff = new Array[Byte](1024);
      var count1 = 0;
      var count2 = 0;
      var count3 = 0;
      var histo = new Array[Int](8);
      for (i <- 0.to(10000)) {
        rand.nextBytes(buff);
        val h = ring.hash(buff);
        val j = (h >>> 61).asInstanceOf[Int];
        histo(j) = histo(j) + 1;
        val server = ring.getServerFor(h);
        if (server == 1) {
          count1 += 1;
        } else if (server == 2) {
          count2 += 1;
        } else if (server == 3) {
          count3 += 1;
        } else {
          assert (false);
        }
      }
      /* check reasonable bounds */
      assert (count1 + 500 > count2);
      assert (count1 - 500 < count2);
      assert (count2 + 500 > count3);
      assert (count2 - 500 < count3);
      assert (count1 + 500 > count3);
      assert (count1 - 500 < count3);
    }
  }
}
