package org.cdlib.was.ngIndexer;

import org.apache.commons.codec.digest.DigestUtils;
import org.cdlib.rabinpoly.RabinPoly;
import scala.collection.immutable.SortedMap;
import scala.collection.immutable.TreeMap;

/** A consistent hashing ring.
  * 
  * @author egh
  */
class ConsistentHashRing[T] {
  implicit object UnsignedLongOrdering extends Ordering[Long] {
    /** Compare two longs not as twos complement numbers, but as a collection of raw bits.
      */
    def compare(x : Long, y : Long) : Int = {
      if (x == y) {
        return 0;
      } else {
        val mask = x ^ y; // first set bit will indicate first different bit
        var l = 0; // l will be lg(mask)
        var tmp = mask >>> 1;
        while (tmp != 0) {
          l += 1; tmp = tmp >> 1;
        } 
        return ((x >>> l) - (y >>> l)).asInstanceOf[Int];
      }
    }
  }

  val DEFAULT_LEVEL = 10;
  
  /* locations of servers on our ring */
  private var locations : SortedMap[Long, T] = new TreeMap[Long,T]();

  private var levels : Map[String, Int] = Map[String, Int]();

  def hash (bytes : Array[Byte]) : Long = 
    UriUtils.bytearray2long(DigestUtils.md5(bytes).slice(8,16));

  def hash (str : String) : Long = hash(str.getBytes("UTF-8"))

  /** Return the server to use for an item with a given place in the ring.
    *
    * @param l The place in the ring.
    */
  def getServerFor(l : Long) : T = {
    val r = locations.range(l, -1L);
    if (r.size == 0) { 
      /* special case when we are at the end of the ring */
      return locations.head._2;
    } else { 
      /* return the next server */
      return r.head._2;
    }
  }

  def getServerFor(id : String) : T = getServerFor(hash(id));

  /** Add a server to the ring.
    *
    * @param id The string to use to generate the locations of the
    * server on the ring.
    * @param t The value to return as the server.
    * @param level Number of locations on the ring that this server will occupy.
    */
  def addServer(id : String, t : T, level : Int) {
    this.levels = levels + Pair(id, level);
    for (loc <- getServerLocations(id, level)) {
      this.locations = this.locations + Pair(loc, t);
    }
  }

  def addServer(id : String, t : T) {
    addServer(id, t, DEFAULT_LEVEL);
  }

  def addServer(t : T) {
    addServer(t.toString, t);
  }
  
  /** Remove a server from the ring.
    *
    * @param id The id of the server to remove.
    */
  def removeServer(id : String) {
    val level = levels.get(id).getOrElse(DEFAULT_LEVEL);
    this.levels = this.levels - id;
    for (loc <- getServerLocations(id, level)) {
      this.locations = this.locations - loc;
    }
  }
  
  /** Get the locations which should be used for a server.
    * Will be consistent for any id.
    */
  private def getServerLocations(id : String, level : Int) : Seq[Long] =
    for (i <- 0.to(level)) 
      yield hash("%s-%d".format(id, i));

  def getServers : Seq[T] = locations.values.toList.distinct;
}
