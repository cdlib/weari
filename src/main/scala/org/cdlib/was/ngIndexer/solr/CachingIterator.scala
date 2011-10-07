/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer.solr;

import scala.collection.mutable.ArrayBuffer;

abstract class CachingIterator[T] extends Iterator[T] {
  protected var cachePos = 0;
  protected var cache = new ArrayBuffer[T]();
  
  def fillCache;

  def _fillCache {
    if (cache.length <= cachePos) {
      cache.clear;
      cachePos = 0;
      fillCache;
    }
  }

  def peek : Option[T] = 
    if (hasNext) { Some(cache(cachePos)); }
    else { return None; }

  def hasNext : Boolean = {
    if (cache.length <= cachePos) _fillCache;
    return (cache.length != 0);
  }
  
  def next : T = { 
    if (!hasNext)  throw new NoSuchElementException();
    val retval = cache(cachePos);
    cachePos += 1;
    return retval;
  }
}
