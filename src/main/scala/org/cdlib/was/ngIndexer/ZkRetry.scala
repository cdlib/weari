package org.cdlib.was.ngIndexer;

import org.apache.zookeeper.KeeperException;

trait ZkRetry {
  /**
   * Retry an operation a number of times, catch ZookeeperExceptions
   * @param what What to do.
   */
  def retry[T] (what: => T) : T = {
    var i = 0;
    while (true) {
      try {
        val retval = what;
        return what;
      } catch {
        case ex : KeeperException.ConnectionLossException =>
          i = i + 1;
          if (i >= 10) throw ex;
        case ex : RuntimeException =>
          /* assuming a ZK exception */
          i = i + 1;
          if (i >= 10) throw ex;          
        case ex : Throwable => 
          throw ex;
      }
    }
    /* should be unreachable */
    return null.asInstanceOf[T];
  }
}
