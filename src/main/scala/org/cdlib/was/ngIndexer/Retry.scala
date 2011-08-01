package org.cdlib.was.ngIndexer;

trait Retry extends Logger {
  /**
   * Retry an operation a number of times.
   * @param times Time to try to retry.
   * @param what What to do.
   * @param except What to do in case we get an exception while still in retry loop.
   */
  def retry (times : Int) (what : =>Unit) (except : (Exception)=>Unit) {
    var i = 0;
    var finished = false;
    while (i < times && !finished) {
      try {
        what;
        finished = true;
      } catch {
        case ex : Exception => {
          except(ex);
          i = i + 1;
          if (i >= times) throw ex;
        }
      }
    }
  }
  
  def retryLog (times : Int) (what : =>Unit) {
    retry (times) (what) { (ex)=>
      logger.error("Caught exception {}; retrying.", ex);
    }
  }
}
