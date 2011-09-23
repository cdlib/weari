package org.cdlib.was.ngIndexer;

import java.io.{PrintWriter,StringWriter};

import org.slf4j.LoggerFactory;

trait Logger { self =>

  val logger = LoggerFactory.getLogger(self.getClass)

  def getStackTrace(th : Throwable) : String = {
    val result = new StringWriter;
    val printWriter = new PrintWriter(result);
    th.printStackTrace(printWriter);
    return result.toString;
  }

  def catchAndLogExceptions (f: => Unit) {
    try {
      f;
    } catch {
      case ex : Exception => {
        logger.error("Caught exception {}.", ex.toString);
        logger.debug(getStackTrace(ex));
      }
    }
  }
}
