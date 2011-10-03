/* Copyright (c) 2011 The Regents of the University of California */

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

  def catchAndLogExceptions[T] (f: => T) : Option[T] = 
    catchAndLogExceptions("Caught exception {}.") (f);

  def catchAndLogExceptions[T] (formatStr : String) (f: => T) : Option[T] = {
    try {
      Some(f);
    } catch {
      case ex : Exception => {
        logger.error(formatStr, ex.toString);
        logger.debug(getStackTrace(ex));
      }
      None;
    }
  }
}
