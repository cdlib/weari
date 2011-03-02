package org.cdlib.was.ngIndexer;

import org.slf4j.LoggerFactory;

trait Logger { this : Object =>
  val logger = LoggerFactory.getLogger(this.getClass);
}
