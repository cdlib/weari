/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.File;

import org.cdlib.ssconf.SSConfig;
import org.cdlib.ssconf.Value;

trait Config extends SSConfig {
  val threadCount     = new Value(5);
  val queueSize       = new Value(1000);
  val jsonBaseDir     = new Value("/user/was/json");
}
