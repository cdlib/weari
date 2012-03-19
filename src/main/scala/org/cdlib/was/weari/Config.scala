/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.io.File;

import org.cdlib.ssconf.SSConfig;
import org.cdlib.ssconf.Value;

trait Config extends SSConfig {
  val threadCount     = new Value(5);
  val queueSize       = new Value(1000);
  val queueRunners    = new Value(3);
  val jsonCacheDir    = new Value(new File(System.getProperty("java.io.tmpdir")));
  val arcServerBase   = new Value("http://localhost:54480/arcs/%s");
  val jsonBaseDir     = new Value("/user/was/json");
}