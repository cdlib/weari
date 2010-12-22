package org.cdlib.was.ngIndexer;

import org.cdlib.ssconf.SSConfig;

trait Config extends SSConfig {
  val indexers : Value[Seq[Pair[String,Int]]] = new Value[Seq[Pair[String,Int]]];
}
