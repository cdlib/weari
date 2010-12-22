package org.cdlib.was.ngIndexer;

import org.cdlib.ssconf.SSConfig;

trait Config extends SSConfig {
  val indexers : Value[Seq[Tuple3[String,String,Int]]] = new Value[Seq[Tuple3[String,String,Int]]];
}
