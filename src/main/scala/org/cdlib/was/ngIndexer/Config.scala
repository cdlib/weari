package org.cdlib.was.ngIndexer;

import org.cdlib.ssconf.SSConfig;
import org.cdlib.ssconf.Value;

trait Config extends SSConfig {
  val indexers : Value[Seq[Tuple3[String,String,Int]]] = 
    new Value[Seq[Tuple3[String,String,Int]]];
  val zooKeeperHosts = new Value[String]();
  val zooKeeperPath  = new Value("/arcIndexQueue");
  val threadCount    = new Value(5);
  val queueSize      = new Value(1000);
  val queueRunners   = new Value(3);
}
