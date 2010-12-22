package org.cdlib.was.ngIndexer;

import scala.collection.mutable.HashMap;

abstract class WebGraph {
  def addLink (link : Outlink);
  def addLinks (links : Seq[Outlink]);
  def nodeIterator : it.unimi.dsi.webgraph.NodeIterator;
  def numNodes : Int;

  var knownUrlCounter = 0;
  var knownUrls = new HashMap[Long, Int]();
  
  def fp2id (l : Long) : Int = {
    knownUrls.get(l) match {
      case Some(i) => return i;
      case None    => {
        knownUrls.update(l, knownUrlCounter);
        knownUrlCounter = knownUrlCounter + 1;
        return knownUrlCounter - 1;
      }
    }
  }
}

object WebGraph {
  def check (wg : WebGraph) {
    val g = new MyImmutableSequentialGraph(wg);
    val it = g.nodeIterator;
    var i = 0;
    while (it.hasNext) {
      val u = it.next;
      if (u != i) throw new RuntimeException("node seq error %d != %d.".format(i, u));
      i = i + 1;
    }
    val numNodes = g.numNodes;
    if (numNodes != i) throw new RuntimeException("num nodes error.");
    val it2 = g.nodeIterator;
    while (it2.hasNext) {
      it2.next;
      if (it.outdegree > 0) {
        val a = it.successorArray;
        if (a.length != it.outdegree) { System.err.println("Bad outdegree.") };
        for (j <- a) {
          if (j > numNodes) { System.err.println("Bad successor %d.".format(j)); }
        }
      }
    }
  }
}
