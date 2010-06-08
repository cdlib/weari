package org.cdlib.was.ngIndexer;

import java.util.Date;
import scala.collection.mutable.HashMap;
import org.archive.net.UURI;

class Outlink (val from : UURI,
               val to : UURI,
               val date : Date,
               val text : String) {}

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
