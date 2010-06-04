package org.cdlib.was.ngIndexer;

import java.util.Date;
import org.archive.net.UURI;

class Outlink (val from : UURI,
               val to : UURI,
               val date : Date,
               val text : String) {}

trait WebGraph {
  def addLink (link : Outlink);
  def addLinks (links : Seq[Outlink]);
  def nodeIterator : it.unimi.dsi.webgraph.NodeIterator;
  def numNodes : Int;
}
