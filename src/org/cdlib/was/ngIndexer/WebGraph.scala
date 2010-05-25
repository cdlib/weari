package org.cdlib.was.ngIndexer;

class Outlink (val from : String,
               val to : String,
               val date : String,
               val text : String) {}

trait WebGraph {
  def addLink (link : Outlink);
  def addLinks (links : Seq[Outlink]);
}
