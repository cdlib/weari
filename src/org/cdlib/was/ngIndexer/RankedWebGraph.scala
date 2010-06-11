package org.cdlib.was.ngIndexer;

import it.unimi.dsi.law.rank._;
import it.unimi.dsi.webgraph._;

class RankedWebGraph (basename : String) extends WebGraph {
  val bvg : BVGraph = BVGraph.load(basename);
  val urlFile = "%s.urls".format(bvg.basename);

  private def chomp(s: String): String = {
    try {
      if ((s.charAt(s.length - 1) == '\n') ||
          (s.charAt(s.length - 1) == '\r')) {
            s.substring(0, s.length - 1)
          } else if (s.substring(s.length - 2, s.length) == "\r\n") {
            s.substring(0, s.length - 2)
          } else {
            s
          }
    } catch {
      case ex: StringIndexOutOfBoundsException => s
    }
  }

  def addLink (link : Outlink) = ();
  def addLinks (links : Seq[Outlink]) = ();

  lazy val urls = scala.io.Source.fromFile(urlFile).getLines.map(chomp).toList.toArray;

  lazy val ranks = {
    val pr = new PageRankPowerMethod(bvg);
    pr.init;
    pr.stepUntil(new PageRank.NormDeltaStoppingCriterion(PageRank.DEFAULT_THRESHOLD));
    pr.rank;
  }

  def numNodes = bvg.numNodes;

  class RankedNodeIterator extends NodeIterator {
    var position = -1;
    val underlying = bvg.nodeIterator();
    def url = urls(position);
    
    override def outdegree = underlying.outdegree;

    override def hasNext = underlying.hasNext;
    
    override def successorArray = underlying.successorArray;

    override def nextInt : Int = next.asInstanceOf[Int];
      
    override def next : java.lang.Integer = {
      position = underlying.nextInt;
      return position;
    }

    def rank = ranks(position);

    def boost = (ranks(position) * numNodes).asInstanceOf[Float];
  }

  override def nodeIterator = new RankedNodeIterator();
}
