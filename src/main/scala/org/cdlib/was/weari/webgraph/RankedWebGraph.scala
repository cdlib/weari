/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari.webgraph;

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
