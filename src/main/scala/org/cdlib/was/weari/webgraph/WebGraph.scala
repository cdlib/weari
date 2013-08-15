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

import scala.collection.mutable.HashMap;

abstract class WebGraph {
  def addLink (link : Outlink);
  def addLinks (links : Seq[Outlink]);
//  def nodeIterator : it.unimi.dsi.webgraph.NodeIterator;
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

// object WebGraph {
//   def check (wg : WebGraph) {
//     val g = new MyImmutableSequentialGraph(wg);
//     val it = g.nodeIterator;
//     var i = 0;
//     while (it.hasNext) {
//       val u = it.next;
//       if (u != i) throw new RuntimeException("node seq error %d != %d.".format(i, u));
//       i = i + 1;
//     }
//     val numNodes = g.numNodes;
//     if (numNodes != i) throw new RuntimeException("num nodes error.");
//     val it2 = g.nodeIterator;
//     while (it2.hasNext) {
//       it2.next;
//       if (it.outdegree > 0) {
//         val a = it.successorArray;
//         if (a.length != it.outdegree) { System.err.println("Bad outdegree.") };
//         for (j <- a) {
//           if (j > numNodes) { System.err.println("Bad successor %d.".format(j)); }
//         }
//       }
//     }
//   }
// }
