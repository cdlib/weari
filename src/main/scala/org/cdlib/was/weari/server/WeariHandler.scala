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

package org.cdlib.was.weari.server;

import grizzled.slf4j.Logging;

import java.util.{List => JList, Map => JMap, UUID};

import org.cdlib.was.weari._;

import scala.collection.JavaConversions.{ iterableAsScalaIterable, mapAsScalaMap, seqAsJavaList };
    
class WeariHandler(config: Config)
  extends thrift.Server.Iface with Logging with ExceptionLogger {

  val weari = new Weari(config);

  /**
   * Catch all non thrift._ exceptions and wrap them in a thrift._ Exception suitable
   * for sending back to a client.
   */
  def throwThriftException[T](f: => T) : T = {
    try {
      f;
    } catch {
      case ex : thrift.BadJSONException  => throw ex;
      case ex : thrift.UnparsedException => throw ex;
      case ex : thrift.ParseException    => throw ex;
      case ex : thrift.IndexException    => throw ex;
      case ex : Exception => {
        error(getStackTrace(ex));
        throw new thrift.IndexException(ex.toString);
      }
    }
  }

  private def convertMap (m : JMap[String, JList[String]]) : scala.collection.immutable.Map[String, Seq[String]] =
    mapAsScalaMap(m).toMap.mapValues(iterableAsScalaIterable(_).toSeq)
    
  /**
   * Index a set of ARCs on a solr server.
   *
   * @param solr The URI of the solr server to index on.
   * @param filterQuery A solr query string to return candidates for
   *   documents to be possibly merged with.
   * @param arcs A list of ARC names to index
   * @param extraId String to append to solr document IDs.
   * @param extraFields Map of extra fields to append to solr documents.
   */
  def index(solr : String,
            filterQuery : String,
            arcs : JList[String],
            extraId : String,
            extraFields : JMap[String, JList[String]]) {
    throwThriftException {
      weari.index(solr, filterQuery, arcs.toSeq, extraId, convertMap(extraFields));
    }
  }

  /**
   * Set fields unconditionally on a group of documents retrieved by a query string.
   */
  def setFields(solr : String,
                queryString : String,
                fields : JMap[String, JList[String]]) {
    throwThriftException {
      weari.setFields(solr, queryString, convertMap(fields));
    }
  }
               
  /**
   * Remove index entries for these ARC files from the solr server.
   */
  def remove(solr : String,
             arcs : JList[String]) {
    throwThriftException {
      weari.remove(solr, arcs.toSeq);
    }
  }

  def clearMergeManager(managerId : String) {
    throwThriftException {
      weari.clearMergeManager(managerId);
    }
  }

  /**
   * Check to see if a given ARC file has been parsed.
   */
  def isArcParsed (arcName : String) : Boolean = {
    throwThriftException {
      weari.isArcParsed(arcName);
    }
  }

  /**
   * Parse ARC files.
   */
  def parseArcs (arcs : JList[String]) {
    throwThriftException {
      weari.parseArcs(arcs.toSeq);
    }
  }

  def deleteParse (arc : String) {
    throwThriftException {
      weari.deleteParse(arc);
    }
  }
}
