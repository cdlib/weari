package org.cdlib.was.ngIndexer.akka;

import akka.actor.Actor;

import org.cdlib.was.ngIndexer._;

class ArcIndexer extends Actor {
  val httpClient = new SimpleHttpClient;

  def receive = {
    case IndexArc (uri) => {
      val SolrIndexer.ArcRE(arcName) = uri.getPath;
      httpClient.getUri(uri) { (stream)=>
        eachRecord(stream, arcName) {(rec) => 
          val tmpFile = writeStreamToTempFile(rec);
          parsers ! ParseFile(tmpFile, rec);
        }
      }
    }
  }
}
