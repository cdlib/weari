package org.cdlib.was.ngIndexer.akka;

import akka.actor.Actor;

import org.cdlib.was.ngIndexer.MyParser;

class ParserWorker extends Actor {
  val parser = new MyParser;

  def receive = {
    case ParseFile(file, record) => {
      val result = parser.parse(file, record.getMediaTypeStr, record.getUrl, record.getDate);
    }      
    case ParseBytes(bytes, record) => {
      val result = parser.parse(bytes, record.getMediaTypeStr, record.getUrl, record.getDate);
      // self.reply(MakeSolrDocument(record = record,
      //                             result = result));
    }
  }
}
