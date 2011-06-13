package org.cdlib.was.ngIndexer.akka;

import akka.actor.Actor;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.ngIndexer.SolrDocumentModifier.makeDocument;

class DocumentCreator extends Actor {
  def receive = {
    case MakeSolrDocument (rec, parseResult) => {
      makeDocument(rec, parseResult) match {
        case None      => self.reply(MakeDocumentError);
        case Some(doc) => self.reply(IndexDocument(doc));
      }
    }
  }
}
