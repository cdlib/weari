package org.cdlib.was.ngIndexer.akka;

import akka.actor.Actor;

class ArcIndexer extends Actor {
  def receive = {
    case IndexArc (uri) => ()
  }
}
