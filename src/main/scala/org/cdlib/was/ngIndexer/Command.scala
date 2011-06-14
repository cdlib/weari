package org.cdlib.was.ngIndexer;

import java.net.URI

import net.liftweb.json.JsonParser;

import scala.util.matching.Regex;

class Command;

case class IndexCommand (val uri : URI,
                         val solrUri : URI,
                         val job : String, 
                         val specification : String,
                         val institution : String,
                         val project : String,
                         val tags : Seq[String]) extends Command {
  val ArcRE = new Regex(""".*?([A-Za-z0-9\.-]+arc.gz).*""");
  val ArcRE(arcName) = uri.getPath;
}

object Command {
  
  def parseCommand(json : String) : Option[Command] = {
    // TODO - Make this typesafe
    val cmd = JsonParser.parse(json);
    return (cmd \ "command").values.asInstanceOf[String] match {
      case "INDEX" => {
        Some(new IndexCommand
             (uri           = new URI((cmd \ "uri").values.asInstanceOf[String]),
              solrUri       = new URI((cmd \ "solrUri").values.asInstanceOf[String]),
              job           = (cmd \ "job").values.asInstanceOf[String],
              specification = (cmd \ "specification").values.asInstanceOf[String],
              institution   = (cmd \ "institution").values.asInstanceOf[String],
              project       = (cmd \ "project").values.asInstanceOf[String],
              tags          = (cmd \ "tags").values.asInstanceOf[List[String]]));
      }
      case _ => None;
    }
  }
}
