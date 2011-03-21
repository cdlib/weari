package org.cdlib.was.ngIndexer;

import java.net.URI

import net.liftweb.json.JsonParser;

import scala.util.matching.Regex;

class Command;

case class IndexCommand (val uri : URI, 
                         val job : String, 
                         val specification : String,
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
        val uriString = (cmd \ "uri").values.asInstanceOf[String];
        val job = (cmd \ "job").values.asInstanceOf[String];
        val specification = (cmd \ "specification").values.asInstanceOf[String];
        val project = (cmd \ "project").values.asInstanceOf[String];
        val tags = (cmd \ "tags").values.asInstanceOf[List[String]];
        Some(new IndexCommand (uri=new URI(uriString),
                               job=job,
                               specification=specification,
                               project=project,
                               tags=tags));
      }
      case _ => None;
    }
  }
}
