/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.net.URI;

import java.io.{File,FileReader};

import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

import org.cdlib.was.ngIndexer.SolrFields._;

import scala.util.matching.Regex;

import net.liftweb.json._;
import net.liftweb.json.JsonAST.JValue;

abstract class Command {
  def arcName : String;
}

case class IndexCommand (val uri : String,
                         val solrUri : String,
                         val job : String, 
                         val specification : String,
                         val institution : String,
                         val project : String,
                         val tags : List[String]) extends Command {
  val Utility.ARC_RE(arcName) = new URI(uri).getPath;
  val solrUriReal = new URI(solrUri);
}

case class ParseCommand (val uri : String) extends Command {
  val Utility.ARC_RE(arcName) = new URI(uri).getPath;
}

object Command {
  implicit val formats = DefaultFormats;

  def parse (j : JValue) : List[Command] = {
    /* there has to be a better way */
    j.children.flatMap { (cmd) => 
      val cmdname = cmd \ "command";
      if (cmdname == JString("parse")) {
        Some(cmd.extract[ParseCommand])
      } else if (cmdname == JString("index")) {
        Some(cmd.extract[IndexCommand]);
      } else None;
    }
  }

  def parse (in : String) : List[Command] = 
    parse(JsonParser.parse(in));
  
  def parse (file : File) : List[Command] =
    parse(JsonParser.parse(new FileReader(file), true));
}

class CommandExecutor (config : Config) extends Retry {
  val httpClient = new SimpleHttpClient;
  val indexer = new SolrIndexer;

  def exec (command : Command) {
    command match {
      case cmd : ParseCommand => {
        val arcname = cmd.arcName;
        httpClient.getUri(new URI(cmd.uri)) { stream =>
          val file = new File("%s.json.gz".format(arcname));
          indexer.arc2json(stream, arcname, file);
        }
      }
      case cmd : IndexCommand => {
        val server = 
          new StreamingUpdateSolrServer(cmd.solrUri,
                                        config.queueSize(),
                                        config.threadCount());
        val filter = 
          new QuickIdFilter("specification:\"%s\"".format(cmd.specification), server);
        retryLog (10) {
          httpClient.getUri(new URI(cmd.uri)) { (stream)=>
            indexer.index(stream      = stream,
                          arcName     = cmd.arcName,
                          extraId     = cmd.specification,
                          extraFields = Map(JOB_FIELD           -> cmd.job,
                                            INSTITUTION_FIELD   -> cmd.institution,
                                            TAG_FIELD           -> cmd.tags,
                                            SPECIFICATION_FIELD -> cmd.specification, 
                                            PROJECT_FIELD       -> cmd.project),
                          server      = server,
                          filter      = filter);
          }
        }
      }
      case _ => ();
    }
  }
}
