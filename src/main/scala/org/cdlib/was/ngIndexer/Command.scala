package org.cdlib.was.ngIndexer;

import java.net.URI;

import java.io.{File,FileReader};

import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

import org.cdlib.was.ngIndexer.SolrFields._;

import scala.util.matching.Regex;

import net.liftweb.json.{DefaultFormats,JsonParser,Serialization}
import net.liftweb.json.JsonAST.JValue;

abstract class Command;

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

object Command {
  implicit val formats = DefaultFormats;

  def parse (j : JValue) = j.extract[List[IndexCommand]];

  def parse (in : String) : List[IndexCommand] = 
    parse(JsonParser.parse(in));
  
  def parse (file : File) : List[IndexCommand] =
    parse(JsonParser.parse(new FileReader(file), true));
}

class CommandExecutor (config : Config) extends Retry {
  val httpClient = new SimpleHttpClient;
  val indexer = new SolrIndexer(config);

  def exec (command : Command) {
    command match {
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
                          filter      = filter,
                          config      = config);
                                             }
        }
      }
      case _ => ();
    }
  }
}
