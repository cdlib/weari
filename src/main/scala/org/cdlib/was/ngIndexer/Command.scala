/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.net.URI;

import java.io.{File,FileInputStream};

import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

import org.cdlib.was.ngIndexer.SolrFields._;

import scala.util.matching.Regex;

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

case class ParseCommand (val uri : String,
                         val jsonpath : String) extends Command {
  val Utility.ARC_RE(arcName) = new URI(uri).getPath;

  def jsonfile = new File(jsonpath);
}

class CommandExecutor (config : Config) extends Retry {
  val httpClient = new SimpleHttpClient;
  val indexer = new SolrIndexer;

  def exec (command : Command) {
    command match {
      case cmd : ParseCommand => {
        val arcname = cmd.arcName;
        if (!cmd.jsonfile.exists) {
          catchAndLogExceptions("Top-level parser caught exception: {}") {
            httpClient.getUri(new URI(cmd.uri)) { stream =>
              indexer.arc2json(stream, arcname, cmd.jsonfile);
            }
          }
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
