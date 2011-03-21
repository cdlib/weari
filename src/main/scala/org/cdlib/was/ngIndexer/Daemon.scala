package org.cdlib.was.ngIndexer;


import java.io.File;

import java.net.URI

import net.liftweb.json.JsonParser;

import org.apache.http.conn.HttpHostConnectException;

import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.queue.Item;

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrProcessor.{JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD,
                                              TAG_FIELD};

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};

import scala.util.matching.Regex;

import sun.misc.{Signal, SignalHandler};

object Daemon {
  val httpClient = new SimpleHttpClient;
    
  /* load configuration */
  val config = SolrIndexer.loadConfigOrExit;
  
  /* for work queue */
  val zkHosts = config.zooKeeperHosts();
  val zkPath =  config.zooKeeperPath();
  val threadCount = config.threadCount();

  val ArcRE = new Regex(""".*?([A-Za-z0-9\.-]+arc.gz).*""");

  val handlerFactory = new QueueItemHandlerFactory {
    val indexer = new SolrIndexer(config);
    val locker = new Locker(zkHosts);

    def mkHandler = new QueueItemHandler {
      def handle (item : Item) : Boolean = {
        val cmd = JsonParser.parse(new String(item.getData(), "UTF-8"));
        // TODO - Make this typesafe
        (cmd \ "command").values.asInstanceOf[String] match {
          case "INDEX" => {
            val uriString = (cmd \ "uri").values.asInstanceOf[String];
            val uri = new URI(uriString);
            val ArcRE(arcName) = uriString;
            val job = (cmd \ "job").values.asInstanceOf[String];
            val specification = (cmd \ "specification").values.asInstanceOf[String];
            val project = (cmd \ "project").values.asInstanceOf[String];
            val tags = (cmd \ "tags").values.asInstanceOf[List[String]];
            httpClient.getUri(uri) {
              (stream)=>
                indexer.index(stream, arcName, specification,
                              Map(JOB_FIELD->job,
                                  TAG_FIELD->tags,
                                  SPECIFICATION_FIELD->specification, 
                                  PROJECT_FIELD->project));
            }.getOrElse(false);
          }
          case _ => false // Unknown command
        }
      }
    }
    def finish = { locker.finish; }
  };

  val queueProcessor = 
    new QueueProcessor(zkHosts, zkPath, threadCount, handlerFactory);

  object handler extends SignalHandler {
    def handle (signal : Signal) { 
      queueProcessor.finish;
      handlerFactory.finish;
    }
  }

  def main (args : Array[String]) {   
    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
    queueProcessor.start;
  }
}
