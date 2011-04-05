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

import sun.misc.{Signal, SignalHandler};

object Daemon {
  val httpClient = new SimpleHttpClient;
    
  /* load configuration */
  val config = SolrIndexer.loadConfigOrExit;
  
  /* for work queue */
  val zkHosts = config.zooKeeperHosts();
  val zkPath =  config.zooKeeperPath();
  val threadCount = config.threadCount();


  val handlerFactory = new QueueItemHandlerFactory {
    val indexer = new SolrIndexer(config);
    val locker = new Locker(zkHosts);

    def mkHandler = new QueueItemHandler {
      def handle (item : Item) : Boolean = {
        Command.parseCommand(new String(item.getData(), "UTF-8")) match {
          case Some(cmd : IndexCommand) => 
            httpClient.getUri(cmd.uri) {
              (stream)=>
                indexer.index(stream, cmd.arcName, cmd.specification,
                              Map(JOB_FIELD->cmd.job,
                                  TAG_FIELD->cmd.tags,
                                  SPECIFICATION_FIELD->cmd.specification, 
                                  PROJECT_FIELD->cmd.project),
                              config);
            }.getOrElse(false);
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
