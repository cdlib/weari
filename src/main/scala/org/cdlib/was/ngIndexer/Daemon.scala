/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io.File;

import java.net.URI

import net.liftweb.json.JsonParser;

import org.apache.http.conn.HttpHostConnectException;

import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.queue.Item;

import org.cdlib.ssconf.Configurator;

import org.cdlib.was.ngIndexer.SolrFields.{JOB_FIELD,
                                           INSTITUTION_FIELD,
                                           PROJECT_FIELD,
                                           SPECIFICATION_FIELD,
                                           TAG_FIELD};

import org.menagerie.{DefaultZkSessionManager,ZkSessionManager};

import sun.misc.{Signal, SignalHandler};

object Daemon {    
  /* load configuration */
  val config = SolrIndexer.loadConfigOrExit;
  
  /* for work queue */
  val zkHosts = config.zooKeeperHosts();
  val zkPath =  config.zooKeeperPath();
  val threadCount = config.threadCount();

  val handlerFactory = new QueueItemHandlerFactory {
    val executor = new CommandExecutor(config);

    def mkHandler = new QueueItemHandler {
      def handle (item : Item) : Boolean = {
        val cmds = Command.parse(new String(item.getData(), "UTF-8"));
        cmds.map(executor.exec(_))
        return true;
      }
    }
    def finish = { /*locker.finish;*/ }
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
