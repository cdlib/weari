package org.cdlib.was.ngIndexer;


import java.io.File;

import java.net.URI

import org.apache.http.conn.HttpHostConnectException;

import org.apache.zookeeper.ZooKeeper;

import org.apache.zookeeper.recipes.queue.Item

import org.cdlib.ssconf.Configurator

import org.cdlib.was.ngIndexer.SolrProcessor.{JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD};

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

    def mkHandler = new QueueItemHandler {
      def handle (item : Item) : Boolean = {
        val locker = new Locker(zkHosts);
        val cmd = new String(item.getData(), "UTF-8").split(" ");
        cmd.toList match {
          case List("INDEX", uriString, job, specification, project) => {
            locker.tryToObtainLock ("/arcIndexLock/%s".format(specification)) {
              val uri = new URI(uriString);
              val ArcRE(arcName) = uriString;
              try {
                httpClient.getUri(uri) {
                  (stream)=>
                    indexer.index(stream, arcName, specification,
                                  Map(JOB_FIELD->job,
                                        SPECIFICATION_FIELD->specification, 
                                      PROJECT_FIELD->project));
                }.getOrElse(false);
              } catch {
                case ex : HttpHostConnectException =>
                  return false;
              }
            } /* else failed to obtain lock */ {
              return false;
            }
          }
        }
      }
    }
  };

  val queueProcessor = 
    new QueueProcessor(zkHosts, zkPath, threadCount, handlerFactory);

  object handler extends SignalHandler {
    def handle (signal : Signal) { 
      queueProcessor.finish;
    }
  }

  def main (args : Array[String]) {   
    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
    queueProcessor.start;
  }
}
