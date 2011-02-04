package org.cdlib.was.ngIndexer;

import java.net.URI

import org.apache.zookeeper.recipes.queue.Item

import org.cdlib.ssconf.Configurator

import org.cdlib.was.ngIndexer.SolrProcessor.{JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD};

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
    def mkHandler : QueueItemHandler = {
      val indexer = new SolrIndexer(config);
    
      new QueueItemHandler {
        def handle (item : Item) {
          val cmd = new String(item.getData(), "UTF-8").split(" ");
          System.err.println("got command: %s".format(cmd.toList.toString));
          cmd.toList match {
            case List("INDEX", uriString, job, specification, project) => {
              val uri = new URI(uriString);
              val arcName = uri.getPath.split("/").last;
              System.err.println("Fetching %s".format(uri));
              httpClient.getUri(uri) { (stream)=>
                System.err.println("Indexing %s".format(uri));
                indexer.index(stream, arcName, Map(JOB_FIELD->job,
                                                   SPECIFICATION_FIELD->specification, 
                                                   PROJECT_FIELD->project));
              }
            }
          }
        }
      }
    }
  };

  val queueProcessor = new QueueProcessor(zkHosts, zkPath, threadCount, handlerFactory);

  /* build a queue processor */

  object handler extends SignalHandler {
    def handle (signal : Signal) { queueProcessor.finish }
  }

  def main (args : Array[String]) {   
    Signal.handle(new Signal("TERM"), handler);
    queueProcessor.start;
  }
}
