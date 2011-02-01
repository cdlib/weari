package org.cdlib.was.ngIndexer;

import java.net.URI

import org.apache.zookeeper.recipes.queue.Item

import org.cdlib.ssconf.Configurator

import sun.misc.{Signal, SignalHandler};

object Daemon {
  val httpClient = new SimpleHttpClient;
    
  /* load configuration */
  val configPath = System.getProperty("org.cdlib.was.ngIndexer.ConfigFile");
  if (configPath == null) {
    System.err.println("Please define org.cdlib.was.ngIndexer.ConfigFile!");
    System.exit(1);
  }
  val config : Config = 
    (new Configurator).loadSimple(configPath, classOf[Config]);
  
  
  val indexer = new SolrIndexer(config);

  /* for work queue */
  val zkHosts = config.zooKeeperHosts();
  val zkPath =  config.zooKeeperPath();
  val threadCount = config.threadCount();

  /* build a queue processor */
  val queueProcessor = new QueueProcessor(zkHosts, zkPath, threadCount) {
    def handler (item : Item) {
      val cmd = new String(item.getData(), "UTF-8").split(" ");
      cmd.toList match {
        case List("INDEX", uriString, job, specification, project) => {
          val uri = new URI(uriString);
          val arcName = uri.getPath.split("/").last;
          httpClient.getUri(uri) { (stream)=>
            indexer.index(stream, arcName, job, specification, project);
          }
        }
      }
    }
  }

  object handler extends SignalHandler {
    def handle (signal : Signal) { queueProcessor.finish }
  }

  def main (args : Array[String]) {   
    Signal.handle(new Signal("TERM"), handler);    
    queueProcessor.start;
  }
}
