package org.cdlib.was.ngIndexer;

import java.io.File;

import java.net.URI

import org.apache.http.conn.HttpHostConnectException;

import org.apache.zookeeper.ZooKeeper;

import org.apache.zookeeper.recipes.lock.WriteLock;

import org.apache.zookeeper.recipes.queue.Item

import org.cdlib.ssconf.Configurator

import org.cdlib.was.ngIndexer.SolrProcessor.{JOB_FIELD,
                                              PROJECT_FIELD,
                                              SPECIFICATION_FIELD};

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

    def obtainLock[T] (zooKeeper : ZooKeeper, lockName : String) (proc : T) : T = {
      var lock = new WriteLock(zooKeeper, "/arcIndexLock/%s".format(lockName), null);
      lock.lock();
      while (!lock.isOwner) { Thread.sleep(100); }
      val retval = proc;
      lock.unlock;
      return retval;
    }

    def mkHandler (zooKeeper : ZooKeeper): QueueItemHandler = {
      new QueueItemHandler {
        def handle (item : Item) : Boolean = {
          val cmd = new String(item.getData(), "UTF-8").split(" ");
          cmd.toList match {
            case List("INDEX", uriString, job, specification, project) => {
              obtainLock (zooKeeper, specification) {
                val uri = new URI(uriString);
                val ArcRE(arcName) = uriString;
                try {
                  httpClient.getUri(uri) { (stream)=>
                    System.err.println("Indexing %s".format(uri));
                    val tmpDir = new File(System.getProperty("java.io.tmpdir"));
                    val tmpFile = new File(tmpDir, arcName);
                    Utility.readStreamIntoFile(tmpFile, stream);
                    val retval = indexer.index(tmpFile, Map(JOB_FIELD->job,
                                                            SPECIFICATION_FIELD->specification, 
                                                            PROJECT_FIELD->project));
                    tmpFile.delete;
                    retval;
                  } match {
                    case None        => false;
                    case Some(false) => false;
                    case Some(true)  => true;
                  }
                } catch {
                  case ex : HttpHostConnectException =>
                    return false;
                }
              }
            }
          }
        }
      }
    }
  };

  val queueProcessor = new QueueProcessor(zkHosts, zkPath, threadCount, handlerFactory);

  object handler extends SignalHandler {
    def handle (signal : Signal) { queueProcessor.finish; }
  }

  def main (args : Array[String]) {   
    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
    queueProcessor.start;
  }
}
