package org.cdlib.was.ngIndexer;

import java.io.File;
import java.net.URI;
import java.util.NoSuchElementException;

import scala.collection.mutable.SynchronizedQueue;

/**
 * Class for parsing (W)ARC URIs into JSON files.
 * 
 */
class ThreadedParser (implicit config : Config)  {
  val indexer = new SolrIndexer;
  var q = new SynchronizedQueue[String];
  var finished = false;

  var threads = List[Thread]();
  
  for (f <- config.jsonCacheDir().listFiles) {
    if (f.getName.endsWith("json.tmp"))
      f.delete;
  }

  /**
   * Parse a given (W)ARC, if it is not already parsed.
   */
  def parse (arcname : String) {
    if (!isParsed(arcname) && !isParsing(arcname))
      q.enqueue(arcname);
  }
  
  /**
   * Get the JSON file for the arcname, if it is parsed, otherwise None.
   */
  def getJsonFile (arcname : String) : Option[File] = {
    val jsonfile = new File(config.jsonCacheDir(), "%s.json".format(arcname));
    if (jsonfile.exists) {
      Some(jsonfile);
    } else {
      None;
    }
  }
  
  def isParsed (arcname : String) = getJsonFile(arcname).isDefined;
  
  def isParsing (arcname : String) = q.contains(arcname);
      
  def start {
    for (n <- 1.to(config.threadCount())) {
      val thread = new Thread() {
        val executor = new CommandExecutor(config);
        override def run {
          while (!finished) {
            if (q.isEmpty) {
              Thread.sleep(1000);
            } else {
              try {
                val arcname = q.dequeue;
                val uri = config.arcServerBase().format(arcname);
                val tmpfile = new File(config.jsonCacheDir(), "%s.json.tmp".format(arcname));
                val jsonfile = new File(config.jsonCacheDir(), "%s.json".format(arcname));
                executor.exec(ParseCommand(uri=uri,
                                           jsonpath=tmpfile.getAbsolutePath));
                tmpfile.renameTo(jsonfile);
                tmpfile.delete;
              } catch {
                case ex : NoSuchElementException => { }
              }
            }
          }
        }
      }
      thread.setDaemon(true);
      thread.start;
      threads = thread :: threads;
    }
  }

  def stop {
    finished = true;
    threads.map(_.join);
  }
}
