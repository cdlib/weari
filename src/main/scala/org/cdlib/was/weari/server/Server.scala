package org.cdlib.was.weari.server;

import java.io.File;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import org.cdlib.was.weari.Config;

import org.cdlib.was.weari.thrift;

import org.cdlib.ssconf.Configurator;

import java.util.HashMap;

object Server {
  def loadConfigOrExit : Config = {
    val configPath = System.getProperty("org.cdlib.was.weari.ConfigFile") match {
      case null =>
        val default = new File("indexer.conf");
        if (default.exists) { Some(default.getPath); }
        else { None; }
      case path => Some(path);
    }
    if (configPath.isEmpty) {
      System.err.println("Please define org.cdlib.was.weari.ConfigFile! or create indexer.conf file.");
      System.exit(1);
    }
    return (new Configurator).loadSimple(configPath.get, classOf[Config]);
  }

  def main(args : Array[String]) {
    val config = loadConfigOrExit;
    val handler = new WeariHandler(config);
    val processor = new thrift.Server.Processor(handler);
    
    val simpleRunnable = new Runnable() {
      def run {
        simple(processor);
      }
    };      
    
    val t = new Thread(simpleRunnable);
    t.start();
    t.join();
  }

  def simple(processor : thrift.Server.Processor[WeariHandler]) {
    val serverTransport = new TServerSocket(9090);
    val server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
    server.serve();
  }
}
