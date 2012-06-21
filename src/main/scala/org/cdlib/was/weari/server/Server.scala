/* Copyright (c) 2009-2012 The Regents of the University of California */

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
import org.cdlib.was.weari.Utility.null2option;

import java.util.HashMap;

object Server {
  def main(args : Array[String]) {
    val config = new Config();
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
