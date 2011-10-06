package org.cdlib.was.ngIndexer.scalatra;

import javax.servlet.http.HttpServlet;

import org.cdlib.was.ngIndexer._;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.{Context, ServletHolder};

object Run {
  def main(args : Array[String]) {
    implicit val config = SolrIndexer.loadConfigOrExit;
    val server = new Server(8088);
    val root = new Context(server, "/", Context.SESSIONS);
    val parser = new ThreadedParser;

    val servlet : HttpServlet = new ParserFilter(parser);
    root.addServlet(new ServletHolder(servlet), "/*");

    parser.start;
    server.start;
    server.join;
    parser.stop;
  }
}
