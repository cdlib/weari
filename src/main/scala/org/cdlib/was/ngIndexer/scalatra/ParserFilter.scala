package org.cdlib.was.ngIndexer.scalatra;

import java.io.File;

import org.cdlib.was.ngIndexer._;

import org.scalatra._;

class ParserFilter (parser : ThreadedParser)
                   (implicit config : Config) extends ScalatraServlet {

  get("/") { 
    halt(404) 
  }

  get("/:arcname.json") {
    val arcname = params("arcname");
    parser.getJsonFile(arcname) match {
      case None => {
        parser.parse(arcname);
        halt(202);
      }
      case Some(jsonfile) =>
        halt(body    = jsonfile,
             headers = Map("Content-Encoding" -> "gzip",
                           "Content-Type"     -> "application/json"));
    }
  }
}
