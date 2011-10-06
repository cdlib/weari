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
    val jsonfile = new File(config.jsonCacheDir(), "%s.json".format(arcname));
    if (jsonfile.exists) {
      contentType = "application/json";
      halt(headers = Map("Content-Encoding"->"gzip"),
           body    = jsonfile);
    } else {
      if (!parser.q.contains(arcname) &&
          !(new File(config.jsonCacheDir(), "%s.json.tmp".format(arcname)).exists)) {
            parser.q += arcname;
      }
      halt(202);
    }
  }
}
