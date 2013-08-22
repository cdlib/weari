package org.cdlib.was.weari.pig;

import java.io.{ DataOutputStream, File, IOException, InputStream, OutputStream };

import java.net.URI;

import java.util.UUID;
import java.util.zip.{ GZIPInputStream, GZIPOutputStream };

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.{ FSDataInputStream, FSDataOutputStream, FileSystem, Path, RawLocalFileSystem };

import org.apache.pig.{ ExecType, PigServer };
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.{ PigContext };
import org.apache.pig.impl.util.PropertiesUtil;

import org.cdlib.was.weari._;
import org.cdlib.was.weari.Utility.{ extractArcname, null2option };

import scala.util.matching.Regex;

import com.typesafe.scalalogging.slf4j.Logging;

class PigUtil (config : Config) extends Logging {
  val execType = if (config.useHadoop) {
    ExecType.MAPREDUCE;
  } else {
    ExecType.LOCAL;
  }

  val conf = new Configuration();
  val fs = if (config.useHadoop) {
    FileSystem.get(conf);
  } else {
    FileSystem.get(new URI("file:///"), conf);
  }

  val jsonDir = new Path(config.jsonBaseDir);
  fs.mkdirs(jsonDir);
  val tmpDir = new Path("%s/tmp".format(config.jsonBaseDir));
  fs.mkdirs(tmpDir);

  def mkUUID : String = UUID.randomUUID().toString();

  val ARCNAME_RE = new Regex("""^(.*)\.json(.gz)?$""");

  def delete (p : Path) {
    fs.delete(p, false);
  }

  /**
   * Get the path for the JSON parse of an ARC file.
   * Can return either a .gz or a non-gzipped file.
   * 
   * @param arc The name of the ARC file.
   * @return Path
   */
  def getPath (arcName : String) : Path =
    getPathOption(arcName).getOrElse(throw new thrift.UnparsedException(arcName));
  
  def getArcname(s : String) : String = {
    s match {
      case ARCNAME_RE(arcname) => arcname;
      case o : String => o;
    }
  }

  def readJson(path : Path) : Seq[ParsedArchiveRecord] = {
    val arcname = getArcname(path.toString);
    var in : InputStream = fs.open(path);
    if (path.toString.endsWith("gz")) {
      in = new GZIPInputStream(in);
    }
    return ParsedArchiveRecordSeq.deserializeJson(in);
  }

  def writeInputStreamToTempFile (is : InputStream) : Path = {
    val name = mkUUID;
    val path = new Path(tmpDir, name);
    val os = fs.create(path);
    try {
      Utility.flushStream(is, os);
    } finally {
      is.close;
      os.close;
    }
    return path;
  }

  def writeArcList (arcs : Seq[String], os : DataOutputStream) {
    try {
      for (arcname <- arcs) {
        os.writeBytes("%s\n".format(arcname));
      }
    } finally {
      os.close;
    }
  }

  
  def mkArcList(arcs : Seq[String]) : Path = {
    val arclistName = mkUUID;
    val path = new Path(tmpDir, arclistName)
    writeArcList(arcs, fs.create(path));
    return path;
  }
  
  def mkStorePath = new Path("%s/%s.json.gz".format(tmpDir, mkUUID));

  /**
   * Get the path for the JSON parse of an ARC file.
   * Can return either a .gz or a non-gzipped file. Returns None
   * if no JSON file can be found.
   * 
   * @param arc The name of the ARC file.
   * @return Option[T]
   */
  def getPathOption (arcName : String): Option[Path] = {
    extractArcname(arcName).flatMap { extracted =>
      val jsonPath = new Path(jsonDir, "%s.json.gz".format(extracted));
      if (fs.exists(jsonPath)) Some(jsonPath) else None;
    }
  }

  /**
   * Make an empty JSON file for arcs that parsed to nothing.
   */
  def makeEmptyJson (arc : String) {
    val path = getJsonPath(arc);
    var out : OutputStream = null;
    try {
      out = fs.create(path);
      val gzout = new GZIPOutputStream(out);
      gzout.write("[]".getBytes("UTF-8"));
      gzout.flush;
      gzout.close;
    } finally {
      if (out != null) out.close;
    } 
  }

  def setupClasspath (pigServer : PigServer) {
    /* add jars in classpath to registered jars in pig */
    val cp = System.getProperty("java.class.path").split(File.pathSeparator);
    for (entry <- cp if entry.endsWith("jar")) {
      pigServer.registerJar(entry);
    }
    /* handle * classpath entries */
    for (starred <- cp if starred.endsWith("/*")) {
      val dirname = starred.substring(0, starred.length - 2);
      val dir = new File(dirname);
      for (f <- dir.list()) {
        val entry = new File(dir, f).toString;
        pigServer.registerJar(entry);
      }
    }
  }

  def mkPigServer : PigServer = {
    val properties = PropertiesUtil.loadDefaultProperties();
    properties.setProperty("pig.splitCombination", "false");
    val pigContext = new PigContext(execType, properties);
    return new PigServer(pigContext);
  }

  def parseArcs(arcs : Seq[String]) {
    val arcListPath = mkArcList(arcs.toSeq);
    val storePath = mkStorePath;

    fs.delete(storePath, false);
    try {
      val pigServer = mkPigServer;
      setupClasspath(pigServer);
      pigServer.registerQuery("""
        Data = LOAD '%s' 
        USING org.cdlib.was.weari.pig.ArchiveURLParserLoader()
        AS (filename:chararray, url:chararray, digest:chararray, date:chararray, length:long, content:chararray, detectedMediaType:chararray, suppliedM5A5A5A5AediaType:chararray, title:chararray, isRevisit, outlinks);""".
        format(arcListPath.toString));

      val job = pigServer.store("Data", storePath.toString,
        "org.cdlib.was.weari.pig.JsonParsedArchiveRecordStorer");
      
      while (!job.hasCompleted) {
        Thread.sleep(100);
      }
      if (job.getStatus == ExecJob.JOB_STATUS.FAILED) {
        throw new thrift.ParseException("");
      } else {
        refileJson(storePath);
        /* make an empty JSON file if it was empty */
        for (arc <-arcs if getPathOption(arc).isEmpty) {
          makeEmptyJson(arc);
        }
      }
    } finally {
      try {
        /* clean up temp files */
        fs.delete(arcListPath, false);
        /* being cautious here, we could just do a recursive delete on
         * storePath */
        for (children <- null2option(fs.listStatus(storePath));
             child <- children) {
          val path = child.getPath;
          if (path.getName == "_logs") { 
            fs.delete(path, true);
          } else if (path.getName.matches("""^part-m-[0-9]{5}$""")) {
            fs.delete(path, false);
          }
        }
        fs.delete(storePath, false);
      } catch {
        /* ignore errors cleaning up */
        case ex : IOException => ()
      }
    }
  }

  def getJsonPath (arcname : String) : Path = {
    val cleanArc = extractArcname(arcname).get;
    return new Path(jsonDir, "%s.json.gz".format(cleanArc));
  }

  /**
   * Moves all json.gz files beneath the source to their proper resting place.
   */
  def refileJson (source : Path) {
    for (children <- null2option(fs.listStatus(source));
         child <- children) {
      val path = child.getPath;
      if (child.isDir) {
        refileJson(path);
      } else {
        if (path.getName.endsWith(".json.gz")) {
          fs.rename(path, new Path(jsonDir, path.getName));
        } else if (path.getName == "_SUCCESS") {
          fs.delete(path, false);
        }
      }
    }
  }
}
