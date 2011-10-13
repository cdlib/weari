import sbt._;
import Keys._;

object MyBuild extends Build {
  lazy val scalatraVersion = "2.0.1";

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.cdlib.was",
    version      := "0.1-DEV",
    scalaVersion := "2.9.1")

  lazy val root = Project("root",
                          file("."),
                          settings = buildSettings ++ 
                          Seq(name := "was-ng-indexer",
                              externalPom(baseDirectory(_ / "build.xml")),
                              resolvers := Seq("cdl-public" at "http://mvn.cdlib.org:18880/content/repositories/public",
                                               "cdl-thirdparty" at "http://mvn.cdlib.org:18880/content/repositories/thirdparty",
                                               "archive.org" at "http://builds.archive.org:8080/maven2/")))
}
