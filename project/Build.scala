import com.github.retronym.SbtOneJar;
import sbt._;
import Keys._;

object MyBuild extends Build {
  lazy val scalatraVersion = "2.0.1";

  lazy val distFiles = TaskKey[Seq[(File, String)]]("dist-files", "Files to include in the distribution zip")
  lazy val distPath = SettingKey[File]("dist-path", "Path to generate the distribution zip to")
  lazy val dist = TaskKey[File]("dist", "Generate distribution zip file")

  val distTask = dist <<= (distPath in Runtime, fullClasspath in Runtime) map { (dest, cp) =>
    val jars = for (attributedjar <- cp;
                    val jar = attributedjar.data)
      yield (jar, "lib/%s".format(jar.getName))
    IO.zip(jars, dest);
    dest;
  }

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.cdlib.was",
    version      := "0.1-DEV",
    scalaVersion := "2.9.1",
    exportJars   := true)
                
  lazy val root = Project("root",
                          file("."),
                          settings = buildSettings ++ Seq(distTask) ++ SbtOneJar.oneJarSettings ++ // ++ sbtassembly.Plugin.assemblySettings ++
                            Seq(name := "was-ng-indexer",
                                distFiles := Seq(),
                                distPath <<= (target) { (target) => target / "dist" / "artifacts.zip" },
                                exportJars := true,
                                externalPom(baseDirectory(_ / "pom.xml")),
                                libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "1.6.1" % "test",
                                                            "junit" % "junit" % "4.8.2" % "test"),
                                resolvers := Seq("cdl-public" at "http://mvn.cdlib.org/content/repositories/public",
                                                 "cdl-thirdparty" at "http://mvn.cdlib.org/content/repositories/thirdparty",
                                                 "archive.org" at "http://builds.archive.org:8080/maven2/")));
}
