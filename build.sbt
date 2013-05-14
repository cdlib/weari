organization := "org.cdlib.was"

version      := "0.1-DEV"

scalaVersion := "2.10.1"

exportJars   := true

externalPom()

resolvers ++= Seq("cdl-public" at "http://mvn.cdlib.org/content/repositories/public",
  "cdl-thirdparty" at "http://mvn.cdlib.org/content/repositories/thirdparty",
  "scala-tools.org" at "http://scala-tools.org/repo-releases",
  "archive.org" at "http://builds.archive.org:8080/maven2/",
  "maven" at "http://repo1.maven.org/maven2/",
  "repo.codahale.com" at "http://repo.codahale.com",
  "apache.org-snapshots" at "http://repository.apache.org/content/groups/snapshots/",
  "Guardian Github Releases" at "http://guardian.github.com/maven/repo-releases",
  "Local Maven Repository" at Path.userHome.asURL + "/.m2/repository")


