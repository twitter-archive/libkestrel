import sbt._
import Keys._
import com.twitter.sbt._

object Libkestrel extends Build {
  val utilVersion = "4.0.1"

  lazy val root = Project(
    id = "libkestrel",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      SubversionPublisher.newSettings
  ).settings(
    name := "libkestrel",
    organization := "com.twitter",
    version := "1.0.4-SNAPSHOT",
    scalaVersion := "2.8.1",

    // time-based tests cannot be run in parallel
    logBuffered in Test := false,
    parallelExecution in Test := false,

    libraryDependencies ++= Seq(
      // for now, these are coming from LOCAL repo only:
      "com.twitter" % "util-core" % utilVersion,
      "com.twitter" % "util-logging" % utilVersion,

      // for tests only:
      "org.scalatest" %% "scalatest" % "1.7.1" % "test",
      "com.github.scopt" %% "scopt" % "1.1.3" % "test"
    ),

    SubversionPublisher.subversionRepository := Some("https://svn.twitter.biz/maven-public"),
    publishArtifact in Test := true
  )
}
