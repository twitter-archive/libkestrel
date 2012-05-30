import sbt._
import Keys._
import com.twitter.sbt._

object Libkestrel extends Build {
  val utilVersion = "5.0.3"

  lazy val root = Project(
    id = "libkestrel",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      SubversionPublisher.newSettings ++
      ScalaTestRunner.testSettings
  ).settings(
    name := "libkestrel",
    organization := "com.twitter",
    version := "1.1.1-SNAPSHOT",
    scalaVersion := "2.9.1",

    // time-based tests cannot be run in parallel
    logBuffered in Test := false,
    parallelExecution in Test := false,

    libraryDependencies ++= Seq(
      "com.twitter" % "util-core" % utilVersion,
      "com.twitter" % "util-logging" % utilVersion,

      // for tests only:
      "org.scalatest" %% "scalatest" % "1.7.1" % "test",
      "com.github.scopt" % "scopt_2.9.1" % "2.0.0" % "test",
      "com.twitter" %% "scalatest-mixins" % "1.1.0" % "test"
    ),

    SubversionPublisher.subversionRepository := Some("https://svn.twitter.biz/maven-public"),
    publishArtifact in Test := true
  )
}
