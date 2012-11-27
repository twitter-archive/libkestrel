import sbt._
import Keys._
import com.twitter.sbt._

object Libkestrel extends Build {
  val utilVersion = "5.3.14"

  lazy val root = Project(
    id = "libkestrel",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      SubversionPublisher.newSettings
  ).settings(
    name := "libkestrel",
    organization := "com.twitter",
    version := "1.3.0-SNAPSHOT",
    scalaVersion := "2.9.2",

    // time-based tests cannot be run in parallel
    logBuffered in Test := false,
    parallelExecution in Test := false,

    libraryDependencies ++= Seq(
      "com.twitter" % "util-core" % utilVersion,
      "com.twitter" % "util-logging" % utilVersion,

      // for tests only:
      "org.scalatest" %% "scalatest" % "1.8" % "test",
      "com.github.scopt" % "scopt_2.9.2" % "2.1.0" % "test",
      "com.twitter" % "scalatest-mixins_2.9.1" % "1.1.0" % "test"
    ),

    scalacOptions += "-deprecation",
    SubversionPublisher.subversionRepository := Some("https://svn.twitter.biz/maven-public"),
    publishArtifact in Test := true
  )
}
