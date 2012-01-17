
import com.twitter.sbt._

organization := "com.twitter"

name := "libkestrel"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.9.1"

libraryDependencies += "com.twitter" %% "util-core" % "1.12.9"

libraryDependencies += "com.twitter" %% "util-logging" % "1.12.9"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.6.1" % "test"

libraryDependencies += "com.github.scopt" %% "scopt" % "1.1.2" % "test"

seq(StandardProject.newSettings: _*)


