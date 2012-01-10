package com.twitter.libkestrel

import org.scalatest.{Reporter}
import org.scalatest.events._
import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.util.Duration

class MyReporter extends Reporter {
  case class ErrorData(classname: Option[String], name: String, throwable: Option[Throwable])

  val WIDTH = 60
  val RED = "\033[31m"
  val GREEN = "\033[32m"
  val BOLD = "\033[1m"
  val NORMAL = "\033[0m"

  var total = 0
  var count = 0
  var failed = 0
  val errors = new mutable.ListBuffer[ErrorData]

  def apply(event: Event) {
    event match {
      case RunStarting(ordinal, testCount, configMap, formatter, payload, threadName, timestamp) => {
        total = testCount
        count = 0
        failed = 0
        println()
        println("Running %d tests:".format(total))
      }
      case TestSucceeded(ordinal, suiteName, suiteClassName, testName, duration, formatter, rerunner, payload, threadName, timestamp) => {
        count += 1
        updateDisplay()
      }
      case TestFailed(ordinal, message, suiteName, suiteClassName, testName, throwable, duration, formatter, rerunner, payload, threadName, timestamp) => {
        count += 1
        failed += 1
        errors += ErrorData(suiteClassName, "%s: %s".format(suiteName, testName), throwable)
        updateDisplay()
      }
      case RunCompleted(ordinal, duration, summary, formatter, payload, threadName, timestamp) => {
        println()
        println("Finished in " + durationToHuman(duration.get.milliseconds))
        println()
        errors.foreach { error =>
          println(RED + "FAILED" + NORMAL + ": " + error.name)
          error.throwable.foreach { buildStackTrace(_, error.classname.getOrElse(""), 50).foreach(println) }
          println()
        }
      }
      case _ =>
    }
  }

  def updateDisplay() {
    val hashes = (WIDTH * count.toDouble / total).toInt
    ("#" * hashes)
    val bar = (if (failed > 0) RED else GREEN) + ("#" * hashes) + (" " * (WIDTH - hashes)) + NORMAL
    val note = if (failed > 0) "(errors: %d)".format(failed) else ""
    print("\r [%s] %d/%d %s ".format(bar, count, total, note))
  }

  def durationToHuman(x: Duration) = {
    "%d:%02d.%03d".format(x.inMinutes, x.inSeconds % 60, x.inMilliseconds % 1000)
  }

  def buildStackTrace(throwable: Throwable, highlight: String, limit: Int): List[String] = {
    var out = new mutable.ListBuffer[String]
    if (limit > 0) {
      out ++= throwable.getStackTrace.map { elem =>
        val line = "    at %s".format(elem.toString)
        if (line contains highlight) {
          BOLD + line + NORMAL
        } else {
          line
        }
      }
      if (out.length > limit) {
        out.trimEnd(out.length - limit)
        out += "    (...more...)"
      }
    }
    if ((throwable.getCause ne null) && (throwable.getCause ne throwable)) {
      out += "Caused by %s".format(throwable.getCause.toString)
      out ++= buildStackTrace(throwable.getCause, highlight, limit)
    }
    out.toList
  }
}
