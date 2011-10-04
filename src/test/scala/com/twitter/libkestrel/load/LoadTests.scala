package com.twitter.libkestrel

import scala.collection.JavaConverters._
import com.twitter.conversions.time._
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap
import com.twitter.util.{JavaTimer, Timer, Time, TimeoutException}

class XorRandom {
  var seed: Int = (System.nanoTime / 1000).toInt
  def apply(): Int = {
    seed ^= (seed << 13)
    seed ^= (seed >> 17)
    seed ^= (seed << 5)
    seed & 0x7fffffff
  }
}

object Main {
  val version = "20110614"

  def usage() {
    Console.println("usage: qtest <test> [options]")
    Console.println("    run concurrency load tests against ConcurrentBlockingQueue")
    Console.println()
    Console.println("tests:")
    Console.println("    timeout")
    Console.println("        %s".format(TimeoutTest.description))
    Console.println("    put")
    Console.println("        %s".format(PutTest.description))
    Console.println("    flood")
    Console.println("        %s".format(FloodTest.description))
    Console.println()
    Console.println("use 'qtest <test> --help' to see options for a specific test")
    Console.println()
    Console.println("version %s".format(version))
  }

  def main(args: Array[String]) = {
    args.toList match {
      case "--help" :: xs =>
        usage()
      case "timeout" :: xs =>
        TimeoutTest(xs)
      case "put" :: xs =>
        PutTest(xs)
      case "flood" :: xs =>
        FloodTest(xs)
      case _ =>
        usage()
    }
    System.exit(0)
  }
}
