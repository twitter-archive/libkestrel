package com.twitter.libkestrel

import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicIntegerArray}
import scala.collection.JavaConverters._
import com.twitter.conversions.time._
import com.twitter.util.{TimeoutException, Time, JavaTimer, Timer}

object TimeoutTest {
  val description = "write items into a queue at a slow rate while a bunch of reader threads stampede"

  var writerThreadCount = 1
  var readerThreadCount = 100
  var writeRate = 10.milliseconds
  var readTimeoutLow = 5.milliseconds
  var readTimeoutHigh = 15.milliseconds
  var testTime = 10.seconds
  var oldQueue = false

  implicit val javaTimer: Timer = new JavaTimer()

  def usage() {
    Console.println("usage: qtest timeout [options]")
    Console.println("    %s".format(description))
    Console.println()
    Console.println("options:")
    Console.println("    -w THREADS")
    Console.println("        use THREADS writer threads (default: %d)".format(writerThreadCount))
    Console.println("    -r THREADS")
    Console.println("        use THREADS reader threads (default: %d)".format(readerThreadCount))
    Console.println("    -d MILLISECONDS")
    Console.println("        delay MILLISECONDS between writes (default: %d)".format(writeRate.inMilliseconds))
    Console.println("    -L MILLISECONDS")
    Console.println("        low end of the random reader timeout (default: %d)".format(readTimeoutLow.inMilliseconds))
    Console.println("    -H MILLISECONDS")
    Console.println("        high end of the random reader timeout (default: %d)".format(readTimeoutHigh.inMilliseconds))
    Console.println("    -t MILLISECONDS")
    Console.println("        run test for MILLISECONDS (default: %d)".format(testTime.inMilliseconds))
    Console.println("    -Q")
    Console.println("        use old simple queue instead, for comparison")
  }

  def parseArgs(args: List[String]) {
    args match {
      case Nil =>
      case "--help" :: xs =>
        usage()
        System.exit(0)
      case "-w" :: x :: xs =>
        writerThreadCount = x.toInt
        parseArgs(xs)
      case "-r" :: x :: xs =>
        readerThreadCount = x.toInt
        parseArgs(xs)
      case "-d" :: x :: xs =>
        writeRate = x.toInt.milliseconds
        parseArgs(xs)
      case "-L" :: x :: xs =>
        readTimeoutLow = x.toInt.milliseconds
        parseArgs(xs)
      case "-H" :: x :: xs =>
        readTimeoutHigh = x.toInt.milliseconds
        parseArgs(xs)
      case "-t" :: x :: xs =>
        testTime = x.toInt.milliseconds
        parseArgs(xs)
      case "-Q" :: xs =>
        oldQueue = true
        parseArgs(xs)
      case _ =>
        usage()
        System.exit(1)
    }
  }

  def apply(args: List[String]) {
    parseArgs(args)

    println("timeout: writers=%d, readers=%d, write_rate=%s, read_timeout=(%s, %s), run=%s, oldq=%s".format(
      writerThreadCount, readerThreadCount, writeRate, readTimeoutLow, readTimeoutHigh, testTime, oldQueue
    ))

    val queue = if (oldQueue) {
      SimpleBlockingQueue[String]
    } else {
      ConcurrentBlockingQueue[String]
    }
    val lastId = new AtomicIntegerArray(writerThreadCount)
    val deadline = testTime.fromNow
    val readerDeadline = deadline + readTimeoutHigh * 2

    val writers = (0 until writerThreadCount).map { threadId =>
      new Thread() {
        override def run() {
          var id = 0
          while (deadline > Time.now) {
            Thread.sleep(writeRate.inMilliseconds)
            if (deadline > Time.now) {
              queue.put(threadId + "/" + id)
              id += 1
            }
          }
          lastId.set(threadId, id)
        }
      }
    }.toList

    val random = new Random()
    val range = (readTimeoutHigh.inMilliseconds - readTimeoutLow.inMilliseconds).toInt
    val received = (0 until writerThreadCount).map { i => new ConcurrentHashMap[Int, AtomicInteger] }.toArray

    val readers = (0 until readerThreadCount).map { threadId =>
      new Thread() {
        override def run() {
          while (readerDeadline > Time.now) {
            val timeout = readTimeoutHigh + random.nextInt(range + 1)
            try {
              val item = queue.get(timeout.milliseconds)()
              item.split("/").map { _.toInt }.toList match {
                case List(tid, id) =>
                  received(tid).putIfAbsent(id, new AtomicInteger)
                  received(tid).get(id).incrementAndGet()
                case _ =>
                  println("*** GIBBERISH RECEIVED")
              }
            } catch {
              case e: TimeoutException =>
            }
          }
        }
      }
    }.toList

    readers.foreach { _.start() }
    writers.foreach { _.start() }

    while (deadline > Time.now) {
      Thread.sleep(1000)
      println(queue.toDebug)
    }

    readers.foreach { _.join() }
    writers.foreach { _.join() }

    var ok = true
    (0 until writerThreadCount).foreach { threadId =>
      if (received(threadId).size != lastId.get(threadId)) {
        println("*** Mismatched count for writer %d: wrote=%d read=%d".format(
          threadId, lastId.get(threadId), received(threadId).size
        ))
        ok = false
      } else {
        println("writer %d wrote %d".format(threadId, lastId.get(threadId)))
      }
      received(threadId).asScala.foreach { case (id, count) =>
        if (count.get() != 1) {
          println("*** Writer %d item %d expected 1 receive, got %d".format(
            threadId, id, count.get()
          ))
          ok = false
        }
      }
    }
    if (ok) println("All good. :)")
  }
}
