package com.twitter.libkestrel

import java.util.Random
import scala.collection.JavaConverters._
import com.twitter.conversions.time._
import com.twitter.util.{TimeoutException, Time, JavaTimer, Timer}
import java.util.concurrent.{CountDownLatch, ConcurrentHashMap}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLongArray, AtomicIntegerArray}

object FloodTest {
  val description = "put & get items to/from a queue as fast as possible"

  var writerThreadCount = Runtime.getRuntime.availableProcessors()
  var readerThreadCount = Runtime.getRuntime.availableProcessors()
  var testTime = 10.seconds
  var pollPercent = 25
  var maxItems = 10000
  var validate = false

  implicit val javaTimer: Timer = new JavaTimer()

  def usage() {
    Console.println("usage: qtest flood [options]")
    Console.println("    %s".format(description))
    Console.println()
    Console.println("options:")
    Console.println("    -w THREADS")
    Console.println("        use THREADS writer threads (default: %d)".format(writerThreadCount))
    Console.println("    -r THREADS")
    Console.println("        use THREADS reader threads (default: %d)".format(readerThreadCount))
    Console.println("    -t MILLISECONDS")
    Console.println("        run test for MILLISECONDS (default: %d)".format(testTime.inMilliseconds))
    Console.println("    -p PERCENT")
    Console.println("        use poll instead of get PERCENT of the time (default: %d)".format(pollPercent))
    Console.println("    -x ITEMS")
    Console.println("        slow down the writer threads a bit when the queue reaches ITEMS items (default: %d)".format(maxItems))
    Console.println("    -V")
    Console.println("        validate items afterwards (makes it much slower)")
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
      case "-t" :: x :: xs =>
        testTime = x.toInt.milliseconds
        parseArgs(xs)
      case "-p" :: x :: xs =>
        pollPercent = x.toInt
        parseArgs(xs)
      case "-x" :: x :: xs =>
        maxItems = x.toInt
        parseArgs(xs)
      case "-V" :: xs =>
        validate = true
        parseArgs(xs)
      case _ =>
        usage()
        System.exit(1)
    }
  }

  def apply(args: List[String]) {
    parseArgs(args)

    println("flood: writers=%d, readers=%d, run=%s, poll_percent=%d, max_items=%d, validate=%s".format(
      writerThreadCount, readerThreadCount, testTime, pollPercent, maxItems, validate
    ))

    val queue = ConcurrentBlockingQueue[String]
    val startLatch = new CountDownLatch(1)
    val lastId = new AtomicIntegerArray(writerThreadCount)
    val deadline = testTime.fromNow

    val writers = (0 until writerThreadCount).map { threadId =>
      new Thread() {
        override def run() {
          var id = 0
          while (deadline > Time.now) {
            queue.put(threadId + "/" + id)
            id += 1
            if (queue.size > maxItems) Thread.sleep(5)
          }
          lastId.set(threadId, id)
        }
      }
    }.toList

    val random = new Random()
    val received = (0 until writerThreadCount).map { i => new ConcurrentHashMap[Int, AtomicInteger] }.toArray
    val readCounts = new AtomicIntegerArray(readerThreadCount)
    val readTimings = new AtomicLongArray(readerThreadCount)
    val readPolls = new AtomicIntegerArray(readerThreadCount)

    val readers = (0 until readerThreadCount).map { threadId =>
      new Thread() {
        override def run() {
          startLatch.await()
          val startTime = System.nanoTime
          var count = 0
          var polls = 0
          while (deadline > Time.now || queue.size > 0) {
            val item = if (random.nextInt(100) < pollPercent) {
              polls += 1
              queue.poll()
            } else {
              try {
                Some(queue.get(1.millisecond)())
              } catch {
                case e: TimeoutException => None
              }
            }
            if (item.isDefined) count += 1
            if (validate) {
              item.map { x =>
                x.split("/").map { _.toInt }.toList match {
                  case List(tid, id) =>
                    received(tid).putIfAbsent(id, new AtomicInteger)
                    received(tid).get(id).incrementAndGet()
                  case _ =>
                    println("*** GIBBERISH RECEIVED")
                }
              }
            }
          }
          val timing = System.nanoTime - startTime
          readCounts.set(threadId, count)
          readTimings.set(threadId, timing)
          readPolls.set(threadId, polls)
        }
      }
    }.toList

    writers.foreach { _.start() }
    readers.foreach { _.start() }
    startLatch.countDown()

    while (deadline > Time.now) {
      Thread.sleep(1000)
      println(queue.toDebug)
    }

    readers.foreach { _.join() }
    writers.foreach { _.join() }

    (0 until readerThreadCount).foreach { threadId =>
      val t = readTimings.get(threadId).toDouble / readCounts.get(threadId)
      val pollPercent = readPolls.get(threadId).toDouble * 100 / readCounts.get(threadId)
      println("%3d: %5.0f nsec/read (%3.0f%% polls)".format(threadId, t, pollPercent))
    }

    if (validate) {
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
}
