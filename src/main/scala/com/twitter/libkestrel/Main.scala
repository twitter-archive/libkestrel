package com.twitter.libkestrel

import java.util.Random
import scala.collection.JavaConverters._
import com.twitter.conversions.time._
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap
import com.twitter.util.{JavaTimer, Timer, Time, TimeoutException}

/*
case class Range(respondRate: Double, replaceRate: Double) {
  val updateRate = 1.0 - respondRate - replaceRate

  def toHuman = "(resp=%.2f, repl=%.2f, update=%.2f)".format(respondRate, replaceRate, updateRate)
}
*/

object TestTimeouts {
  val description = "write items into a queue at a slow rate while a bunch of reader threads stampede"

  var writerThreadCount = 1
  var readerThreadCount = 100
  var writeRate = 10.milliseconds
  var readTimeoutLow = 5.milliseconds
  var readTimeoutHigh = 15.milliseconds
  var testTime = 10.seconds

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
      case _ =>
        usage()
        System.exit(1)
    }
  }

  def apply(args: List[String]) {
    parseArgs(args)

    println("timeout: writers=%d, readers=%d, write_rate=%s, read_timeout=(%s, %s), run=%s".format(
      writerThreadCount, readerThreadCount, writeRate, readTimeoutLow, readTimeoutHigh, testTime
    ))

    val queue = ConcurrentBlockingQueue[String]
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

object Main {
  val version = "20110613"

 /*
  var collectedStats = new mutable.HashMap[Range, mutable.Map[String, mutable.ListBuffer[Double]]]

  def runTest(name: String, factory: () => PromiseKeeper[String], range: Range) {
    val promiseReferences = new AtomicReferenceArray[PromiseKeeper[String]](promises)
    (0 until promises).foreach { i => promiseReferences.set(i, factory()) }
    val timings = new Array[Long](threads)
    val responderCounts = new Array[AtomicInteger](threads)
    val respondersSet = new Array[Long](threads)
    val usedUpPromises = new Array[List[PromiseKeeper[String]]](threads)
    (0 until threads).foreach { tid =>
      usedUpPromises(tid) = Nil
      responderCounts(tid) = new AtomicInteger(0)
    }

    val threadList = (0 until threads).map { threadId =>
      new Thread("worker-" + threadId) {
        override def run() {
          val random = new Random()
          val result = Try { "hello" }
          val startTime = System.nanoTime
          var i = 0
          while (i < rounds) {
            val promiseId = random.nextInt(promises)

            val r = random.nextFloat()
            if (r < range.respondRate) {
              respondersSet(threadId) += 1
              promiseReferences.get(promiseId).respond { _ => responderCounts(threadId).incrementAndGet() }
            } else if (r < range.respondRate + range.replaceRate) {
              val oldPromise = promiseReferences.getAndSet(promiseId, factory())
              if (correctnessCheck) {
                usedUpPromises(threadId) = oldPromise :: usedUpPromises(threadId)
              }
            } else {
              promiseReferences.get(promiseId).updateIfEmpty(result)
            }
            i += 1
          }
          timings(threadId) += System.nanoTime - startTime
        }
      }
    }.toList

    threadList.foreach { _.start() }
    threadList.foreach { _.join() }

    // flush any remaining promises
    val result = Try { "goodbye" }
    (0 until promises).foreach { i => promiseReferences.get(i).updateIfEmpty(result) }
    (0 until threads).foreach { tid =>
      usedUpPromises(tid).foreach { promise => promise.updateIfEmpty(result) }
    }

    val totalTimings = timings.sum
    val averageTiming = totalTimings.toDouble / (threads * rounds)
    println("%s: %6.1f nsec/op avg %s".format(name, averageTiming, range.toHuman))

    val stats = collectedStats.getOrElseUpdate(range, new mutable.HashMap[String, mutable.ListBuffer[Double]])
    stats.getOrElseUpdate(name, new mutable.ListBuffer[Double]) += averageTiming

    if (correctnessCheck) {
      // did they actually all match up?
      var baddies = 0
      (0 until threads).foreach { tid =>
        val count = responderCounts(tid).get()
        if (count != respondersSet(tid)) {
          baddies += 1
          println("    baddy: responders %d != executed %d".format(respondersSet(tid), count))
        }
      }
      if (baddies > 0) {
        println("    *** INCORRECT COUNTS: " + baddies)
      }
    }
  }

  def cycle(ranges: List[Range]) {
    List(
      ("current", { () => new CurrentPromise[String] }),
      ("simple", { () => new SimplePromise[String] }),
      ("robey", { () => new RobeyPromise[String] }),
      ("plus", { () => new PlusPromise[String] })
    ).foreach { case (name, factory) =>
      println
      ranges.foreach { range => runTest(name, factory, range) }
    }
  }
*/

  def usage() {
    Console.println("usage: qtest <test> [options]")
    Console.println("    run concurrency load tests against ConcurrentBlockingQueue")
    Console.println()
    Console.println("tests:")
    Console.println("    timeout")
    Console.println("        %s".format(TestTimeouts.description))
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
        TestTimeouts(xs)
      case _ =>
        usage()
    }
    System.exit(0)
  }
/*
    val ranges = List(
      Range(0.9, 0.05),
      Range(0.5, 0.2),
      Range(0.4, 0.4),
      Range(0.2, 0.2),
      Range(0.2, 0.5),
      Range(0.05, 0.9)
    )
    println("promises: threads=" + threads + " promises=" + promises + " rounds=" + rounds)

    // do them a lot, to prime the jit.
    println()
    println("... warming up the jit ...")
    (0 until jitCycles).foreach { _ => cycle(ranges) }

    // clear stats, and do them for real.
    println()
    println("... ok for real now ...")
    collectedStats.clear()
    (0 until cycles).foreach { _ => cycle(ranges) }

    println()
    println("... what happened! ...")
    println()

    // interesting stats!
    collectedStats.foreach { case (range, stats) =>
      println("%s:".format(range.toHuman))
      val averagedStats = new mutable.HashMap[String, Double]
      val minStats = new mutable.HashMap[String, Double]
      val maxStats = new mutable.HashMap[String, Double]
      stats.foreach { case (name, list) =>
        averagedStats(name) = list.sum / list.size
        minStats(name) = list.min
        maxStats(name) = list.max
      }
      val sorted = averagedStats.toList.sortBy { case (name, v) => v }
      val winner = sorted.head._2
      sorted.foreach { case (name, v) =>
        val avgMultiplier = v / winner
        val minMultiplier = minStats(name) / winner
        var maxMultiplier = maxStats(name) / winner
        println("  %s: %.1f nsec, %.2fx - %.2fx - %.2fx".format(name, v, minMultiplier, avgMultiplier, maxMultiplier))
      }
      println()
    }

    println()
    */
}
