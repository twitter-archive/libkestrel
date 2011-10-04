package com.twitter.libkestrel
package load

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReferenceArray
import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.util.{JavaTimer, Timer}

object PutTest {
  val description = "write as many items as we can into a queue, concurrently, and time it"

  var threadCount = Runtime.getRuntime.availableProcessors() * 2
  var itemCount = 10000
  var itemLimit = 1000
  var cycles = 10
  var oldQueue = false

  implicit val javaTimer: Timer = new JavaTimer()

  def usage() {
    Console.println("usage: qtest put [options]")
    Console.println("    %s".format(description))
    Console.println()
    Console.println("options:")
    Console.println("    -t THREADS")
    Console.println("        use THREADS writer threads (default: %d)".format(threadCount))
    Console.println("    -n ITEMS")
    Console.println("        write ITEMS items in each thread (default: %d)".format(itemCount))
    Console.println("    -L ITEMS")
    Console.println("        limit total queue size to ITEMS (default: %d)".format(itemLimit))
    Console.println("    -C CYCLES")
    Console.println("        run test CYCLES times (for jit warmup) (default: %d)".format(cycles))
    Console.println("    -Q")
    Console.println("        use old simple queue instead, for comparison")
  }

  def parseArgs(args: List[String]) {
    args match {
      case Nil =>
      case "--help" :: xs =>
        usage()
        System.exit(0)
      case "-t" :: x :: xs =>
        threadCount = x.toInt
        parseArgs(xs)
      case "-n" :: x :: xs =>
        itemCount = x.toInt
        parseArgs(xs)
      case "-L" :: x :: xs =>
        itemLimit = x.toInt
        parseArgs(xs)
      case "-C" :: x :: xs =>
        cycles = x.toInt
        parseArgs(xs)
      case "-Q" :: xs =>
        oldQueue = true
        parseArgs(xs)
      case _ =>
        usage()
        System.exit(1)
    }
  }

  def cycle() {
    val queue = if (oldQueue) {
      SimpleBlockingQueue[String](itemLimit, ConcurrentBlockingQueue.FullPolicy.DropOldest)
    } else {
      ConcurrentBlockingQueue[String](itemLimit, ConcurrentBlockingQueue.FullPolicy.DropOldest)
    }

    val startLatch = new CountDownLatch(1)
    val timings = new AtomicReferenceArray[Long](threadCount)
    val threads = (0 until threadCount).map { threadId =>
      new Thread() {
        override def run() {
          startLatch.await()
          val startTime = System.nanoTime
          (0 until itemCount).foreach { id =>
            queue.put(threadId + "/" + id)
          }
          val elapsed = System.nanoTime - startTime
          timings.set(threadId, elapsed)
        }
      }
    }.toList

    threads.foreach { _.start() }
    startLatch.countDown()
    threads.foreach { _.join() }

    val totalTime = (0 until threadCount).map { tid => timings.get(tid) }.sum
    val totalItems = threadCount * itemCount
    println("%6.2f nsec/item".format(totalTime.toDouble / totalItems))

    println("    " + queue.toDebug)

    // drain the queue and verify that items look okay and have a loose ordering.
    val itemSets = (0 until threadCount).map { i => new mutable.HashSet[Int] }.toArray
    while (queue.size > 0) {
      queue.get()().split("/").map { _.toInt }.toList match {
        case List(tid, id) =>
          itemSets(tid) += id
        case _ =>
          println("*** GIBBERISH RECEIVED")
      }
    }
    itemSets.indices.foreach { tid =>
      val list = itemSets(tid).toList.sorted
      // with a large number of threads, some starvation will occur.
      if (list.size > 0) {
        if (list.head + list.size - 1 != list.last) {
          println("*** Thread %d contains [%d,%d) size %d".format(tid, list.head, list.last + 1, list.size))
        }
        if (list.last != itemCount - 1) {
          println("*** Thread %d tail item %d is not %d".format(tid, list.last, itemCount - 1))
        }
      }
    }
  }

  def apply(args: List[String]) {
    parseArgs(args)

    println("put: writers=%d, items=%d, item_limit=%d, cycles=%d, oldq=%s".format(
      threadCount, itemCount, itemLimit, cycles, oldQueue
    ))
    (0 until cycles).foreach { n => cycle() }
  }
}
