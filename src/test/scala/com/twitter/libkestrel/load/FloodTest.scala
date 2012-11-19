/*
 * Copyright 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.libkestrel
package load

import java.util.concurrent.{CountDownLatch, ConcurrentHashMap}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLongArray, AtomicIntegerArray}
import scala.collection.JavaConverters._
import com.twitter.conversions.time._
import com.twitter.util.Time

object FloodTest extends LoadTesting {
  val description = "put & get items to/from a queue as fast as possible"

  var writerThreadCount = Runtime.getRuntime.availableProcessors()
  var readerThreadCount = Runtime.getRuntime.availableProcessors()
  var testTime = 10.seconds
  var pollPercent = 25
  var maxItems = 10000
  var minBytes = 0
  var validate = false

  val parser = new CommonOptionParser("qtest flood") {
    common()
    opt("w", "writers", "<threads>", "set number of writer threads (default: %d)".format(writerThreadCount), { x: String =>
      writerThreadCount = x.toInt
    })
    opt("r", "readers", "<threads>", "set number of reader threads (default: %d)".format(readerThreadCount), { x: String =>
      readerThreadCount = x.toInt
    })
    opt("t", "time", "<milliseconds>", "run test for specified time in milliseconds (default: %d)".format(testTime.inMilliseconds), { x: String =>
      testTime = x.toInt.milliseconds
    })
    opt("p", "percent", "<percent>", "set percentage of time to use poll instead of get (default: %d)".format(pollPercent), { x: String =>
      pollPercent = x.toInt
    })
    opt("x", "target", "<items>", "slow down the writer threads a bit when the queue reaches this size (default: %d)".format(maxItems), { x: String =>
      maxItems = x.toInt
    })
    opt("b", "bytes", "<bytes>", "size in bytes of payload (default: %d == variable, just big enough for validation)".format(minBytes), { x: String =>
      minBytes = x.toInt
    })
    opt("V", "validate", "validate items afterwards (makes it much slower)", { validate = true; () })
  }

  def apply(args: List[String]) {
    setup()
    if (!parser.parse(args)) {
      System.exit(1)
    }
    val queue = makeQueue()

    println("flood: writers=%d, readers=%d, item_limit=%d, run=%s, poll_percent=%d, max_items=%d, validate=%s, queue=%s".format(
      writerThreadCount, readerThreadCount, itemLimit, testTime, pollPercent, maxItems, validate, queue.toDebug
    ))
    maybeSleep()

    val startLatch = new CountDownLatch(1)
    val lastId = new AtomicIntegerArray(writerThreadCount)
    val writerDone = new AtomicIntegerArray(writerThreadCount)
    val deadline = testTime.fromNow

    val messageFormat = minBytes match {
      case b if b <= 3 =>  "%d/%d"
      case b if b <= 15 => "%d/%0" + (b - 2) + "d"
      case b =>            val spaces = ("%-" + (b - 15) + "s").format(" "); "%d/%010d/[" + spaces + "]"
    }

    val writers = (0 until writerThreadCount).map { threadId =>
      new Thread() {
        setName("writer-%d".format(threadId))
        override def run() {
          var id = 0
          while (deadline > Time.now) {
            queue.put(messageFormat.format(threadId, id))
            id += 1
            if (queue.size > maxItems) Thread.sleep(5)
          }
          lastId.set(threadId, id)
          writerDone.set(threadId, 1)
        }
      }
    }.toList

    val random = new XorRandom()
    val received = (0 until writerThreadCount).map { i => new ConcurrentHashMap[Int, AtomicInteger] }.toArray
    val readCounts = new AtomicIntegerArray(readerThreadCount)
    val readTimings = new AtomicLongArray(readerThreadCount)
    val readPolls = new AtomicIntegerArray(readerThreadCount)

    def writersDone(): Boolean = {
      (0 until writerThreadCount).foreach { i =>
        if (writerDone.get(i) != 1) return false
      }
      true
    }

    val readers = (0 until readerThreadCount).map { threadId =>
      new Thread() {
        setName("reader-%d".format(threadId))
        override def run() {
          startLatch.await()
          val startTime = System.nanoTime
          var count = 0
          var polls = 0
          while (deadline > Time.now || queue.size > 0 || !writersDone()) {
            val item = if (random() % 100 < pollPercent) {
              polls += 1
              queue.poll()()
            } else {
              queue.get(Before(1.millisecond.fromNow))()
            }
            if (item.isDefined) count += 1
            if (validate) {
              item.map { x =>
                x.split("/").slice(0, 2).map { _.toInt }.toList match {
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

    queue.evictWaiters()
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
          (0 until lastId.get(threadId)).foreach { id =>
            val atom = received(threadId).get(id)
            if (atom eq null) {
              print("%d(0) ".format(id))
            } else if (atom.get() != 1) {
              print("%d(%d) ".format(id, atom.get()))
            }
          }
          println()
          ok = false
        } else {
          println("writer %d wrote %d".format(threadId, lastId.get(threadId)))
        }
        received(threadId).asScala.foreach { case (id, count) =>
          if (count.get() != 1) {
            println("*** Writer %d's item %d expected 1 receive, got %d".format(
              threadId, id, count.get()
            ))
            ok = false
          }
        }
      }
      if (ok) println("All good. :)")
    } else {
      val totalRead = (0 until readerThreadCount).foldLeft(0) { (total, threadId) =>
        total + readCounts.get(threadId)
      }
      val totalWritten = (0 until writerThreadCount).foldLeft(0) { (total, threadId) =>
        total + lastId.get(threadId)
      }
      println("Writer wrote %d, readers received %d".format(totalWritten, totalRead))
      if (totalRead == totalWritten) {
        println("All good. :)")
      } else {
        println("*** counts did not match")
      }
    }

    queue.close()
  }
}
