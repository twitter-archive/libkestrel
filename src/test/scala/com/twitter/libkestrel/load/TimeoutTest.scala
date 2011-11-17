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

import com.twitter.conversions.time._
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicIntegerArray}
import scala.collection.JavaConverters._

object TimeoutTest extends LoadTesting {
  val description = "write items into a queue at a slow rate while a bunch of reader threads stampede"

  var writerThreadCount = 1
  var readerThreadCount = 100
  var writeRate = 10.milliseconds
  var readTimeoutLow = 5.milliseconds
  var readTimeoutHigh = 15.milliseconds
  var testTime = 10.seconds

  val parser = new CommonOptionParser("qtest timeout") {
    common()
    opt("w", "writers", "<threads>", "set number of writer threads (default: %d)".format(writerThreadCount), { x: String =>
      writerThreadCount = x.toInt
    })
    opt("r", "readers", "<threads>", "set number of reader threads (default: %d)".format(readerThreadCount), { x: String =>
      readerThreadCount = x.toInt
    })
    opt("d", "delay", "<milliseconds>", "delay between writes (default: %d)".format(writeRate.inMilliseconds), { x: String =>
      writeRate = x.toInt.milliseconds
    })
    opt("L", "low", "<milliseconds>", "low end of the random reader timeout (default: %d)".format(readTimeoutLow.inMilliseconds), { x: String =>
      readTimeoutLow = x.toInt.milliseconds
    })
    opt("H", "high", "<milliseconds>", "high end of the random reader timeout (default: %d)".format(readTimeoutHigh.inMilliseconds), { x: String =>
      readTimeoutHigh = x.toInt.milliseconds
    })
    opt("t", "timeout", "<milliseconds>", "run test for this long (default: %d)".format(testTime.inMilliseconds), { x: String =>
      testTime = x.toInt.milliseconds
    })
  }

  def apply(args: List[String]) {
    setup()
    if (!parser.parse(args)) {
      System.exit(1)
    }
    val queue = makeQueue()

    println("timeout: writers=%d, readers=%d, item_limit=%d, write_rate=%s, read_timeout=(%s, %s), run=%s, queue=%s".format(
      writerThreadCount, readerThreadCount, itemLimit, writeRate, readTimeoutLow, readTimeoutHigh, testTime, queue.toDebug
    ))

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

    val random = new XorRandom()
    val range = (readTimeoutHigh.inMilliseconds - readTimeoutLow.inMilliseconds).toInt
    val received = (0 until writerThreadCount).map { i => new ConcurrentHashMap[Int, AtomicInteger] }.toArray

    val readers = (0 until readerThreadCount).map { threadId =>
      new Thread() {
        override def run() {
          while (readerDeadline > Time.now) {
            val timeout = readTimeoutHigh + random() % (range + 1)
            val optItem = queue.get(timeout.milliseconds.fromNow)()
            optItem match {
              case None =>
              case Some(item) => {
                item.split("/").map { _.toInt }.toList match {
                  case List(tid, id) =>
                    received(tid).putIfAbsent(id, new AtomicInteger)
                    received(tid).get(id).incrementAndGet()
                  case _ =>
                    println("*** GIBBERISH RECEIVED")
                }
              }
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
