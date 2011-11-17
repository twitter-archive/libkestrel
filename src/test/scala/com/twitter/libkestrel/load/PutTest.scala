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

import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReferenceArray
import scala.collection.mutable
import com.twitter.conversions.time._

object PutTest extends LoadTesting {
  val description = "write as many items as we can into a queue, concurrently, and time it"

  var threadCount = Runtime.getRuntime.availableProcessors() * 2
  var itemCount = 10000
  var cycles = 10

  val parser = new CommonOptionParser("qtest put") {
    common()
    opt("t", "threads", "<threads>", "set number of writer threads (default: %d)".format(threadCount), { x: String =>
      threadCount = x.toInt
    })
    opt("n", "items", "<items>", "set number of items to write in each thread (default: %d)".format(itemCount), { x: String =>
      itemCount = x.toInt
    })
    opt("C", "cycles", "<cycles>", "set number of test runs (for jit warmup) (default: %d)".format(cycles), { x: String =>
      cycles = x.toInt
    })
  }

  def cycle(queue: BlockingQueue[String]) {
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
      queue.get()().get.split("/").map { _.toInt }.toList match {
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
    setup()
    if (!parser.parse(args)) {
      System.exit(1)
    }
    val queue = makeQueue()

    println("put: writers=%d, items=%d, item_limit=%d, cycles=%d, queue=%s".format(
      threadCount, itemCount, itemLimit, cycles, queue.toDebug
    ))
    (0 until cycles).foreach { n => cycle(queue) }
    queue.close()
  }
}
