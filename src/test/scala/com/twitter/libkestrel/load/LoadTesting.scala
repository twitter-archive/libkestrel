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

import com.twitter.logging.{ConsoleHandler, FileHandler, Formatter, Logger, Policy}
import com.twitter.util.{JavaTimer, Timer}
import java.io.{File, FilenameFilter}
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor
import scopt.OptionParser
import config._

trait LoadTesting {
  implicit val javaTimer: Timer = new JavaTimer()
  val scheduler = new ScheduledThreadPoolExecutor(1)
  implicit val stringCodec: Codec[String] = new Codec[String] {
    def encode(item: String) = ByteBuffer.wrap(item.getBytes)
    def decode(data: ByteBuffer) = {
      val bytes = new Array[Byte](data.remaining)
      data.get(bytes)
      new String(bytes)
    }
  }

  sealed trait QueueType
  object QueueType {
    case object Simple extends QueueType
    case object Concurrent extends QueueType
    case object Journaled extends QueueType
  }

  var queueType: QueueType = QueueType.Concurrent
  var itemLimit = 10000
  var sleep = 0
  var preClean = false

  class CommonOptionParser(name: String) extends OptionParser(name) {
    def common() {
      help(None, "help", "show this help screen")
      opt("S", "simple", "use old simple synchronized-based queue", { queueType = QueueType.Simple; () })
      opt("J", "journal", "use journaled queue in /tmp", { queueType = QueueType.Journaled; () })
      opt("L", "limit", "<items>", "limit total queue size (default: %d)".format(itemLimit), { x: String =>
        itemLimit = x.toInt
      })
      opt("z", "sleep", "number of seconds to sleep before starting (for profiling) (default: %d)".format(sleep), { x: String =>
        sleep = x.toInt
      })
      opt("c", "clean", "delete any stale queue files ahead of starting the test (default off)", {
        preClean = true
      })
    }
  }

  def makeQueue(): BlockingQueue[String] = {
    queueType match {
      case QueueType.Simple => {
        SimpleBlockingQueue[String](itemLimit, ConcurrentBlockingQueue.FullPolicy.DropOldest)
      }
      case QueueType.Concurrent => {
        ConcurrentBlockingQueue[String](itemLimit, ConcurrentBlockingQueue.FullPolicy.DropOldest)
      }
      case QueueType.Journaled => {
        val dir = new File("/tmp")
        val queueName = "test"
        if (preClean) {
          dir.listFiles(new FilenameFilter {
            val prefix = queueName + "."
            def accept(dir: File, name: String) = name.startsWith(prefix)
          }).foreach { _.delete() }
        }

        new JournaledQueue(new JournaledQueueConfig(
          name = queueName,
          defaultReaderConfig = new JournaledQueueReaderConfig(
            maxItems = itemLimit,
            fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
          )
        ), dir, javaTimer, scheduler).toBlockingQueue[String]
      }
    }
  }

  def maybeSleep() {
    if (sleep > 0) {
      println("Sleeping %d seconds...".format(sleep))
      Thread.sleep(sleep * 1000)
      println("Okay.")
    }
  }

  def setup() {
    val logLevel = Logger.levelNames(Option[String](System.getenv("log")).getOrElse("FATAL").toUpperCase)
    val rootLog = Logger.get("")
    rootLog.setLevel(logLevel)
    System.getenv("logfile") match {
      case null => {
        rootLog.addHandler(new ConsoleHandler(new Formatter(), None))
      }
      case filename => {
        rootLog.addHandler(new FileHandler(filename, Policy.Never, true, 0, new Formatter(), None))
      }
    }
  }
}
