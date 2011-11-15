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

import com.twitter.util.{JavaTimer, Timer}
import java.io.File
import scopt.OptionParser

trait LoadTesting {
  implicit val javaTimer: Timer = new JavaTimer()
  implicit val stringCodec: Codec[String] = new Codec[String] {
    def encode(item: String) = item.getBytes
    def decode(data: Array[Byte]) = new String(data)
  }

  sealed trait QueueType
  object QueueType {
    case object Simple extends QueueType
    case object Concurrent extends QueueType
    case object Journaled extends QueueType
  }

  var queueType: QueueType = QueueType.Concurrent
  var itemLimit = 1000

  class CommonOptionParser(name: String) extends OptionParser(name) {
    def common() {
      help(None, "help", "show this help screen")
      opt("S", "simple", "use old simple synchronized-based queue", { queueType = QueueType.Simple; () })
      opt("J", "journal", "use journaled queue in /tmp", { queueType = QueueType.Journaled; () })
      opt("L", "limit", "<items>", "limit total queue size (default: %d)".format(itemLimit), { x: String =>
        itemLimit = x.toInt
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
        new JournaledQueue(new JournaledQueueConfig(
          name = "test",
          defaultReaderConfig = new JournaledQueueReaderConfig(
            maxItems = itemLimit,
            fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
          )
        ), new File("/tmp"), javaTimer).toBlockingQueue[String]
      }
    }
  }
}
