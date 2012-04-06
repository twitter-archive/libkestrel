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

import com.twitter.conversions.time._
import com.twitter.libkestrel.config._
import com.twitter.util.JavaTimer
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

class JournaledBlockingQueueSpec extends Spec with ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
  val config = new JournaledQueueConfig(name = "test")
  def makeReaderConfig() = new JournaledQueueReaderConfig()

  val timer = new JavaTimer(isDaemon = true)
  val scheduler = new ScheduledThreadPoolExecutor(1)

  def makeQueue(
    config: JournaledQueueConfig = config,
    readerConfig: JournaledQueueReaderConfig = makeReaderConfig()
  ) = {
    new JournaledQueue(config.copy(defaultReaderConfig = readerConfig), testFolder, timer, scheduler)
  }

  def makeCodec() = {
    new Codec[String] {
      def encode(item: String) = ByteBuffer.wrap(item.getBytes)
      def decode(data: ByteBuffer): String = {
        val bytes = new Array[Byte](data.remaining)
        data.mark
        data.get(bytes)
        data.reset
        new String(bytes)
      }
    }
  }

  describe("JournaledBlockingQueue") {
    def makeBlockingQueue(queue: JournaledQueue, codec: Codec[String] = makeCodec()) = {
      val bq = queue.toBlockingQueue(codec)

      bq.put("first")
      bq.put("second")
      bq.put("third")

      bq
    }

    it("auto-commits on get") {
      val queue = makeQueue()
      val reader = queue.reader("")
      val blockingQueue = makeBlockingQueue(queue)

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      assert(blockingQueue.get(100.seconds.fromNow)() === Some("first"))
      assert(blockingQueue.get()() === Some("second"))

      assert(reader.items === 1)
      assert(reader.openItems === 0)

      blockingQueue.close()
    }

    it("auto-commits on poll") {
      val queue = makeQueue()
      val reader = queue.reader("")
      val blockingQueue = makeBlockingQueue(queue)

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      assert(blockingQueue.poll()() === Some("first"))

      assert(reader.items === 2)
      assert(reader.openItems === 0)

      blockingQueue.close()
    }
  }

  describe("TransactionalJournaledBlockingQueue") {
    def makeTransactionalBlockingQueue(queue: JournaledQueue, codec: Codec[String] = makeCodec()) = {
      val bq = queue.toTransactionalBlockingQueue(codec)

      bq.put("first")
      bq.put("second")
      bq.put("third")

      bq
    }

    it("allows open transactions to be committed after get") {
      val queue = makeQueue()
      val reader = queue.reader("")
      val blockingQueue = makeTransactionalBlockingQueue(queue)

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      val txn1 = blockingQueue.get(100.seconds.fromNow)().get
      val txn2 = blockingQueue.get()().get

      assert(txn1.item === "first")
      assert(txn2.item === "second")

      assert(reader.items === 3)
      assert(reader.openItems === 2)

      txn1.commit()

      assert(reader.items === 2)
      assert(reader.openItems === 1)

      txn2.commit()

      assert(reader.items === 1)
      assert(reader.openItems === 0)

      blockingQueue.close()
    }

    it("allows open transactions to be rolled back after get") {
      val queue = makeQueue()
      val reader = queue.reader("")
      val blockingQueue = makeTransactionalBlockingQueue(queue)

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      val txn1 = blockingQueue.get(100.seconds.fromNow)().get
      val txn2 = blockingQueue.get()().get

      assert(txn1.item === "first")
      assert(txn2.item === "second")

      assert(reader.items === 3)
      assert(reader.openItems === 2)

      txn1.rollback()

      assert(reader.items === 3)
      assert(reader.openItems === 1)

      txn2.commit()

      assert(reader.items === 2)
      assert(reader.openItems === 0)

      val txn3 = blockingQueue.get()().get

      assert(txn3.item === "first")
      txn3.commit()

      blockingQueue.close()
    }

    it("allows open transactions to be committed after poll") {
      val queue = makeQueue()
      val reader = queue.reader("")
      val blockingQueue = makeTransactionalBlockingQueue(queue)

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      val txn = blockingQueue.poll()().get

      assert(txn.item === "first")

      assert(reader.items === 3)
      assert(reader.openItems === 1)

      txn.commit()

      assert(reader.items === 2)
      assert(reader.openItems === 0)

      blockingQueue.close()
    }

    it("allows open transactions to be rolled back after poll") {
      val queue = makeQueue()
      val reader = queue.reader("")
      val blockingQueue = makeTransactionalBlockingQueue(queue)

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      val txn1 = blockingQueue.poll()().get

      assert(txn1.item === "first")

      assert(reader.items === 3)
      assert(reader.openItems === 1)

      txn1.rollback()

      assert(reader.items === 3)
      assert(reader.openItems === 0)

      val txn2 = blockingQueue.get()().get

      assert(txn2.item === "first")
      txn2.commit()

      blockingQueue.close()
    }
  }
}
