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

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.util._
import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.atomic.AtomicLong
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}
import config._

class JournaledQueueSpec extends Spec with ShouldMatchers with TempFolder with TestLogging {
  class Counters {
    val expired = new AtomicLong(0)
    val discarded = new AtomicLong(0)
    val put = new AtomicLong(0)
  }
  val counters = new Counters()

  val config = new JournaledQueueConfig(name = "test")

  def makeReaderConfig() = {
    new JournaledQueueReaderConfig(
      incrExpiredCount = { () => counters.expired.incrementAndGet() },
      incrDiscardedCount = { () => counters.discarded.incrementAndGet() },
      incrPutCount = { () => counters.put.incrementAndGet() }
    )
  }

  val timer = new JavaTimer(isDaemon = true)

  def makeQueue(
    config: JournaledQueueConfig = config,
    readerConfig: JournaledQueueReaderConfig = makeReaderConfig()
  ) = {
    counters.expired.set(0L)
    counters.discarded.set(0L)
    counters.put.set(0L)
    new JournaledQueue(config.copy(defaultReaderConfig = readerConfig), testFolder, timer)
  }

  def haveId(id: Long) = new Matcher[Array[Byte]]() {
    def apply(data: Array[Byte]) = MatchResult(
      {
        val buffer = ByteBuffer.wrap(data)
        buffer.order(ByteOrder.BIG_ENDIAN)
        buffer.getLong() == id
      },
      "data " + data.toList + " doesn't match id " + id,
      "data " + data.toList + " matches id " + id
    )
  }

  def makeId(id: Long, size: Int) = {
    val data = new Array[Byte](size)
    val buffer = ByteBuffer.wrap(data)
    buffer.order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(id)
    data
  }

  def setupWriteJournals(itemsPerJournal: Int, journals: Int, expiredItems: Int = 0) {
    var id = 1L
    (0 until journals).foreach { journalId =>
      val jf = JournalFile.createWriter(new File(testFolder, "test." + journalId), null, Duration.MaxValue)
      (0 until itemsPerJournal).foreach { n =>
        val x = new Array[Byte](1024)
        val buffer = ByteBuffer.wrap(x)
        buffer.order(ByteOrder.BIG_ENDIAN)
        buffer.putLong(id)
        if (id <= expiredItems) {
          jf.put(QueueItem(id, 10.seconds.ago, Some(5.seconds.ago), x))
        } else {
          jf.put(QueueItem(id, Time.now, None, x))
        }
        id += 1
      }
      jf.close()
    }
  }

  def setupReadJournal(clientName: String, headId: Long, doneIds: Seq[Long] = Nil) {
    val jf = JournalFile.createReader(new File(testFolder, "test.read." + clientName), null, Duration.MaxValue)
    jf.readHead(headId)
    jf.readDone(doneIds)
    jf.close()
  }

  describe("JournaledQueue") {
    it("can be created") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = makeQueue()
        val reader = q.reader("")
        assert(new File(testFolder, "test." + Time.now.inMilliseconds).exists)
        reader.checkpoint()
        assert(new File(testFolder, "test.read.").exists)
        q.close()
      }
    }

    it("disallows bad characters") {
      val q = makeQueue(config = config.copy(name = "this_is-okay"))
      intercept[Exception] { makeQueue(config = config.copy(name = "evil^queue")) }
      intercept[Exception] { makeQueue(config = config.copy(name = "evilqueue ")) }
      intercept[Exception] { makeQueue(config = config.copy(name = "evil.queue")) }
    }

    it("won't re-create the default reader if a named reader is created") {
      val q = makeQueue()
      val reader1 = q.reader("")
      val reader2 = q.reader("client1")
      intercept[Exception] { q.reader("") }
    }

    it("can destroy a reader") {
      val q = makeQueue()
      val reader1 = q.reader("client1")
      val reader2 = q.reader("client2")
      assert(!new File(testFolder, "test.read.").exists)
      assert(new File(testFolder, "test.read.client1").exists)
      assert(new File(testFolder, "test.read.client2").exists)

      q.dropReader("client2")
      assert(!new File(testFolder, "test.read.").exists)
      assert(new File(testFolder, "test.read.client1").exists)
      assert(!new File(testFolder, "test.read.client2").exists)

      q.dropReader("client1")
      assert(!new File(testFolder, "test.read.").exists)
      assert(!new File(testFolder, "test.read.client1").exists)
      assert(!new File(testFolder, "test.read.client2").exists)

      q.reader("")
      assert(new File(testFolder, "test.read.").exists)
    }

    it("starts new readers at the end of the queue") {
      setupWriteJournals(4, 2)
      val q = makeQueue()
      val reader = q.reader("")
      assert(reader.items === 0)
      assert(reader.bytes === 0)
      assert(reader.memoryBytes === 0)
      q.close()
    }

    describe("can read existing journals") {
      it("small") {
        setupWriteJournals(4, 2)
        setupReadJournal("", 3L)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.items === 5)
        assert(reader.bytes === 5 * 1024)
        assert(reader.memoryBytes === 5 * 1024)
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(4L)
        assert(item.get.id === 4L)
        q.close()
      }

      it("in read-behind") {
        setupWriteJournals(4, 2)
        (0L to 4L).foreach { readId =>
          setupReadJournal("", readId)
          val readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes)
          val q = makeQueue(readerConfig = readerConfig)
          val reader = q.reader("")

          assert(reader.items === 8 - readId)
          assert(reader.bytes === (8 - readId) * 1024)
          assert(reader.memoryBytes === 4 * 1024)
          q.close()
        }
      }

      it("with a caught-up reader") {
        setupWriteJournals(4, 2)
        setupReadJournal("", 8L)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.items === 0)
        assert(reader.bytes === 0L)
        assert(reader.memoryBytes === 0L)
        assert(reader.get(None)() == None)
        q.close()
      }
    }

    it("fills read-behind as items are removed") {
      setupWriteJournals(4, 2)
      setupReadJournal("", 0)
      val readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes)
      val q = makeQueue(readerConfig = readerConfig)
      val reader = q.reader("")

      assert(reader.items === 8)
      assert(reader.bytes === 8 * 1024)
      assert(reader.memoryBytes === 4 * 1024)

      (1L to 8L).foreach { id =>
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(id)
        assert(item.get.id == id)

        assert(reader.items === 8 - id + 1)
        assert(reader.bytes === (8 - id + 1) * 1024)
        assert(reader.memoryBytes === ((8 - id + 1) min 4) * 1024)

        reader.commit(item.get.id)

        assert(reader.items === 8 - id)
        assert(reader.bytes === (8 - id) * 1024)
        assert(reader.memoryBytes === ((8 - id) min 4) * 1024)
      }

      assert(reader.get(None)() === None)
      assert(reader.items === 0)
      assert(reader.bytes === 0)
      assert(reader.memoryBytes === 0)
      q.close()
    }

    it("tracks open reads") {
      setupWriteJournals(4, 1)
      setupReadJournal("", 3)
      val q = makeQueue()
      val reader = q.reader("")

      val item = reader.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 4L)
      assert(reader.get(None)() === None)

      assert(reader.items === 1)
      assert(reader.bytes === 1024L)
      assert(reader.openItems === 1)
      assert(reader.openBytes === 1024L)

      reader.unget(item.get.id)
      assert(reader.items === 1)
      assert(reader.bytes === 1024L)
      assert(reader.openItems === 0)
      assert(reader.openBytes === 0L)
      q.close()
    }

    it("gives returned items to the next reader") {
      setupWriteJournals(4, 1)
      setupReadJournal("", 3)
      val q = makeQueue()
      val reader = q.reader("")

      val item = reader.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 4L)
      assert(reader.get(None)() === None)

      val future = reader.get(Some(1.second.fromNow))
      reader.unget(item.get.id)
      assert(future.isDefined)
      assert(future().get.id === 4L)
      q.close()
    }

    it("can commit items out of order") {
      val q = makeQueue()
      q.put("first".getBytes, Time.now, None)
      q.put("second".getBytes, Time.now, None)

      val reader = q.reader("")
      val item1 = reader.get(None)()
      val item2 = reader.get(None)()

      assert(item1.isDefined)
      assert(item1.get.id === 1)
      assert(item2.isDefined)
      assert(item2.get.id === 2)

      reader.commit(2)
      reader.checkpoint()
      reader.commit(1)
      reader.checkpoint()

      assert(reader.items === 0)
      assert(reader.bytes === 0)
      q.close()

      val q2 = makeQueue()
      assert(q2.reader("").items === 0)
      assert(JournalFile.openReader(new File(testFolder, "test.read."), null, Duration.MaxValue).toList === List(
        JournalFile.Record.ReadHead(2L),
        JournalFile.Record.ReadDone(Array[Long]())
      ))
    }

    it("can recover from stale checkpoints") {
      val q = makeQueue(config = config.copy(journalSize = 1.kilobyte))
      val reader = q.reader("")
      (1 to 5).foreach { id =>
        q.put(makeId(id, 512), Time.now, None)

        val item = reader.get(None)()
        assert(item.isDefined)
        assert(item.get.id === id)
        item.get.data should haveId(id)

        reader.commit(id)
        if (id == 1) reader.checkpoint()
      }

      // emulate a crashed server by not closing the old queue.
      // all journal files will be erased except #3, so reader should resume there.
      val q2 = makeQueue(config = config.copy(journalSize = 1.kilobyte))
      val reader2 = q2.reader("")
      val item = reader2.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 3)
    }

    it("peek") {
      setupWriteJournals(4, 1)
      setupReadJournal("", 0)
      val q = makeQueue()
      val reader = q.reader("")

      val item = reader.peek()()
      assert(item.isDefined)
      assert(item.get.id === 1L)

      val item2 = reader.get(None)()
      assert(item2.isDefined)
      assert(item2.get.id === 1L)
      q.close()
    }

    it("expires old items") {
      setupWriteJournals(4, 1, expiredItems = 1)
      setupReadJournal("", 0)
      val q = makeQueue()
      val reader = q.reader("")

      assert(reader.expired === 0)
      val item = reader.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 2L)
      assert(reader.expired === 1)
      assert(counters.expired.get === 1)
      q.close()
    }

    it("sends expired items to the callback") {
      Time.withCurrentTimeFrozen { timeMutator =>
        var received: Option[QueueItem] = None
        def callback(item: QueueItem) {
          received = Some(item)
        }

        val q = makeQueue(readerConfig = makeReaderConfig.copy(processExpiredItem = callback))
        val reader = q.reader("")
        q.put("dead!".getBytes, Time.now, Some(1.second.fromNow))

        timeMutator.advance(1.second)
        assert(reader.get(None)() === None)
        assert(received.isDefined)
        assert(new String(received.get.data) === "dead!")
      }
    }

    it("limits the number of expirations in a single sweep") {
      Time.withCurrentTimeFrozen { timeMutator =>
        var received: List[String] = Nil
        def callback(item: QueueItem) {
          received ::= new String(item.data)
        }

        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          processExpiredItem = callback,
          maxExpireSweep = 3
        ))
        val reader = q.reader("")
        (1 to 10).foreach { id =>
          q.put(id.toString.getBytes, Time.now, Some(100.milliseconds.fromNow))
        }

        timeMutator.advance(100.milliseconds)
        assert(reader.items === 10)
        assert(received === Nil)

        q.put("poof".getBytes, Time.now, None)
        assert(reader.items === 8)
        assert(received === List("3", "2", "1"))
        received = Nil

        q.put("poof".getBytes, Time.now, None)
        assert(reader.items === 6)
        assert(received === List("6", "5", "4"))
        q.close()
      }
    }

    it("honors default expiration") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = makeQueue(readerConfig = makeReaderConfig.copy(maxAge = Some(1.second)))
        q.put("hi".getBytes, Time.now, None)

        timeMutator.advance(1.second)
        assert(q.reader("").get(None)() === None)
      }
    }

    it("saves archived journals") {
      setupWriteJournals(4, 2)
      setupReadJournal("", 0)
      val q = makeQueue(config = config.copy(saveArchivedJournals = Some(testFolder)))
      val reader = q.reader("")

      (0 until 8).foreach { id =>
        val item = reader.get(None)()
        assert(item.isDefined)
        reader.commit(item.get.id)
      }
      q.checkpoint()
      assert(new File(testFolder, "archive~test.0").exists)
      assert(!new File(testFolder, "test.0").exists)
      assert(new File(testFolder, "test.1").exists)
    }

    it("tracks age") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = makeQueue()
        val reader = q.reader("")

        q.put("commie".getBytes, Time.now, None)
        q.put("spooky".getBytes, Time.now, None)
        timeMutator.advance(15.milliseconds)
        assert(reader.age === 0.milliseconds)
        val item1 = reader.get(None)()
        assert(item1.isDefined)
        reader.commit(item1.get.id)
        assert(reader.age === 15.milliseconds)
        val item2 = reader.get(None)()
        assert(item2.isDefined)
        reader.commit(item2.get.id)
        assert(reader.age === 0.milliseconds)
      }
    }

    describe("checkpoint") {
      it("on close") {
        setupWriteJournals(4, 1)
        setupReadJournal("", 0)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.items === 4)
        val item = reader.get(None)()
        assert(item.isDefined)
        reader.commit(item.get.id)
        assert(reader.items === 3)
        q.close()

        val q2 = makeQueue()
        val reader2 = q.reader("")
        assert(reader2.items === 3)
        q2.close()
      }

      it("on timer") {
        setupWriteJournals(4, 1)
        setupReadJournal("", 0)
        val q = makeQueue(config = config.copy(checkpointTimer = 5.milliseconds))
        val reader = q.reader("")

        assert(reader.items === 4)
        assert(JournalFile.openReader(new File(testFolder, "test.read."), null, Duration.MaxValue).toList === List(
          JournalFile.Record.ReadHead(0L),
          JournalFile.Record.ReadDone(Array[Long]())
        ))

        val item = reader.get(None)()
        assert(item.isDefined)
        reader.commit(item.get.id)
        assert(reader.items === 3)

        Thread.sleep(100)

        assert(JournalFile.openReader(new File(testFolder, "test.read."), null, Duration.MaxValue).toList === List(
          JournalFile.Record.ReadHead(1L),
          JournalFile.Record.ReadDone(Array[Long]())
        ))
        q.close()
      }
    }

    it("can configure two readers differently") {
      val q = makeQueue(config = config.copy(
        readerConfigs = Map("small" -> new JournaledQueueReaderConfig(maxMemorySize = 1.kilobyte))
      ))
      val reader1 = q.reader("big")
      val reader2 = q.reader("small")

      (1 to 5).foreach { id =>
        q.put(makeId(id, 512), Time.now, None)
      }
      assert(!reader1.inReadBehind)
      assert(reader2.inReadBehind)
      q.close()
    }

    describe("put") {
      it("can put and get") {
        val q = makeQueue()
        val reader = q.reader("")
        q.put("hi".getBytes, Time.now, None)
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(new String(item.get.data) === "hi")
        assert(counters.put.get === 1)
        q.close()
      }

      it("discards old entries") {
        val q = makeQueue(readerConfig = makeReaderConfig().copy(
          maxItems = 1,
          fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
        ))
        val reader = q.reader("")

        q.put("hi".getBytes, Time.now, None)
        assert(reader.items === 1)

        q.put("scoot over".getBytes, Time.now, None)
        assert(reader.items === 1)

        val item = reader.get(None)()
        assert(item.isDefined)
        assert(new String(item.get.data) === "scoot over")
        assert(counters.put.get === 2)
        assert(counters.discarded.get === 1)
        assert(reader.discarded === 1)
        q.close()
      }

      it("enters read-behind") {
        val q = makeQueue(readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes))
        val reader = q.reader("")
        (1 to 8).foreach { id =>
          q.put(makeId(id, 1024), Time.now, None)
          assert(reader.items === id)
          assert(reader.bytes === 1024L * id)
          assert(reader.memoryBytes === (4096L min (id * 1024)))
        }
        (1 to 8).foreach { id =>
          val item = reader.get(None)()
          assert(item.isDefined)
          item.get.data should haveId(id)
          assert(item.get.id === id)
          reader.commit(item.get.id)
          assert(reader.items === (8 - id))
          assert(reader.bytes === 1024L * (8 - id))
          assert(reader.memoryBytes === (4096L min ((8 - id) * 1024)))
        }
        q.close()
      }

      it("refuses too-large items") {
        val q = makeQueue(config = config.copy(maxItemSize = 1.kilobyte))
        assert(q.put(new Array[Byte](1025), Time.now, None) === None)
      }

      it("honors maxItems") {
        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxItems = 3,
          fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
        ))
        val reader = q.reader("")
        (1 to 5).foreach { id =>
          q.put(id.toString.getBytes, Time.now, None)
        }
        assert(reader.items === 3)
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(new String(item.get.data) === "3")
        q.close()
      }

      it("honors maxSize") {
        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxSize = 3.kilobytes,
          fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
        ))
        val reader = q.reader("")
        (1 to 5).foreach { id =>
          q.put(makeId(id, 1024), Time.now, None)
        }
        assert(reader.items === 3)
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(3)
        q.close()
      }
    }

    it("exists without a journal") {
      val q = makeQueue(config = config.copy(journaled = false))
      val reader = q.reader("")

      q.put("first post".getBytes, Time.now, None)
      q.put("laggy".getBytes, Time.now, None)
      val item = reader.get(None)()
      assert(item.isDefined)
      assert(new String(item.get.data) === "first post")
      q.close()

      assert(!new File(testFolder, "test.read.").exists)
      assert(testFolder.list().size === 0)

      val q2 = makeQueue(config = config.copy(journaled = false))
      val reader2 = q.reader("")
      val item2 = reader.get(None)()
      assert(!item2.isDefined)
      q2.close()
    }

    it("erases journals when asked") {
      setupWriteJournals(4, 3)
      setupReadJournal("", 0)
      val q = makeQueue()
      val reader = q.reader("")

      assert(new File(testFolder, "test.1").exists)
      assert(new File(testFolder, "test.read.").exists)
      q.erase()
      assert(!new File(testFolder, "test.1").exists)
      assert(!new File(testFolder, "test.read.").exists)
      q.close()
    }

    describe("flushes items") {
      it("normally") {
        val q = makeQueue(config = config.copy(journaled = false))
        val reader = q.reader("")

        (1 to 4).foreach { id =>
          q.put(new Array[Byte](1024), Time.now, None)
        }
        assert(reader.items === 4)
        reader.flush()
        assert(reader.items === 0)
        assert(!reader.get(None)().isDefined)
        q.close()
      }

      it("from read-behind") {
        val q = makeQueue(readerConfig = makeReaderConfig.copy(maxMemorySize = 1.kilobyte))
        val reader = q.reader("")

        (1 to 5).foreach { id =>
          q.put(new Array[Byte](512), Time.now, None)
        }
        assert(reader.items === 5)
        assert(reader.inReadBehind)
        reader.flush()
        assert(reader.items === 0)
        assert(!reader.inReadBehind)
        assert(!reader.get(None)().isDefined)
        q.close()
      }
    }
  }
}
