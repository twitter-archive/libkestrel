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
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.atomic.AtomicLong
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}
import config._

class JournaledQueueSpec extends Spec with ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
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

  def stringToBuffer(s: String) = ByteBuffer.wrap(s.getBytes)
  def bufferToString(b: ByteBuffer): String = {
    val bytes = new Array[Byte](b.remaining)
    b.mark
    b.get(bytes)
    b.reset
    new String(bytes)
  }
  def bufferToBytes(b: ByteBuffer): List[Byte] = bufferToString(b).getBytes.toList

  def haveId(id: Long) = new Matcher[ByteBuffer]() {
    def apply(buffer: ByteBuffer) = MatchResult(
      {
        buffer.mark
        buffer.order(ByteOrder.BIG_ENDIAN)
        try {
          buffer.getLong() == id
        } finally {
          buffer.reset
        }
      },
      "data " + bufferToBytes(buffer) + " doesn't match id " + id,
      "data " + bufferToBytes(buffer) + " matches id " + id
    )
  }

  def makeId(id: Long, size: Int) = {
    val buffer = ByteBuffer.allocate(size)
    buffer.order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(id)
    buffer.rewind
    buffer
  }

  def eventually(f: => Boolean): Boolean = {
    val deadline = 5.seconds.fromNow
    while (deadline > Time.now) {
      if (f) return true
      Thread.sleep(10)
    }
    false
  }

  def setupWriteJournals(itemsPerJournal: Int, journals: Int, expiredItems: Int = 0) {
    var id = 1L
    (0 until journals).foreach { journalId =>
      val jf = JournalFile.create(new File(testFolder, "test." + journalId), null, Duration.MaxValue, 16.kilobytes)
      (0 until itemsPerJournal).foreach { n =>
        val x = makeId(id, 1024)
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

  def setupBookmarkFile(clientName: String, headId: Long, doneIds: Seq[Long] = Nil) {
    val bf = BookmarkFile.create(new File(testFolder, "test.read." + clientName))
    bf.readHead(headId)
    bf.readDone(doneIds)
    bf.close()
  }

  describe("JournaledQueue") {
    it("can be created") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = makeQueue()
        val reader = q.reader("")
        assert(new File(testFolder, "test." + Time.now.inMilliseconds).exists)
        reader.checkpoint()
        assert(new File(testFolder, "test.read.").exists)
        reader.close()
        q.close()
      }
    }

    it("disallows bad characters") {
      val q = makeQueue(config = config.copy(name = "this_is-okay"))
      intercept[Exception] { makeQueue(config = config.copy(name = "evil^queue")) }
      intercept[Exception] { makeQueue(config = config.copy(name = "evilqueue ")) }
      intercept[Exception] { makeQueue(config = config.copy(name = "evil.queue")) }
      q.close()
    }

    it("won't re-create the default reader if a named reader is created") {
      val q = makeQueue()
      val reader1 = q.reader("")
      val reader2 = q.reader("client1")
      intercept[Exception] { q.reader("") }
      q.close()
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
      q.reader("").close()
      q.close()
    }

    it("starts new readers at the end of the queue") {
      setupWriteJournals(4, 2)
      val q = makeQueue()
      val reader = q.reader("")
      assert(reader.items === 0)
      assert(reader.bytes === 0)
      assert(reader.memoryItems === 0)
      assert(reader.memoryBytes === 0)
      reader.close()
      q.close()
    }

    describe("can read existing journals") {
      it("small") {
        setupWriteJournals(4, 2)
        setupBookmarkFile("", 3L)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.items === 5)
        assert(reader.bytes === 5 * 1024)
        assert(reader.memoryItems === 5)
        assert(reader.memoryBytes === 5 * 1024)
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(4L)
        assert(item.get.id === 4L)
        reader.close()
        q.close()
      }

      it("in read-behind") {
        setupWriteJournals(4, 2)
        (0L to 4L).foreach { readId =>
          setupBookmarkFile("", readId)
          val readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes)
          val q = makeQueue(readerConfig = readerConfig)
          val reader = q.reader("")

          assert(reader.items === 8 - readId)
          assert(reader.bytes === (8 - readId) * 1024)
          assert(reader.memoryItems === 4)
          assert(reader.memoryBytes === 4 * 1024)
          reader.close()
          q.close()
        }
      }

      it("with a caught-up reader") {
        setupWriteJournals(4, 2)
        setupBookmarkFile("", 8L)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.items === 0)
        assert(reader.bytes === 0L)
        assert(reader.memoryItems === 0)
        assert(reader.memoryBytes === 0L)
        assert(reader.get(None)() == None)
        q.close()
      }
    }

    it("moves into and then out of read-behind") {
      setupWriteJournals(0, 0)
      setupBookmarkFile("", 0)
      val readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes)
      val q = makeQueue(readerConfig = readerConfig)
      val reader = q.reader("")

      assert(reader.items === 0)
      assert(reader.bytes === 0)
      assert(reader.memoryItems === 0)
      assert(reader.memoryBytes === 0)

      (1L to 50L).foreach { id =>
        q.put(makeId(id, 1024), Time.now, None)

        assert(reader.items === (id min 6))
        assert(reader.bytes === ((id min 6) * 1024))
        assert(reader.memoryItems === (id min 4))
        assert(reader.memoryBytes === ((id min 4) * 1024))

        if (id > 5) {
          val item = reader.get(None)()
          assert(item.isDefined)
          item.get.data should haveId(id - 5)
          assert(item.get.id == id - 5)
          reader.commit(item.get.id)
        }

        assert(reader.items === (id min 5))
        assert(reader.bytes === ((id min 5) * 1024))
        assert(reader.memoryItems === (id min 4))
        assert(reader.memoryBytes === ((id min 4) * 1024))
      }

      (1L to 5L).foreach { id =>
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(id + 45)
        assert(item.get.id == id + 45)
        reader.commit(item.get.id)

        assert(reader.items === 5 - id)
        assert(reader.bytes === (5 - id) * 1024)
        assert(reader.memoryItems === 5 - id)
        assert(reader.memoryBytes === (5 - id) * 1024)
      }

      q.close()
    }

    it("fills read-behind as items are removed") {
      setupWriteJournals(4, 2)
      setupBookmarkFile("", 0)
      val readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes)
      val q = makeQueue(readerConfig = readerConfig)
      val reader = q.reader("")

      assert(reader.items === 8)
      assert(reader.bytes === 8 * 1024)
      assert(reader.memoryItems === 4)
      assert(reader.memoryBytes === 4 * 1024)

      (1L to 8L).foreach { id =>
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(id)
        assert(item.get.id == id)

        assert(reader.items === 8 - id + 1)
        assert(reader.bytes === (8 - id + 1) * 1024)
        assert(reader.memoryItems === ((8 - id + 1) min 4))
        assert(reader.memoryBytes === ((8 - id + 1) min 4) * 1024)

        reader.commit(item.get.id)

        assert(reader.items === 8 - id)
        assert(reader.bytes === (8 - id) * 1024)
        assert(reader.memoryItems === ((8 - id) min 4))
        assert(reader.memoryBytes === ((8 - id) min 4) * 1024)
      }

      assert(reader.get(None)() === None)
      assert(reader.items === 0)
      assert(reader.bytes === 0)
      assert(reader.memoryItems === 0)
      assert(reader.memoryBytes === 0)
      q.close()
    }

    it("tracks get hit/miss counts") {
      setupWriteJournals(4, 1)
      setupBookmarkFile("", 3)
      val q = makeQueue()
      try {
        val reader = q.reader("")
        assert(reader.getHitCount.get === 0L)
        assert(reader.getMissCount.get === 0L)

        val item = reader.get(None)()
        assert(item.isDefined)
        assert(reader.getHitCount.get === 1L)
        assert(reader.getMissCount.get === 0L)
        reader.commit(item.get.id)

        val item2 = reader.get(None)()
        assert(! item2.isDefined)
        assert(reader.getHitCount.get === 1L)
        assert(reader.getMissCount.get === 1L)
      } finally {
        q.close()
      }
    }

    describe("open read tracking") {
      def setupTracking = {
        setupWriteJournals(4, 1)
        setupBookmarkFile("", 3)
        val q = makeQueue()
        val reader = q.reader("")
        (q, reader)
      }

      it("tracks open reads") {
        val (q, reader) = setupTracking
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(item.get.id === 4L)
        assert(reader.get(None)() === None)

        assert(reader.items === 1)
        assert(reader.bytes === 1024L)
        assert(reader.openItems === 1)
        assert(reader.openBytes === 1024L)
        assert(reader.openedItemCount.get == 1L)
        assert(reader.canceledItemCount.get == 0L)

        q.close()
      }

      it("tracks canceled reads") {
        val (q, reader) = setupTracking

        val item = reader.get(None)()
        reader.get(None)()

        reader.unget(item.get.id)
        assert(reader.items === 1)
        assert(reader.bytes === 1024L)
        assert(reader.openItems === 0)
        assert(reader.openBytes === 0L)
        assert(reader.openedItemCount.get == 1L)
        assert(reader.canceledItemCount.get == 1L)
        q.close()
      }

      it("counts opened reads since queue load") {
        val (q, reader) = setupTracking

        val item = reader.get(None)()
        reader.get(None)()
        reader.unget(item.get.id)

        val itemAgain = reader.get(None)()
        reader.commit(item.get.id)

        assert(reader.openItems === 0)
        assert(reader.openedItemCount.get == 2L)
        assert(reader.canceledItemCount.get == 1L)
        q.close()
      }
    }

    it("gives returned items to the next reader") {
      setupWriteJournals(4, 1)
      setupBookmarkFile("", 3)
      val q = makeQueue()
      val reader = q.reader("")

      val item = reader.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 4L)
      assert(reader.get(None)() === None)

      val future = reader.get(Some(Before(1.second.fromNow)))
      reader.unget(item.get.id)
      assert(future.isDefined)
      assert(future().get.id === 4L)
      q.close()
    }

    it("can commit items out of order") {
      val q = makeQueue()
      q.put(stringToBuffer("first"), Time.now, None)
      q.put(stringToBuffer("second"), Time.now, None)

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
      assert(BookmarkFile.open(new File(testFolder, "test.read.")).toList === List(
        Record.ReadHead(2L),
        Record.ReadDone(Array[Long]())
      ))
      q2.close()
    }

    it("re-delivers aborted reads") {
      val q = makeQueue()
      q.put(stringToBuffer("first"), Time.now, None)
      q.put(stringToBuffer("second"), Time.now, None)

      val reader = q.reader("")
      val item1 = reader.get(None)()
      assert(item1.isDefined)
      assert(item1.get.id === 1)
      assert(bufferToString(item1.get.data) == "first")
      reader.unget(1)

      val item2 = reader.get(None)()
      assert(item2.isDefined)
      assert(item2.get.id === 1)
      assert(bufferToString(item2.get.data) == "first")
      q.close()
    }

    it("hands off reads that keep getting aborted") {
      val readerConfig = makeReaderConfig().copy(errorHandler = { item => item.errorCount >= 2 })
      val q = makeQueue(readerConfig = readerConfig)
      q.put(stringToBuffer("doomed"), Time.now, None)

      val reader = q.reader("")
      val item1 = reader.get(None)()
      assert(item1.isDefined)
      assert(item1.get.id === 1)
      assert(bufferToString(item1.get.data) == "doomed")
      assert(item1.get.errorCount === 0)

      reader.unget(1)

      val item2 = reader.get(None)()
      assert(item2.isDefined)
      assert(item2.get.id === 1)
      assert(item2.get.errorCount === 1)

      reader.unget(1)

      val item3 = reader.get(None)()
      assert(!item3.isDefined)
      q.close()
    }

    it("can recover from stale checkpoints") {
      val q = makeQueue(config = config.copy(journalSize = 1.kilobyte))
      val reader = q.reader("")
      (1 to 5).foreach { id =>
        q.put(makeId(id, 475), Time.now, None)

        val item = reader.get(None)()
        assert(item.isDefined)
        assert(item.get.id === id)
        item.get.data should haveId(id)

        reader.commit(id)
        if (id == 1) reader.checkpoint()
      }

      // emulate a crashed server by not closing the old queue.
      MemoryMappedFile.reset()

      // all journal files will be erased except #3, so reader should resume there.
      val q2 = makeQueue(config = config.copy(journalSize = 1.kilobyte))
      val reader2 = q2.reader("")
      val item = reader2.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 3)

      q2.close()
      q.close()
    }

    it("peek") {
      setupWriteJournals(4, 1)
      setupBookmarkFile("", 0)
      val q = makeQueue()
      val reader = q.reader("")

      val item = reader.peek(None)()
      assert(item.isDefined)
      assert(item.get.id === 1L)

      val item2 = reader.get(None)()
      assert(item2.isDefined)
      assert(item2.get.id === 1L)
      q.close()
    }

    describe("old item expiration") {
      it("expires old items on get") {
        setupWriteJournals(4, 1, expiredItems = 1)
        setupBookmarkFile("", 0)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.expiredCount.get === 0)
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(item.get.id === 2L)
        assert(eventually(reader.expiredCount.get == 1))
        q.close()
      }

      it("expires old items on put") {
        setupWriteJournals(4, 1, expiredItems = 1)
        setupBookmarkFile("", 0)
        val q = makeQueue()
        val reader = q.reader("")

        assert(reader.expiredCount.get === 0)
        q.put(stringToBuffer("new item"), Time.now, None)
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(item.get.id === 2L)
        assert(eventually(reader.expiredCount.get == 1))
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
          q.put(stringToBuffer("dead!"), Time.now, Some(1.second.fromNow))

          timeMutator.advance(1.second)
          assert(reader.get(None)() === None)
          assert(received.isDefined)
          assert(bufferToString(received.get.data) === "dead!")
          q.close()
        }
      }

      it("limits the number of expirations in a single sweep") {
        Time.withCurrentTimeFrozen { timeMutator =>
          var received: List[String] = Nil
          def callback(item: QueueItem) {
            received ::= bufferToString(item.data)
          }

          val q = makeQueue(readerConfig = makeReaderConfig.copy(
            processExpiredItem = callback,
            maxExpireSweep = 3
          ))
          val reader = q.reader("")
          (1 to 10).foreach { id =>
            q.put(stringToBuffer(id.toString), Time.now, Some(100.milliseconds.fromNow))
          }

          timeMutator.advance(100.milliseconds)
          assert(reader.items === 10)
          assert(received === Nil)

          q.discardExpired()
          assert(reader.items === 7)
          assert(received === List("3", "2", "1"))
          received = Nil

          q.discardExpired()
          assert(reader.items === 4)
          assert(received === List("6", "5", "4"))
          q.close()
        }
      }

      it("ignores maxExpireSweep when expiring items on get/put") {
        Time.withCurrentTimeFrozen { timeMutator =>
          val q = makeQueue(readerConfig = makeReaderConfig.copy(
            maxExpireSweep = 3
          ))
          val reader = q.reader("")
          (1 to 10).foreach { id =>
            q.put(stringToBuffer(id.toString), Time.now, Some(100.milliseconds.fromNow))
          }

          timeMutator.advance(100.milliseconds)
          assert(reader.items === 10)

          q.put(stringToBuffer("poof"), Time.now, None)
          assert(reader.items === 1)
          q.flush()

          (1 to 10).foreach { id =>
            q.put(stringToBuffer(id.toString), Time.now, Some(100.milliseconds.fromNow))
          }

          timeMutator.advance(100.milliseconds)
          assert(reader.items === 10)

          assert(!reader.get(None)().isDefined)
          assert(reader.items === 0)

          q.close()
        }
      }

      it("defaults expiration of items with a limit") {
        setupWriteJournals(10, 1, expiredItems = 9)
        setupBookmarkFile("", 0)

        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxExpireSweep = 3
        ))
        val reader = q.reader("")

        assert(reader.expiredCount.get === 0)
        assert(q.discardExpired() === 3)
        assert(reader.expiredCount.get === 3)
        assert(reader.items === 7)
        q.close()
      }

      it("allows explicit expiration of items without limit") {
        setupWriteJournals(10, 1, expiredItems = 9)
        setupBookmarkFile("", 0)

        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxExpireSweep = 3
        ))
        val reader = q.reader("")

        assert(reader.expiredCount.get === 0)
        assert(q.discardExpired(false) === 9)
        assert(reader.expiredCount.get === 9)
        q.close()
      }

      it("stops unlimited expiration when queue is empty") {
        setupWriteJournals(10, 1, expiredItems = 10)
        setupBookmarkFile("", 0)

        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxExpireSweep = 3
        ))
        val reader = q.reader("")

        assert(reader.items == 10)
        assert(q.discardExpired(false) === 10)
        assert(reader.items == 0)
        q.close()
      }

      it("reports number of expired items across readers") {
        setupWriteJournals(10, 1, expiredItems = 9)
        setupBookmarkFile("1", 0)
        setupBookmarkFile("2", 0)
        setupBookmarkFile("3", 0)

        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxExpireSweep = 3
        ))
        val reader1 = q.reader("1")
        val reader2 = q.reader("2")
        val reader3 = q.reader("3")

        assert(reader1.items === 10)
        assert(reader2.items === 10)
        assert(reader3.items === 10)
        assert(reader1.expiredCount.get === 0)
        assert(reader2.expiredCount.get === 0)
        assert(reader3.expiredCount.get === 0)
        assert(q.discardExpired() >= 3)
        assert(reader1.expiredCount.get === 3)
        assert(reader2.expiredCount.get === 3)
        assert(reader3.expiredCount.get === 3)
        q.close()
      }

      it("honors default expiration") {
        Time.withCurrentTimeFrozen { timeMutator =>
          val q = makeQueue(readerConfig = makeReaderConfig.copy(maxAge = Some(1.second)))
          q.put(stringToBuffer("hi"), Time.now, None)

          timeMutator.advance(1.second)
          assert(q.reader("").get(None)() === None)
          q.close()
        }
      }
    }

    it("saves archived journals") {
      setupWriteJournals(4, 2)
      setupBookmarkFile("", 0)
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
      q.close()
    }

    it("tracks age") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = makeQueue()
        val reader = q.reader("")

        q.put(stringToBuffer("commie"), Time.now, None)
        q.put(stringToBuffer("spooky"), Time.now, None)
        assert(reader.age === 0.milliseconds)

        timeMutator.advance(15.milliseconds)
        assert(reader.age === 15.milliseconds)

        val item1 = reader.get(None)()
        assert(item1.isDefined)
        reader.commit(item1.get.id)
        assert(reader.age === 15.milliseconds)

        val item2 = reader.get(None)()
        assert(item2.isDefined)
        reader.commit(item2.get.id)
        assert(reader.age === 0.milliseconds)
        q.close()
      }
    }

    describe("checkpoint") {
      it("on close") {
        setupWriteJournals(4, 1)
        setupBookmarkFile("", 0)
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
        setupBookmarkFile("", 0)
        val q = makeQueue(config = config.copy(checkpointTimer = 5.milliseconds))
        val reader = q.reader("")

        assert(reader.items === 4)
        assert(BookmarkFile.open(new File(testFolder, "test.read.")).toList === List(
          Record.ReadHead(0L),
          Record.ReadDone(Array[Long]())
        ))

        val item = reader.get(None)()
        assert(item.isDefined)
        reader.commit(item.get.id)
        assert(reader.items === 3)

        Thread.sleep(100)

        assert(BookmarkFile.open(new File(testFolder, "test.read.")).toList === List(
          Record.ReadHead(1L),
          Record.ReadDone(Array[Long]())
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
        q.put(stringToBuffer("hi"), Time.now, None)
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(bufferToString(item.get.data) === "hi")
        assert(reader.putCount.get === 1)
        assert(reader.putBytes.get === 2)
        q.close()
      }

      it("discards old entries") {
        val q = makeQueue(readerConfig = makeReaderConfig().copy(
          maxItems = 1,
          fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
        ))
        val reader = q.reader("")

        q.put(stringToBuffer("hi"), Time.now, None)
        assert(reader.items === 1)

        q.put(stringToBuffer("scoot over"), Time.now, None)
        assert(eventually(reader.items == 1))

        val item = reader.get(None)()
        assert(item.isDefined)
        assert(bufferToString(item.get.data) === "scoot over")
        assert(reader.putCount.get === 2)
        assert(reader.putBytes.get === 12)
        assert(reader.discardedCount.get === 1)
        q.close()
      }

      it("enters read-behind") {
        val q = makeQueue(readerConfig = makeReaderConfig().copy(maxMemorySize = 4.kilobytes))
        val reader = q.reader("")
        (1 to 8).foreach { id =>
          q.put(makeId(id, 1024), Time.now, None)
          assert(reader.items === id)
          assert(reader.bytes === 1024L * id)
          assert(reader.memoryItems === (4 min id))
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
          assert(reader.memoryItems === (4 min (8 - id)))
          assert(reader.memoryBytes === (4096L min ((8 - id) * 1024)))
        }
        q.close()
      }

      it("refuses too-large items") {
        val q = makeQueue(config = config.copy(maxItemSize = 1.kilobyte))
        assert(q.put(ByteBuffer.allocate(1025), Time.now, None) === None)
        q.close()
      }

      it("honors maxItems") {
        val q = makeQueue(readerConfig = makeReaderConfig.copy(
          maxItems = 3,
          fullPolicy = ConcurrentBlockingQueue.FullPolicy.DropOldest
        ))
        val reader = q.reader("")
        (1 to 5).foreach { id =>
          q.put(stringToBuffer(id.toString), Time.now, None)
        }
        assert(reader.items === 3)
        val item = reader.get(None)()
        assert(item.isDefined)
        assert(bufferToString(item.get.data) === "3")
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

      q.put(stringToBuffer("first post"), Time.now, None)
      q.put(stringToBuffer("laggy"), Time.now, None)
      val item = reader.get(None)()
      assert(item.isDefined)
      assert(bufferToString(item.get.data) === "first post")
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
      setupBookmarkFile("", 0)
      val q = makeQueue()
      val reader = q.reader("")

      assert(new File(testFolder, "test.1").exists)
      assert(new File(testFolder, "test.read.").exists)
      q.erase()
      assert(!new File(testFolder, "test.1").exists)
      assert(!new File(testFolder, "test.read.").exists)
    }

    describe("flushes items") {
      it("normally") {
        val q = makeQueue(config = config.copy(journaled = false))
        val reader = q.reader("")

        (1 to 4).foreach { id =>
          q.put(ByteBuffer.allocate(1024), Time.now, None)
        }
        assert(reader.items === 4)
        assert(reader.flushCount.get === 0)
        reader.flush()
        assert(reader.items === 0)
        assert(reader.flushCount.get === 1)
        assert(!reader.get(None)().isDefined)
        q.close()
      }

      it("from read-behind") {
        val q = makeQueue(readerConfig = makeReaderConfig.copy(maxMemorySize = 1.kilobyte))
        val reader = q.reader("")

        (1 to 5).foreach { id =>
          q.put(ByteBuffer.allocate(512), Time.now, None)
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

    it("expires an empty queue") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = makeQueue(readerConfig = makeReaderConfig.copy(maxQueueAge = Some(5.seconds)))
        val reader = q.reader("")

        assert(! reader.isReadyForExpiration)

        q.put(stringToBuffer("hi"), Time.now, None)
        timeMutator.advance(6.seconds)
        assert(! reader.isReadyForExpiration)

        assert(reader.get(None)().isDefined)
        assert(reader.isReadyForExpiration)
        q.close()
      }
    }
  }
}
