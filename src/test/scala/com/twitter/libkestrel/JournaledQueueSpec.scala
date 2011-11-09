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
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class JournaledQueueSpec extends Spec with ShouldMatchers with TempFolder with TestLogging {
  val READER_CONFIG = new JournaledQueueReaderConfig()
  val CONFIG = new JournaledQueueConfig(name = "test")

  val timer = new JavaTimer(isDaemon = true)

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

  def setupWriteJournals(itemsPerJournal: Int, journals: Int) {
    var id = 1L
    (0 until journals).foreach { journalId =>
      val jf = JournalFile.createWriter(new File(testFolder, "test." + journalId), null, Duration.MaxValue)
      (0 until itemsPerJournal).foreach { n =>
        val x = new Array[Byte](1024)
        val buffer = ByteBuffer.wrap(x)
        buffer.order(ByteOrder.BIG_ENDIAN)
        buffer.putLong(id)
        jf.put(QueueItem(id, Time.now, None, x))
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
        val q = new JournaledQueue(CONFIG, testFolder, null)
        val reader = q.reader("")
        assert(new File(testFolder, "test." + Time.now.inMilliseconds).exists)
        reader.checkpoint()
        assert(new File(testFolder, "test.read.").exists)
      }
    }

    it("starts new readers at the end of the queue") {
      setupWriteJournals(4, 2)
      val q = new JournaledQueue(CONFIG, testFolder, null)
      val reader = q.reader("")
      assert(reader.items === 0)
      assert(reader.bytes === 0)
      assert(reader.memoryBytes === 0)
    }

    describe("can read existing journals") {
      it("small") {
        setupWriteJournals(4, 2)
        setupReadJournal("", 3L)
        val q = new JournaledQueue(CONFIG, testFolder, null)
        val reader = q.reader("")

        assert(reader.items === 5)
        assert(reader.bytes === 5 * 1024)
        assert(reader.memoryBytes === 5 * 1024)
        val item = reader.get(None)()
        assert(item.isDefined)
        item.get.data should haveId(4L)
        assert(item.get.id === 4L)
      }

      it("in read-behind") {
        setupWriteJournals(4, 2)
        (0L to 4L).foreach { readId =>
          setupReadJournal("", readId)
          val readerConfig = READER_CONFIG.copy(maxMemorySize = 4.kilobytes)
          val q = new JournaledQueue(CONFIG.copy(defaultReaderConfig = readerConfig), testFolder, null)
          val reader = q.reader("")

          assert(reader.items === 8 - readId)
          assert(reader.bytes === (8 - readId) * 1024)
          assert(reader.memoryBytes === 4 * 1024)
        }
      }

      it("with a caught-up reader") {
        setupWriteJournals(4, 2)
        setupReadJournal("", 8L)
        val q = new JournaledQueue(CONFIG, testFolder, null)
        val reader = q.reader("")

        assert(reader.items === 0)
        assert(reader.bytes === 0L)
        assert(reader.memoryBytes === 0L)
        assert(reader.get(None)() == None)
      }
    }

    it("fills read-behind as items are removed") {
      setupWriteJournals(4, 2)
      setupReadJournal("", 0)
      val readerConfig = READER_CONFIG.copy(maxMemorySize = 4.kilobytes)
      val q = new JournaledQueue(CONFIG.copy(defaultReaderConfig = readerConfig), testFolder, null)
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
    }

    it("tracks open reads") {
      setupWriteJournals(4, 1)
      setupReadJournal("", 3)
      val q = new JournaledQueue(CONFIG, testFolder, timer)
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
    }

    it("gives returned items to the next reader") {
      setupWriteJournals(4, 1)
      setupReadJournal("", 3)
      val q = new JournaledQueue(CONFIG, testFolder, timer)
      val reader = q.reader("")

      val item = reader.get(None)()
      assert(item.isDefined)
      assert(item.get.id === 4L)
      assert(reader.get(None)() === None)

      val future = reader.get(Some(1.second.fromNow))
      reader.unget(item.get.id)
      assert(future.isDefined)
      assert(future().get.id === 4L)
    }

    it("peek") {
      setupWriteJournals(4, 1)
      setupReadJournal("", 0)
      val q = new JournaledQueue(CONFIG, testFolder, timer)
      val reader = q.reader("")

      val item = reader.peek()()
      assert(item.isDefined)
      assert(item.get.id === 1L)

      val item2 = reader.get(None)()
      assert(item2.isDefined)
      assert(item2.get.id === 1L)
    }
  }
}

/*


class JournaledQueueSpec extends Specification with Argh with TestFolder {
  "JournaledQueue" should {

    "exist without a journal" in {
      val q = new JournaledQueue(CONFIG.copy(journaled = false), testFolder, null)

      //q.put()
      // FIXME
    }
  }
}
*/
