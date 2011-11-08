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

trait TempFolder extends AbstractSuite { self: Suite =>
  import com.twitter.io.Files

  var testFolder: File = _

  abstract override def withFixture(test: NoArgTest) {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null
    do {
      folder = new File(tempFolder, "scala-test-" + System.currentTimeMillis)
    } while (! folder.mkdir)
    testFolder = folder
    try {
      super.withFixture(test)
    } finally {
      Files.delete(testFolder)
    }
  }
}

trait TestLogging2 extends AbstractSuite { self: Suite =>
  import com.twitter.logging._
  import java.util.{logging => jlogging}

  val logLevel = Logger.levelNames(Option[String](System.getenv("log")).getOrElse("FATAL").toUpperCase)

  private val rootLog = Logger.get("")
  private var oldLevel: jlogging.Level = _

  abstract override def withFixture(test: NoArgTest) {
    oldLevel = rootLog.getLevel()
    rootLog.setLevel(logLevel)
    rootLog.addHandler(new ConsoleHandler(new Formatter(), None))
    try {
      super.withFixture(test)
    } finally {
      rootLog.clearHandlers()
      rootLog.setLevel(oldLevel)
    }
  }
}

class JournaledQueueSpec extends Spec with ShouldMatchers with TempFolder with TestLogging2 {
  val READER_CONFIG = new JournaledQueueReaderConfig()
  val CONFIG = new JournaledQueueConfig(name = "test")

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
