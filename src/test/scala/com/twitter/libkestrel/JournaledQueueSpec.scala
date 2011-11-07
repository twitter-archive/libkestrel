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
import com.twitter.logging.TestLogging
import com.twitter.util._
import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import org.specs.Specification
import org.specs.matcher.Matcher

trait Argh { self: Specification =>
  import com.twitter.logging._
  import java.util.{logging => jlogging}

  val logLevel = Logger.levelNames(Option[String](System.getenv("log")).getOrElse("FATAL").toUpperCase)

  private val rootLog = Logger.get("")
  private var oldLevel: jlogging.Level = _

  doBeforeSpec {
    oldLevel = rootLog.getLevel()
    rootLog.setLevel(logLevel)
    rootLog.addHandler(new ConsoleHandler(new Formatter(), None))
  }

  doAfterSpec {
    rootLog.clearHandlers()
    rootLog.setLevel(oldLevel)
  }
}

class JournaledQueueSpec extends Specification with Argh with TestFolder {
  val READER_CONFIG = new JournaledQueueReaderConfig()
  val CONFIG = new JournaledQueueConfig(name = "test")

  case class haveId(id: Long) extends Matcher[Array[Byte]]() {
    def apply(data: => Array[Byte]) = (
      {
        val buffer = ByteBuffer.wrap(data)
        buffer.order(ByteOrder.BIG_ENDIAN)
        buffer.getLong() == id
      },
      "data matches id " + id,
      "data doesn't match id " + id
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

  "JournaledQueue" should {
    "create" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        val q = new JournaledQueue(CONFIG, testFolder, null)
        val reader = q.reader("")
        new File(testFolder, "test." + Time.now.inMilliseconds).exists mustEqual true
        reader.checkpoint()
        new File(testFolder, "test.read.").exists mustEqual true
      }
    }

    "read existing journals" in {
      "small" in {
        setupWriteJournals(4, 2)
        setupReadJournal("", 3L)
        val q = new JournaledQueue(CONFIG, testFolder, null)
        val reader = q.reader("")

        reader.items mustEqual 5
        reader.bytes mustEqual 5 * 1024
        reader.memoryBytes mustEqual 5 * 1024
        val item = reader.get(None)()
        item must beSome[QueueItem].which { item =>
          item.data must haveId(4L)
          item.id mustEqual 4L
        }
      }

      "in read-behind" in {
        setupWriteJournals(4, 2)
        (0L to 3L).foreach { readId =>
          setupReadJournal("", readId)
          val readerConfig = READER_CONFIG.copy(maxMemorySize = 4.kilobytes)
          val q = new JournaledQueue(CONFIG.copy(defaultReaderConfig = readerConfig), testFolder, null)
          val reader = q.reader("")

          reader.items mustEqual (8 - readId)
          reader.bytes mustEqual ((8 - readId) * 1024)
          reader.memoryBytes mustEqual (4 * 1024)
        }
      }

      "with a caught-up reader" in {
        setupWriteJournals(4, 2)
        setupReadJournal("", 8L)
        val q = new JournaledQueue(CONFIG, testFolder, null)
        val reader = q.reader("")

        reader.items mustEqual 0
        reader.bytes mustEqual 0L
        reader.memoryBytes mustEqual 0L
        reader.get(None)() mustEqual None
      }
    }

    "exist without a journal" in {
      val q = new JournaledQueue(CONFIG.copy(journaled = false), testFolder, null)

      //q.put()
      // FIXME
    }
  }
}
