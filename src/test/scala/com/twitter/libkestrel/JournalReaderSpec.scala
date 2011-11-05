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
import java.nio.ByteBuffer
import org.specs.Specification

class JournalReaderSpec extends Specification with TestLogging with TestFolder {
  def makeJournal(name: String, maxFileSize: StorageUnit): Journal =
    new Journal(testFolder, name, maxFileSize, null, Duration.MaxValue)

  def makeJournal(name: String): Journal = makeJournal(name, 16.megabytes)

  "Journal#Reader" should {
    def makeFiles() {
      val jf1 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
      jf1.put(QueueItem(100L, Time.now, None, "100".getBytes))
      jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
      jf1.close()

      val jf2 = JournalFile.createWriter(new File(testFolder, "test.2"), null, Duration.MaxValue)
      jf2.put(QueueItem(102L, Time.now, None, "102".getBytes))
      jf2.put(QueueItem(103L, Time.now, None, "103".getBytes))
      jf2.close()

      val jf3 = JournalFile.createWriter(new File(testFolder, "test.3"), null, Duration.MaxValue)
      jf3.put(QueueItem(104L, Time.now, None, "104".getBytes))
      jf3.put(QueueItem(105L, Time.now, None, "105".getBytes))
      jf3.close()
    }

    "be created with a checkpoint file" in {
      val j = makeJournal("test")
      j.reader("hello")

      new File(testFolder, "test.read.hello").exists mustEqual true
      j.close()
    }

    "write a checkpoint" in {
      val file = new File(testFolder, "a1")
      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.head = 123L
      reader.commit(125L)
      reader.commit(130L)
      reader.checkpoint()

      JournalFile.openReader(file, null, Duration.MaxValue).toList mustEqual List(
        JournalFile.Record.ReadHead(123L),
        JournalFile.Record.ReadDone(Array(125L, 130L))
      )
    }

    "read a checkpoint" in {
      val file = new File(testFolder, "a1")
      val jf = JournalFile.createReader(file, null, Duration.MaxValue)
      jf.readHead(900L)
      jf.readDone(Array(902L, 903L))
      jf.close()

      val jf2 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
      jf2.put(QueueItem(890L, Time.now, None, "hi".getBytes))
      jf2.put(QueueItem(910L, Time.now, None, "hi".getBytes))
      jf2.close()

      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.readState()
      reader.head mustEqual 900L
      reader.doneSet.toList.sorted mustEqual List(902L, 903L)
    }

    "track committed items" in {
      val file = new File(testFolder, "a1")
      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.head = 123L

      reader.commit(124L)
      reader.head mustEqual 124L
      reader.doneSet.toList.sorted mustEqual List()

      reader.commit(126L)
      reader.commit(127L)
      reader.commit(129L)
      reader.head mustEqual 124L
      reader.doneSet.toList.sorted mustEqual List(126L, 127L, 129L)

      reader.commit(125L)
      reader.head mustEqual 127L
      reader.doneSet.toList.sorted mustEqual List(129L)

      reader.commit(130L)
      reader.commit(128L)
      reader.head mustEqual 130L
      reader.doneSet.toList.sorted mustEqual List()
    }

    "flush all items" in {
      val j = makeJournal("test")
      val (item1, future1) = j.put("hi".getBytes, Time.now, None)()
      val reader = j.reader("1")
      reader.commit(item1.id)
      val (item2, future2) = j.put("bye".getBytes, Time.now, None)()

      reader.head mustEqual item1.id
      reader.flush()
      reader.head mustEqual item2.id
    }

    "read-behind" in {
      "start" in {
        val jf = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
        jf.put(QueueItem(100L, Time.now, None, "100".getBytes))
        jf.put(QueueItem(101L, Time.now, None, "101".getBytes))
        jf.close()

        val j = makeJournal("test")
        val reader = j.reader("client1")
        reader.head = 100L
        reader.inReadBehind mustEqual false
        reader.startReadBehind(100L)
        reader.inReadBehind mustEqual true
        val item = reader.nextReadBehind()
        item.map { _.id } mustEqual Some(101L)
        new String(item.get.data) mustEqual "101"

        reader.endReadBehind()
        reader.inReadBehind mustEqual false
      }

      "start at file edge" in {
        val jf1 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
        jf1.put(QueueItem(100L, Time.now, None, "100".getBytes))
        jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
        jf1.close()

        val jf2 = JournalFile.createWriter(new File(testFolder, "test.2"), null, Duration.MaxValue)
        jf2.put(QueueItem(102L, Time.now, None, "102".getBytes))
        jf2.put(QueueItem(103L, Time.now, None, "103".getBytes))
        jf2.close()

        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.head = 101L
        reader.startReadBehind(101L)
        reader.nextReadBehind().map { _.id } mustEqual Some(102L)
      }

      "across journal files" in {
        "when asked to" in {
          makeFiles()
          val j = makeJournal("test")
          val reader = j.reader("client")
          reader.head = 102L
          reader.startReadBehind(102L)
          reader.nextReadBehind().map { _.id } mustEqual Some(103L)

          val item = reader.nextReadBehind()
          item.map { _.id } mustEqual Some(104L)
          new String(item.get.data) mustEqual "104"
          j.close()
        }

        "when asked not to" in {
          makeFiles()
          val j = makeJournal("test")
          val reader = j.reader("client")
          reader.head = 102L
          reader.startReadBehind(102L)
          reader.nextReadBehind(followFiles = false).map { _.id } mustEqual Some(103L)
          reader.nextReadBehind(followFiles = false) mustEqual None
          j.close()
        }
      }

      "until it catches up" in {
        val jf1 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
        jf1.put(QueueItem(100L, Time.now, None, "100".getBytes))
        jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
        jf1.close()

        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.head = 100L
        reader.startReadBehind(100L)

        reader.inReadBehind mustEqual true
        reader.nextReadBehind().map { _.id } mustEqual Some(101L)
        reader.inReadBehind mustEqual true
        reader.nextReadBehind().map { _.id } mustEqual None
        reader.inReadBehind mustEqual false
        j.close()
      }
    }

    "fileInfosAfterReadBehind" in {
      makeFiles()
      val j = makeJournal("test")
      val reader = j.reader("client")
      reader.head = 100L
      reader.startReadBehind(100L)
      reader.nextReadBehind(followFiles = false).map { _.id } mustEqual Some(101L)
      reader.nextReadBehind(followFiles = false) mustEqual None
      reader.fileInfosAfterReadBehind.toList mustEqual List(
        FileInfo(new File(testFolder, "test.2"), 102L, 103L, 2, 6),
        FileInfo(new File(testFolder, "test.3"), 104L, 105L, 2, 6)
      )
    }
  }
}
