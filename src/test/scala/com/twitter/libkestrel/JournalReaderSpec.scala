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
import java.nio.ByteBuffer
import org.scalatest.{AbstractSuite, FunSpec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class JournalReaderSpec extends FunSpec with ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {

  implicit def stringToBuffer(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes)

  def makeJournal(name: String, maxFileSize: StorageUnit): Journal =
    new Journal(testFolder, name, maxFileSize, null, Duration.MaxValue, None)

  def makeJournal(name: String): Journal = makeJournal(name, 16.megabytes)

  def makeFiles() {
    val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
    jf1.put(QueueItem(100L, Time.now, None, "100"))
    jf1.put(QueueItem(101L, Time.now, None, "101"))
    jf1.close()

    val jf2 = JournalFile.create(new File(testFolder, "test.2"), null, Duration.MaxValue, 16.kilobytes)
    jf2.put(QueueItem(102L, Time.now, None, "102"))
    jf2.put(QueueItem(103L, Time.now, None, "103"))
    jf2.close()

    val jf3 = JournalFile.create(new File(testFolder, "test.3"), null, Duration.MaxValue, 16.kilobytes)
    jf3.put(QueueItem(104L, Time.now, None, "104"))
    jf3.put(QueueItem(105L, Time.now, None, "105"))
    jf3.close()
  }

  def queueItem(id: Long) = QueueItem(id, Time.now, None, "blah")

  describe("Journal#Reader") {
    it("created with a checkpoint file") {
      val j = makeJournal("test")
      j.reader("hello")

      assert(new File(testFolder, "test.read.hello").exists)
      j.close()
    }

    it("write a checkpoint") {
      val file = new File(testFolder, "a1")
      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.head = 123L
      reader.commit(queueItem(125L))
      reader.commit(queueItem(130L))
      reader.checkpoint()

      assert(BookmarkFile.open(file).toList === List(
        Record.ReadHead(123L),
        Record.ReadDone(Array(125L, 130L))
      ))

      j.close()
    }

    it("read a checkpoint") {
      val file = new File(testFolder, "a1")
      val bf = BookmarkFile.create(file)
      bf.readHead(900L)
      bf.readDone(Array(902L, 903L))
      bf.close()

      val jf = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
      jf.put(queueItem(890L))
      jf.put(queueItem(910L))
      jf.close()

      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.open()
      assert(reader.head === 900L)
      assert(reader.doneSet.toList.sorted === List(902L, 903L))
      reader.close()
      j.close()
    }

    it("track committed items") {
      val file = new File(testFolder, "a1")
      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.head = 123L

      reader.commit(queueItem(124L))
      assert(reader.head === 124L)
      assert(reader.doneSet.toList.sorted === List())

      reader.commit(queueItem(126L))
      reader.commit(queueItem(127L))
      reader.commit(queueItem(129L))
      assert(reader.head === 124L)
      assert(reader.doneSet.toList.sorted === List(126L, 127L, 129L))

      reader.commit(queueItem(125L))
      assert(reader.head === 127L)
      assert(reader.doneSet.toList.sorted === List(129L))

      reader.commit(queueItem(130L))
      reader.commit(queueItem(128L))
      assert(reader.head === 130L)
      assert(reader.doneSet.toList.sorted === List())

      j.close()
    }

    it("flush all items") {
      val j = makeJournal("test")
      val (item1, future1) = j.put(ByteBuffer.wrap("hi".getBytes), Time.now, None)
      val reader = j.reader("1")
      reader.open()
      reader.commit(item1)
      val (item2, future2) = j.put(ByteBuffer.wrap("bye".getBytes), Time.now, None)

      assert(reader.head === item1.id)
      reader.flush()
      assert(reader.head === item2.id)

      j.close()
    }

    describe("file boundaries") {
      def createJournalFiles(journalName: String, startId: Long, idsPerFile: Int, files: Int, head: Long) {
        for (fileNum <- 1 to files) {
          val name = "%s.%d".format(journalName, fileNum)
          val jf = JournalFile.create(new File(testFolder, name), null, Duration.MaxValue, 16.kilobytes)
          for(idNum <- 1 to idsPerFile) {
            val id = startId + ((fileNum - 1) * idsPerFile) + (idNum - 1)
            jf.put(QueueItem(id, Time.now, None, ByteBuffer.wrap(id.toString.getBytes)))
          }
          jf.close()
        }

        val name = "%s.read.client".format(journalName)
        val bf = BookmarkFile.create(new File(testFolder, name))
        bf.readHead(head)
        bf.close()
      }

      it("should start at a file edge") {
        createJournalFiles("test", 100L, 2, 2, 101L)
        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.open()

        assert(reader.next().map { _.id } === Some(102L))

        j.close()
      }

      it("should cross files") {
        createJournalFiles("test", 100L, 2, 2, 100L)
        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.open()

        assert(reader.next().map { _.id } === Some(101L))
        assert(reader.next().map { _.id } === Some(102L))

        j.close()
      }

      it("should peek at leading file edge") {
        createJournalFiles("test", 100L, 2, 2, 101L)
        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.open()

        assert(reader.peekOldest().map { _.id } === Some(102L))
        assert(reader.peekOldest().map { _.id } === Some(102L))

        j.close()
      }

      it("should peek at trailing file edge") {
        createJournalFiles("test", 100L, 2, 2, 100L)
        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.open()

        assert(reader.peekOldest().map { _.id } === Some(101L))
        assert(reader.peekOldest().map { _.id } === Some(101L))

        j.close()
      }

      it("should conditionally get at leading file edge") {
        createJournalFiles("test", 100L, 2, 2, 101L)
        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.open()

        assert(reader.nextIf { _ => false } === None)
        assert(reader.nextIf { _ => true }.map { _.id } === Some(102L))
        assert(reader.nextIf { _ => true }.map { _.id } === Some(103L))

        j.close()
      }

      it("should conditionally get at trailing file edge") {
        createJournalFiles("test", 100L, 2, 2, 100L)
        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.open()

        assert(reader.nextIf { _ => false } === None)
        assert(reader.nextIf { _ => true }.map { _.id } === Some(101L))
        assert(reader.nextIf { _ => true }.map { _.id } === Some(102L))

        j.close()
      }
    }

    it("fileInfosAfter") {
      makeFiles()
      val j = makeJournal("test")
      val reader = j.reader("client")
      reader.head = 100L

      assert(j.fileInfosAfter(101L).toList === List(
        FileInfo(new File(testFolder, "test.2"), 102L, 103L, 2, 6),
        FileInfo(new File(testFolder, "test.3"), 104L, 105L, 2, 6)
      ))
      j.close()
    }
  }
}
