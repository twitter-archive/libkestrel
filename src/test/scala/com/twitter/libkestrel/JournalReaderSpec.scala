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
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class JournalReaderSpec extends ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
  def makeJournal(name: String, maxFileSize: StorageUnit): Journal =
    new Journal(testFolder, name, maxFileSize, null, Duration.MaxValue, None)

  def makeJournal(name: String): Journal = makeJournal(name, 16.megabytes)

  def makeFiles() {
    val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
    jf1.put(QueueItem(100L, Time.now, None, ByteBuffer.wrap("100".getBytes)))
    jf1.put(QueueItem(101L, Time.now, None, ByteBuffer.wrap("101".getBytes)))
    jf1.close()

    val jf2 = JournalFile.create(new File(testFolder, "test.2"), null, Duration.MaxValue, 16.kilobytes)
    jf2.put(QueueItem(102L, Time.now, None, ByteBuffer.wrap("102".getBytes)))
    jf2.put(QueueItem(103L, Time.now, None, ByteBuffer.wrap("103".getBytes)))
    jf2.close()

    val jf3 = JournalFile.create(new File(testFolder, "test.3"), null, Duration.MaxValue, 16.kilobytes)
    jf3.put(QueueItem(104L, Time.now, None, ByteBuffer.wrap("104".getBytes)))
    jf3.put(QueueItem(105L, Time.now, None, ByteBuffer.wrap("105".getBytes)))
    jf3.close()
  }

  def bufferToString(b: ByteBuffer) = {
    val bytes = new Array[Byte](b.remaining)
    b.get(bytes)
    new String(bytes)
  }

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
      reader.commit(125L)
      reader.commit(130L)
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
      jf.put(QueueItem(890L, Time.now, None, ByteBuffer.wrap("hi".getBytes)))
      jf.put(QueueItem(910L, Time.now, None, ByteBuffer.wrap("hi".getBytes)))
      jf.close()

      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.readState()
      assert(reader.head === 900L)
      assert(reader.doneSet.toList.sorted === List(902L, 903L))
      j.close()
    }

    it("track committed items") {
      val file = new File(testFolder, "a1")
      val j = makeJournal("test")
      val reader = new j.Reader("1", file)
      reader.head = 123L

      reader.commit(124L)
      assert(reader.head === 124L)
      assert(reader.doneSet.toList.sorted === List())

      reader.commit(126L)
      reader.commit(127L)
      reader.commit(129L)
      assert(reader.head === 124L)
      assert(reader.doneSet.toList.sorted === List(126L, 127L, 129L))

      reader.commit(125L)
      assert(reader.head === 127L)
      assert(reader.doneSet.toList.sorted === List(129L))

      reader.commit(130L)
      reader.commit(128L)
      assert(reader.head === 130L)
      assert(reader.doneSet.toList.sorted === List())

      j.close()
    }

    it("flush all items") {
      val j = makeJournal("test")
      val (item1, future1) = j.put(ByteBuffer.wrap("hi".getBytes), Time.now, None)()
      val reader = j.reader("1")
      reader.commit(item1.id)
      val (item2, future2) = j.put(ByteBuffer.wrap("bye".getBytes), Time.now, None)()

      assert(reader.head === item1.id)
      reader.flush()
      assert(reader.head === item2.id)

      j.close()
    }

    describe("read-behind") {
      it("start") {
        val jf = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
        jf.put(QueueItem(100L, Time.now, None, ByteBuffer.wrap("100".getBytes)))
        jf.put(QueueItem(101L, Time.now, None, ByteBuffer.wrap("101".getBytes)))
        jf.close()

        val j = makeJournal("test")
        val reader = j.reader("client1")
        reader.head = 100L
        assert(!reader.inReadBehind)
        reader.startReadBehind(100L)
        assert(reader.inReadBehind)
        val item = reader.nextReadBehind()
        assert(item.map { _.id } === Some(101L))
        assert(bufferToString(item.get.data) === "101")

        reader.endReadBehind()
        assert(!reader.inReadBehind)

        j.close()
      }

      it("start at file edge") {
        val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
        jf1.put(QueueItem(100L, Time.now, None, ByteBuffer.wrap("100".getBytes)))
        jf1.put(QueueItem(101L, Time.now, None, ByteBuffer.wrap("101".getBytes)))
        jf1.close()

        val jf2 = JournalFile.create(new File(testFolder, "test.2"), null, Duration.MaxValue, 16.kilobytes)
        jf2.put(QueueItem(102L, Time.now, None, ByteBuffer.wrap("102".getBytes)))
        jf2.put(QueueItem(103L, Time.now, None, ByteBuffer.wrap("103".getBytes)))
        jf2.close()

        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.head = 101L
        reader.startReadBehind(101L)
        assert(reader.nextReadBehind().map { _.id } === Some(102L))

        j.close()
      }

      describe("across journal files") {
        it("when asked to") {
          makeFiles()
          val j = makeJournal("test")
          val reader = j.reader("client")
          reader.head = 102L
          reader.startReadBehind(102L)
          assert(reader.nextReadBehind().map { _.id } === Some(103L))

          val item = reader.nextReadBehind()
          assert(item.map { _.id } === Some(104L))
          assert(bufferToString(item.get.data) === "104")
          j.close()
        }

        it("when asked not to") {
          makeFiles()
          val j = makeJournal("test")
          val reader = j.reader("client")
          reader.head = 102L
          val scanner = new reader.Scanner(102L, followFiles = false)
          assert(scanner.next().map { _.id } === Some(103L))
          assert(scanner.next() === None)
          j.close()
        }
      }

      it("until it catches up") {
        val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
        jf1.put(QueueItem(100L, Time.now, None, ByteBuffer.wrap("100".getBytes)))
        jf1.put(QueueItem(101L, Time.now, None, ByteBuffer.wrap("101".getBytes)))
        jf1.close()

        val j = makeJournal("test")
        val reader = j.reader("client")
        reader.head = 100L
        reader.startReadBehind(100L)

        assert(reader.inReadBehind === true)
        assert(reader.nextReadBehind().map { _.id } === Some(101L))
        assert(reader.inReadBehind === true)
        assert(reader.nextReadBehind().map { _.id } === None)
        assert(reader.inReadBehind === false)
        j.close()
      }
    }

    it("fileInfosAfter") {
      makeFiles()
      val j = makeJournal("test")
      val reader = j.reader("client")
      reader.head = 100L
      reader.startReadBehind(100L)
      assert(j.fileInfosAfter(101L).toList === List(
        FileInfo(new File(testFolder, "test.2"), 102L, 103L, 2, 6),
        FileInfo(new File(testFolder, "test.3"), 104L, 105L, 2, 6)
      ))
      j.close()
    }

  }
}
