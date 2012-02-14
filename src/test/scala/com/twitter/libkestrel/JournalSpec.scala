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

class JournalSpec extends ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
  def makeJournal(name: String, maxFileSize: StorageUnit): Journal =
    new Journal(testFolder, name, maxFileSize, null, Duration.MaxValue, None)

  def makeJournal(name: String): Journal = makeJournal(name, 16.megabytes)

  def addItem(j: Journal, size: Int) = {
    val now = Time.now.inMilliseconds
    j.put(ByteBuffer.allocate(size), Time.now, None)
    now
  }

  def stringToBuffer(s: String) = ByteBuffer.wrap(s.getBytes)

  describe("Journal") {
    it("find reader/writer files") {
      Time.withCurrentTimeFrozen { timeMutator =>
        List(
          "test.read.client1", "test.read.client2", "test.read.client1~~", "test.readmenot",
          "test.read."
        ).foreach { name =>
          BookmarkFile.create(new File(testFolder, name)).close()
        }
        List(
          "test.901", "test.8000", "test.3leet", "test.1", "test.5005"
        ).foreach { name =>
          val jf = JournalFile.create(new File(testFolder, name), null, Duration.MaxValue, 16.kilobytes)
          jf.put(QueueItem(1L, Time.now, None, ByteBuffer.allocate(1)))
          jf.close()
        }

        val j = makeJournal("test")
        assert(j.writerFiles().map { _.getName }.toSet ===
          Set("test.901", "test.8000", "test.1", "test.5005"))
        assert(j.readerFiles().map { _.getName }.toSet ===
          Set("test.read.client1", "test.read.client2"))
        j.close()

        new File(testFolder, "test.read.client1").delete()
        new File(testFolder, "test.read.client2").delete()
        val j2 = makeJournal("test")
        assert(j2.readerFiles().map { _.getName }.toSet ===
          Set("test.read."))
        j2.close()
      }
    }

    it("erase old temporary files") {
      List("test.1", "test.read.1", "test.read.1~~").foreach { name =>
        new File(testFolder, name).createNewFile()
      }

      val j = makeJournal("test")
      assert(!new File(testFolder, "test.read.1~~").exists)
      j.close()
    }

    it("erase all journal files") {
      List("test.1", "test.read.1", "test.read.1~~", "testbad").foreach { name =>
        new File(testFolder, name).createNewFile()
      }

      val j = makeJournal("test")
      j.erase()

      assert(testFolder.list.toList === List("testbad"))
    }

    it("report size correctly") {
      val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.megabytes)
      jf1.put(QueueItem(101L, Time.now, None, ByteBuffer.allocate(1000)))
      jf1.close()
      val jf2 = JournalFile.create(new File(testFolder, "test.2"), null, Duration.MaxValue, 16.megabytes)
      jf2.put(QueueItem(102L, Time.now, None, ByteBuffer.allocate(1000)))
      jf2.close()

      val j = makeJournal("test")
      assert(j.journalSize === 2058L)
      j.close()
    }

    describe("fileForId") {
      it("startup") {
        List(
          ("test.901", 901),
          ("test.8000", 8000),
          ("test.1", 1),
          ("test.5005", 5005)
        ).foreach { case (name, id) =>
          val jf = JournalFile.create(new File(testFolder, name), null, Duration.MaxValue, 16.kilobytes)
          jf.put(QueueItem(id, Time.now, None, ByteBuffer.allocate(5)))
          jf.close()
        }

        val j = makeJournal("test")
        assert(j.fileInfoForId(1) === Some(FileInfo(new File(testFolder, "test.1"), 1, 1, 1, 5)))
        assert(j.fileInfoForId(0) === None)
        assert(j.fileInfoForId(555) === Some(FileInfo(new File(testFolder, "test.1"), 1, 1, 1, 5)))
        assert(j.fileInfoForId(900) === Some(FileInfo(new File(testFolder, "test.1"), 1, 1, 1, 5)))
        assert(j.fileInfoForId(901) === Some(FileInfo(new File(testFolder, "test.901"), 901, 901, 1, 5)))
        assert(j.fileInfoForId(902) === Some(FileInfo(new File(testFolder, "test.901"), 901, 901, 1, 5)))
        assert(j.fileInfoForId(6666) === Some(FileInfo(new File(testFolder, "test.5005"), 5005, 5005, 1, 5)))
        assert(j.fileInfoForId(9999) === Some(FileInfo(new File(testFolder, "test.8000"), 8000, 8000, 1, 5)))
        j.close()
      }

      it("during journal rotation") {
        Time.withCurrentTimeFrozen { timeMutator =>
          val j = makeJournal("test", 1078.bytes)
          j.put(ByteBuffer.allocate(512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(ByteBuffer.allocate(512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(ByteBuffer.allocate(512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(ByteBuffer.allocate(512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(ByteBuffer.allocate(512), Time.now, None)

          val file1 = new File(testFolder, "test." + 4.milliseconds.ago.inMilliseconds)
          val file2 = new File(testFolder, "test." + 2.milliseconds.ago.inMilliseconds)
          val file3 = new File(testFolder, "test." + Time.now.inMilliseconds)

          assert(j.fileInfoForId(1) === Some(FileInfo(file1, 1, 2, 2, 1024)))
          assert(j.fileInfoForId(2) === Some(FileInfo(file1, 1, 2, 2, 1024)))
          assert(j.fileInfoForId(3) === Some(FileInfo(file2, 3, 4, 2, 1024)))
          assert(j.fileInfoForId(4) === Some(FileInfo(file2, 3, 4, 2, 1024)))
          assert(j.fileInfoForId(5) === Some(FileInfo(file3, 5, 0, 0, 0)))
          j.close()
        }
      }
    }

    it("checkpoint readers") {
      List("test.read.client1", "test.read.client2").foreach { name =>
        val bf = BookmarkFile.create(new File(testFolder, name))
        bf.readHead(100L)
        bf.readDone(Array(102L))
        bf.close()
      }
      val jf = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
      jf.put(QueueItem(100L, Time.now, None, stringToBuffer("hi")))
      jf.put(QueueItem(105L, Time.now, None, stringToBuffer("hi")))
      jf.close()

      val j = makeJournal("test")
      j.reader("client1").commit(101L)
      j.reader("client2").commit(103L)
      j.checkpoint()
      j.close()

      assert(BookmarkFile.open(new File(testFolder, "test.read.client1")).toList === List(
        Record.ReadHead(102L),
        Record.ReadDone(Array[Long]())
      ))

      assert(BookmarkFile.open(new File(testFolder, "test.read.client2")).toList === List(
        Record.ReadHead(100L),
        Record.ReadDone(Array(102L, 103L))
      ))
    }

    it("doesn't checkpoint readers that haven't changed") {
      val j = makeJournal("test")
      j.reader("client1").commit(1L)
      j.reader("client1").checkpoint()
      assert(new File(testFolder, "test.read.client1").exists)

      new File(testFolder, "test.read.client1").delete()
      assert(!new File(testFolder, "test.read.client1").exists)
      j.reader("client1").checkpoint()
      assert(!new File(testFolder, "test.read.client1").exists)
      j.close()
    }

    it("make new reader") {
      val j = makeJournal("test")
      var r = j.reader("new")
      r.head = 100L
      r.commit(101L)
      r.checkpoint()
      j.close()

      assert(BookmarkFile.open(new File(testFolder, "test.read.new")).toList === List(
        Record.ReadHead(101L),
        Record.ReadDone(Array[Long]())
      ))
    }

    it("create a default reader when no others exist") {
      val j = makeJournal("test")
      j.close()

      assert(BookmarkFile.open(new File(testFolder, "test.read.")).toList === List(
        Record.ReadHead(0L),
        Record.ReadDone(Array[Long]())
      ))
    }

    it("convert the default reader to a named reader when one is created") {
      val j = makeJournal("test")
      val reader = j.reader("")
      reader.head = 100L
      reader.checkpoint()

      assert(new File(testFolder, "test.read.").exists)

      j.reader("hello")
      assert(!new File(testFolder, "test.read.").exists)
      assert(new File(testFolder, "test.read.hello").exists)

      assert(BookmarkFile.open(new File(testFolder, "test.read.hello")).toList === List(
        Record.ReadHead(100L),
        Record.ReadDone(Array[Long]())
      ))

      j.close()
    }

    describe("recover a reader") {
      /*
       * rationale:
       * this can happen if a write journal is corrupted/truncated, losing the last few items, and
       * a reader had already written a state file out claiming to have finished processing the
       * items that are now lost.
       */
      it("with a head id in the future") {
        // create main journal
        val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
        jf1.put(QueueItem(390L, Time.now, None, stringToBuffer("hi")))
        jf1.put(QueueItem(400L, Time.now, None, stringToBuffer("hi")))
        jf1.close()

        // create bookmarks with impossible ids
        val bf1 = BookmarkFile.create(new File(testFolder, "test.read.1"))
        bf1.readHead(402L)
        bf1.close()
        val bf2 = BookmarkFile.create(new File(testFolder, "test.read.2"))
        bf2.readHead(390L)
        bf2.readDone(Array(395L, 403L))
        bf2.close()

        val j = makeJournal("test")
        val r1 = j.reader("1")
        assert(r1.head === 400L)
        assert(r1.doneSet === Set())
        val r2 = j.reader("2")
        assert(r2.head === 390L)
        assert(r2.doneSet === Set(395L))

        j.close()
      }

      it("with a head id that doesn't exist anymore") {
        val jf1 = JournalFile.create(new File(testFolder, "test.1"), null, Duration.MaxValue, 16.kilobytes)
        jf1.put(QueueItem(800L, Time.now, None, stringToBuffer("hi")))
        jf1.close()
        val bf = BookmarkFile.create(new File(testFolder, "test.read.1"))
        bf.readHead(600L)
        bf.readDone(Array[Long]())
        bf.close()

        val j = makeJournal("test")
        assert(j.reader("1").head === 799L)

        j.close()
      }
    }

    it("start with an empty journal") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)
        val j = makeJournal("test")
        val (item, future) = j.put(stringToBuffer("hi"), Time.now, None)()
        assert(item.id === 1L)
        j.close()

        val file = new File(testFolder, "test." + Time.now.inMilliseconds)
        val jf = JournalFile.open(file)
        assert(jf.readNext() ===
          Some(Record.Put(QueueItem(1L, roundedTime, None, stringToBuffer("hi")))))
        jf.close()
      }
    }

    it("append new items to the end of the last journal") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)
        val file1 = new File(testFolder, "test.1")
        val jf1 = JournalFile.create(file1, null, Duration.MaxValue, 16.kilobytes)
        jf1.put(QueueItem(101L, Time.now, None, stringToBuffer("101")))
        jf1.close()
        val file2 = new File(testFolder, "test.2")
        val jf2 = JournalFile.create(file2, null, Duration.MaxValue, 16.kilobytes)
        jf2.put(QueueItem(102L, Time.now, None, stringToBuffer("102")))
        jf2.close()

        val j = makeJournal("test")
        val (item, future) = j.put(stringToBuffer("hi"), Time.now, None)()
        assert(item.id === 103L)
        j.close()

        val jf3 = JournalFile.open(file2)
        assert(jf3.readNext() ===
          Some(Record.Put(QueueItem(102L, roundedTime, None, stringToBuffer("102")))))
        assert(jf3.readNext() ===
          Some(Record.Put(QueueItem(103L, roundedTime, None, stringToBuffer("hi")))))
        assert(jf3.readNext() === None)
        jf3.close()
      }
    }

    it("truncate corrupted journal") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)

        // write 2 valid entries, but truncate the last one to make it corrupted.
        val file = new File(testFolder, "test.1")
        val jf = JournalFile.create(file, null, Duration.MaxValue, 16.kilobytes)
        jf.put(QueueItem(101L, Time.now, None, stringToBuffer("101")))
        jf.put(QueueItem(102L, Time.now, None, stringToBuffer("102")))
        jf.close()

        val raf = new RandomAccessFile(file, "rw")
        raf.setLength(file.length - 1)
        raf.close()

        val j = makeJournal("test")
        val (item, future) = j.put(stringToBuffer("hi"), Time.now, None)()
        assert(item.id === 102L)
        j.close()

        val jf2 = JournalFile.open(file)
        assert(jf2.readNext() ===
          Some(Record.Put(QueueItem(101L, roundedTime, None, stringToBuffer("101")))))
        assert(jf2.readNext() ===
          Some(Record.Put(QueueItem(102L, roundedTime, None, stringToBuffer("hi")))))
        assert(jf2.readNext() === None)
        jf2.close()
      }
    }

    it("rotate journal files") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val j = makeJournal("test", 1.kilobyte)

        val time1 = addItem(j, 475)
        timeMutator.advance(1.millisecond)
        val time2 = addItem(j, 475)
        timeMutator.advance(1.millisecond)
        val time3 = addItem(j, 475)
        j.close()

        val file1 = new File(testFolder, "test." + time1)
        val file2 = new File(testFolder, "test." + time3)
        val defaultReader = new File(testFolder, "test.read.")
        assert(testFolder.list.sorted.toList === List(file1, file2, defaultReader).map { _.getName() })
        val jf1 = JournalFile.open(file1)
        assert(jf1.toList === List(
          Record.Put(QueueItem(1L, Time.fromMilliseconds(time1), None, ByteBuffer.allocate(475))),
          Record.Put(QueueItem(2L, Time.fromMilliseconds(time2), None, ByteBuffer.allocate(475)))
        ))
        jf1.close()
        val jf2 = JournalFile.open(file2)
        assert(jf2.toList === List(
          Record.Put(QueueItem(3L, Time.fromMilliseconds(time3), None, ByteBuffer.allocate(475)))
        ))
        jf2.close()
      }
    }

    describe("clean up any dead files behind it") {
      it("when a client catches up") {
        Time.withCurrentTimeFrozen { timeMutator =>
          val j = makeJournal("test", 1.kilobyte)
          val reader = j.reader("client")

          val time1 = addItem(j, 475)
          timeMutator.advance(1.millisecond)
          val time2 = addItem(j, 475)
          timeMutator.advance(1.millisecond)
          val time3 = addItem(j, 475)
          timeMutator.advance(1.millisecond)

          assert(new File(testFolder, "test." + time1).exists)
          assert(new File(testFolder, "test." + time3).exists)

          reader.commit(1L)
          reader.commit(2L)
          j.checkpoint()

          assert(!new File(testFolder, "test." + time1).exists)
          assert(new File(testFolder, "test." + time3).exists)

          j.close()
        }
      }

      it("when the journal moves to a new file") {
        Time.withCurrentTimeFrozen { timeMutator =>
          val j = makeJournal("test", 1.kilobyte)
          val reader = j.reader("client")
          val time0 = Time.now.inMilliseconds
          timeMutator.advance(1.millisecond)

          val time1 = addItem(j, 512)
          timeMutator.advance(1.millisecond)
          val time2 = addItem(j, 512)
          timeMutator.advance(1.millisecond)
          reader.commit(1L)
          reader.commit(2L)
          reader.checkpoint()

          assert(new File(testFolder, "test." + time0).exists)
          assert(new File(testFolder, "test." + time2).exists)

          val time3 = addItem(j, 512)
          timeMutator.advance(1.millisecond)
          val time4 = addItem(j, 512)

          assert(!new File(testFolder, "test." + time0).exists)
          assert(new File(testFolder, "test." + time3).exists)
          assert(new File(testFolder, "test." + time4).exists)

          j.close()
        }
      }
    }
  }
}
