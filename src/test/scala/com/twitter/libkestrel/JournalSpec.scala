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

class JournalSpec extends Specification with TestLogging with TestFolder {
  def makeJournal(name: String, maxFileSize: StorageUnit): Journal =
    new Journal(testFolder, name, maxFileSize, null, Duration.MaxValue)

  def makeJournal(name: String): Journal = makeJournal(name, 16.megabytes)

  def addItem(j: Journal, size: Int) = {
    val now = Time.now.inMilliseconds
    j.put(new Array[Byte](size), Time.now, None)
    now
  }

  "Journal" should {
    "find reader/writer files" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        List(
          "test.read.client1", "test.read.client2", "test.read.client1~~", "test.readmenot",
          "test.read."
        ).foreach { name =>
          JournalFile.createReader(new File(testFolder, name), null, Duration.MaxValue).close()
        }
        List(
          "test.901", "test.8000", "test.3leet", "test.1", "test.5005"
        ).foreach { name =>
          val jf = JournalFile.createWriter(new File(testFolder, name), null, Duration.MaxValue)
          jf.put(QueueItem(1L, Time.now, None, new Array[Byte](1)))
          jf.close()
        }

        val j = makeJournal("test")
        j.writerFiles().map { _.getName }.toSet mustEqual
          Set("test.901", "test.8000", "test.1", "test.5005")
        j.readerFiles().map { _.getName }.toSet mustEqual
          Set("test.read.client1", "test.read.client2")
        j.close()

        new File(testFolder, "test.read.client1").delete()
        new File(testFolder, "test.read.client2").delete()
        val j2 = makeJournal("test")
        j2.readerFiles().map { _.getName }.toSet mustEqual
          Set("test.read.")
        j2.close()
      }
    }

    "erase old temporary files" in {
      List("test.1", "test.read.1", "test.read.1~~").foreach { name =>
        new File(testFolder, name).createNewFile()
      }

      val j = makeJournal("test")
      new File(testFolder, "test.read.1~~").exists() mustEqual false
    }

    "erase all journal files" in {
      List("test.1", "test.read.1", "test.read.1~~", "testbad").foreach { name =>
        new File(testFolder, name).createNewFile()
      }

      val j = makeJournal("test")
      j.erase()

      testFolder.list.toList mustEqual List("testbad")
    }

    "report size correctly" in {
      val jf1 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
      jf1.put(QueueItem(101L, Time.now, None, new Array[Byte](1000)))
      jf1.close()
      val jf2 = JournalFile.createWriter(new File(testFolder, "test.2"), null, Duration.MaxValue)
      jf2.put(QueueItem(102L, Time.now, None, new Array[Byte](1000)))
      jf2.close()

      val j = makeJournal("test")
      j.journalSize mustEqual 2050L
    }

    "fileForId" in {
      "startup" in {
        List(
          ("test.901", 901),
          ("test.8000", 8000),
          ("test.1", 1),
          ("test.5005", 5005)
        ).foreach { case (name, id) =>
          val jf = JournalFile.createWriter(new File(testFolder, name), null, Duration.MaxValue)
          jf.put(QueueItem(id, Time.now, None, new Array[Byte](5)))
          jf.close()
        }

        val j = makeJournal("test")
        j.fileInfoForId(1) mustEqual Some(FileInfo(new File(testFolder, "test.1"), 1, 1, 1, 5))
        j.fileInfoForId(0) mustEqual None
        j.fileInfoForId(555) mustEqual Some(FileInfo(new File(testFolder, "test.1"), 1, 1, 1, 5))
        j.fileInfoForId(900) mustEqual Some(FileInfo(new File(testFolder, "test.1"), 1, 1, 1, 5))
        j.fileInfoForId(901) mustEqual Some(FileInfo(new File(testFolder, "test.901"), 901, 901, 1, 5))
        j.fileInfoForId(902) mustEqual Some(FileInfo(new File(testFolder, "test.901"), 901, 901, 1, 5))
        j.fileInfoForId(6666) mustEqual Some(FileInfo(new File(testFolder, "test.5005"), 5005, 5005, 1, 5))
        j.fileInfoForId(9999) mustEqual Some(FileInfo(new File(testFolder, "test.8000"), 8000, 8000, 1, 5))
      }

      "during journal rotation" in {
        Time.withCurrentTimeFrozen { timeMutator =>
          val j = makeJournal("test", 1.kilobyte)
          j.put(new Array[Byte](512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(new Array[Byte](512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(new Array[Byte](512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(new Array[Byte](512), Time.now, None)
          timeMutator.advance(1.millisecond)
          j.put(new Array[Byte](512), Time.now, None)

          val file1 = new File(testFolder, "test." + 4.milliseconds.ago.inMilliseconds)
          val file2 = new File(testFolder, "test." + 3.milliseconds.ago.inMilliseconds)
          val file3 = new File(testFolder, "test." + 1.millisecond.ago.inMilliseconds)

          j.fileInfoForId(1) mustEqual Some(FileInfo(file1, 1, 2, 2, 1024))
          j.fileInfoForId(2) mustEqual Some(FileInfo(file1, 1, 2, 2, 1024))
          j.fileInfoForId(3) mustEqual Some(FileInfo(file2, 3, 4, 2, 1024))
          j.fileInfoForId(4) mustEqual Some(FileInfo(file2, 3, 4, 2, 1024))
          j.fileInfoForId(5) mustEqual Some(FileInfo(file3, 5, 0, 0, 0))
          j.close()
        }
      }
    }

    "checkpoint readers" in {
      List("test.read.client1", "test.read.client2").foreach { name =>
        val jf = JournalFile.createReader(new File(testFolder, name), null, Duration.MaxValue)
        jf.readHead(100L)
        jf.readDone(Array(102L))
        jf.close()
      }
      val jf = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
      jf.put(QueueItem(100L, Time.now, None, "hi".getBytes))
      jf.put(QueueItem(105L, Time.now, None, "hi".getBytes))
      jf.close()

      val j = makeJournal("test")
      j.reader("client1").commit(101L)
      j.reader("client2").commit(103L)
      j.checkpoint()
      j.close()

      JournalFile.openReader(new File(testFolder, "test.read.client1"), null, Duration.MaxValue).toList mustEqual List(
        JournalFile.Record.ReadHead(102L),
        JournalFile.Record.ReadDone(Array[Long]())
      )

      JournalFile.openReader(new File(testFolder, "test.read.client2"), null, Duration.MaxValue).toList mustEqual List(
        JournalFile.Record.ReadHead(100L),
        JournalFile.Record.ReadDone(Array(102L, 103L))
      )
    }

    "make new reader" in {
      val j = makeJournal("test")
      var r = j.reader("new")
      r.head = 100L
      r.commit(101L)
      r.checkpoint()
      j.close()

      JournalFile.openReader(new File(testFolder, "test.read.new"), null, Duration.MaxValue).toList mustEqual List(
        JournalFile.Record.ReadHead(101L),
        JournalFile.Record.ReadDone(Array[Long]())
      )
    }

    "create a default reader when no others exist" in {
      val j = makeJournal("test")
      j.close()

      JournalFile.openReader(new File(testFolder, "test.read."), null, Duration.MaxValue).toList mustEqual List(
        JournalFile.Record.ReadHead(0L),
        JournalFile.Record.ReadDone(Array[Long]())
      )
    }

    "convert the default reader to a named reader when one is created" in {
      val j = makeJournal("test")
      val reader = j.reader("")
      reader.head = 100L
      reader.checkpoint()

      new File(testFolder, "test.read.").exists mustEqual true

      j.reader("hello")
      new File(testFolder, "test.read.").exists mustEqual false
      new File(testFolder, "test.read.hello").exists mustEqual true

      JournalFile.openReader(new File(testFolder, "test.read.hello"), null, Duration.MaxValue).toList mustEqual List(
        JournalFile.Record.ReadHead(100L),
        JournalFile.Record.ReadDone(Array[Long]())
      )
    }

    "recover a reader" in {
      /*
       * rationale:
       * this can happen if a write journal is corrupted/truncated, losing the last few items, and
       * a reader had already written a state file out claiming to have finished processing the
       * items that are now lost.
       */
      "with a head id in the future" in {
        // create main journal
        val jf1 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
        jf1.put(QueueItem(390L, Time.now, None, "hi".getBytes))
        jf1.put(QueueItem(400L, Time.now, None, "hi".getBytes))
        jf1.close()

        // create readers with impossible ids
        val jf2 = JournalFile.createReader(new File(testFolder, "test.read.1"), null, Duration.MaxValue)
        jf2.readHead(402L)
        jf2.close()
        val jf3 = JournalFile.createReader(new File(testFolder, "test.read.2"), null, Duration.MaxValue)
        jf3.readHead(390L)
        jf3.readDone(Array(395L, 403L))
        jf3.close()

        val j = makeJournal("test")
        val r1 = j.reader("1")
        r1.head mustEqual 400L
        r1.doneSet mustEqual Set()
        val r2 = j.reader("2")
        r2.head mustEqual 390L
        r2.doneSet mustEqual Set(395L)
      }

      "with a head id that doesn't exist anymore" in {
        val jf1 = JournalFile.createWriter(new File(testFolder, "test.1"), null, Duration.MaxValue)
        jf1.put(QueueItem(800L, Time.now, None, "hi".getBytes))
        jf1.close()
        val jf2 = JournalFile.createReader(new File(testFolder, "test.read.1"), null, Duration.MaxValue)
        jf2.readHead(600L)
        jf2.readDone(Array[Long]())
        jf2.close()

        val j = makeJournal("test")
        j.reader("1").head mustEqual 799L
      }
    }

    "start with an empty journal" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)
        val j = makeJournal("test")
        val (item, future) = j.put("hi".getBytes, Time.now, None)()
        item.id mustEqual 1L
        j.close()

        val file = new File(testFolder, "test." + Time.now.inMilliseconds)
        val jf = JournalFile.openWriter(file, null, Duration.MaxValue)
        jf.readNext() mustEqual
          Some(JournalFile.Record.Put(QueueItem(1L, roundedTime, None, "hi".getBytes)))
        jf.close()
      }
    }

    "append new items to the end of the last journal" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)
        val file1 = new File(testFolder, "test.1")
        val jf1 = JournalFile.createWriter(file1, null, Duration.MaxValue)
        jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
        jf1.close()
        val file2 = new File(testFolder, "test.2")
        val jf2 = JournalFile.createWriter(file2, null, Duration.MaxValue)
        jf2.put(QueueItem(102L, Time.now, None, "102".getBytes))
        jf2.close()

        val j = makeJournal("test")
        val (item, future) = j.put("hi".getBytes, Time.now, None)()
        item.id mustEqual 103L
        j.close()

        val jf3 = JournalFile.openWriter(file2, null, Duration.MaxValue)
        jf3.readNext() mustEqual
          Some(JournalFile.Record.Put(QueueItem(102L, roundedTime, None, "102".getBytes)))
        jf3.readNext() mustEqual
          Some(JournalFile.Record.Put(QueueItem(103L, roundedTime, None, "hi".getBytes)))
        jf3.readNext() mustEqual None
        jf3.close()
      }
    }

    "truncate corrupted journal" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)

        // write 2 valid entries, but truncate the last one to make it corrupted.
        val file = new File(testFolder, "test.1")
        val jf = JournalFile.createWriter(file, null, Duration.MaxValue)
        jf.put(QueueItem(101L, Time.now, None, "101".getBytes))
        jf.put(QueueItem(102L, Time.now, None, "102".getBytes))
        jf.close()

        val raf = new RandomAccessFile(file, "rw")
        raf.setLength(file.length - 1)
        raf.close()

        val j = makeJournal("test")
        val (item, future) = j.put("hi".getBytes, Time.now, None)()
        item.id mustEqual 102L
        j.close()

        val jf2 = JournalFile.openWriter(file, null, Duration.MaxValue)
        jf2.readNext() mustEqual
          Some(JournalFile.Record.Put(QueueItem(101L, roundedTime, None, "101".getBytes)))
        jf2.readNext() mustEqual
          Some(JournalFile.Record.Put(QueueItem(102L, roundedTime, None, "hi".getBytes)))
        jf2.readNext() mustEqual None
        jf2.close()
      }
    }

    "rotate journal files" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        val j = makeJournal("test", 1.kilobyte)

        val time1 = addItem(j, 512)
        timeMutator.advance(1.millisecond)
        val time2 = addItem(j, 512)
        timeMutator.advance(1.millisecond)
        val time3 = addItem(j, 512)
        j.close()

        val file1 = new File(testFolder, "test." + time1)
        val file2 = new File(testFolder, "test." + time2)
        val defaultReader = new File(testFolder, "test.read.")
        testFolder.list.toList mustEqual List(file1, file2, defaultReader).map { _.getName() }
        JournalFile.openWriter(file1, null, Duration.MaxValue).toList mustEqual List(
          JournalFile.Record.Put(QueueItem(1L, Time.fromMilliseconds(time1), None, new Array[Byte](512))),
          JournalFile.Record.Put(QueueItem(2L, Time.fromMilliseconds(time2), None, new Array[Byte](512)))
        )
        JournalFile.openWriter(file2, null, Duration.MaxValue).toList mustEqual List(
          JournalFile.Record.Put(QueueItem(3L, Time.fromMilliseconds(time3), None, new Array[Byte](512)))
        )
      }
    }

    "clean up any dead files behind it" in {
      "when a client catches up" in {
        Time.withCurrentTimeFrozen { timeMutator =>
          val j = makeJournal("test", 1.kilobyte)
          val reader = j.reader("client")

          val time1 = addItem(j, 512)
          timeMutator.advance(1.millisecond)
          val time2 = addItem(j, 512)
          timeMutator.advance(1.millisecond)
          val time3 = addItem(j, 512)
          timeMutator.advance(1.millisecond)

          new File(testFolder, "test." + time1).exists mustEqual true
          new File(testFolder, "test." + time2).exists mustEqual true

          reader.commit(1L)
          reader.commit(2L)
          j.checkpoint()

          new File(testFolder, "test." + time1).exists mustEqual false
          new File(testFolder, "test." + time2).exists mustEqual true

          j.close()
        }
      }

      "when the journal moves to a new file" in {
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

          new File(testFolder, "test." + time0).exists mustEqual true
          new File(testFolder, "test." + time2).exists mustEqual true

          val time3 = addItem(j, 512)
          timeMutator.advance(1.millisecond)
          val time4 = addItem(j, 512)

          new File(testFolder, "test." + time0).exists mustEqual false
          new File(testFolder, "test." + time2).exists mustEqual true
          new File(testFolder, "test." + time4).exists mustEqual true
        }
      }
    }
  }
}
