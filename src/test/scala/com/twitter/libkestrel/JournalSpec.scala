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

import com.twitter.util._
import java.io._
import java.nio.ByteBuffer
import org.specs.Specification

class JournalSpec extends Specification with TempFolder {
  "Journal" should {
    "find reader/writer files" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { timeMutator =>
          List(
            "test.901", "test.8000", "test.3leet", "test.read.client1", "test.read.client2",
            "test.readmenot", "test.1", "test.5005", "test.read.client1~~"
          ).foreach { name =>
            new File(folderName, name).createNewFile()
          }

          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          j.writerFiles().map { _.getName }.toSet mustEqual
            Set("test.901", "test.8000", "test.1", "test.5005", "test." + Time.now.inMilliseconds)
          j.readerFiles().map { _.getName }.toSet mustEqual
            Set("test.read.client1", "test.read.client2")
        }
      }
    }

    "fileForId" in {
      withTempFolder {
        List(
          ("test.901", 901),
          ("test.8000", 8000),
          ("test.1", 1),
          ("test.5005", 5005)
        ).foreach { case (name, id) =>
          val jf = JournalFile.createWriter(new File(folderName, name), null, Duration.MaxValue)
          jf.put(QueueItem(id, Time.now, None, new Array[Byte](1)))
          jf.close()
        }

        val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        j.fileForId(1) mustEqual Some(new File(folderName, "test.1"))
        j.fileForId(0) mustEqual None
        j.fileForId(555) mustEqual Some(new File(folderName, "test.1"))
        j.fileForId(900) mustEqual Some(new File(folderName, "test.1"))
        j.fileForId(901) mustEqual Some(new File(folderName, "test.901"))
        j.fileForId(902) mustEqual Some(new File(folderName, "test.901"))
        j.fileForId(6666) mustEqual Some(new File(folderName, "test.5005"))
        j.fileForId(9999) mustEqual Some(new File(folderName, "test.8000"))
      }
    }

    "checkpoint readers" in {
      withTempFolder {
        List("test.read.client1", "test.read.client2").foreach { name =>
          val jf = JournalFile.createReader(new File(folderName, name), null, Duration.MaxValue)
          jf.readHead(100L)
          jf.readDone(Array(102L))
          jf.close()
        }

        val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        j.reader("client1", 0L).commit(101L)
        j.reader("client2", 0L).commit(103L)
        j.checkpoint()
        j.close()

        JournalFile.openReader(new File(folderName, "test.read.client1"), null, Duration.MaxValue).toList mustEqual List(
          JournalFile.Record.ReadHead(102L),
          JournalFile.Record.ReadDone(Array[Long]())
        )

        JournalFile.openReader(new File(folderName, "test.read.client2"), null, Duration.MaxValue).toList mustEqual List(
          JournalFile.Record.ReadHead(100L),
          JournalFile.Record.ReadDone(Array(102L, 103L))
        )
      }
    }

    "make new reader" in {
      withTempFolder {
        val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        j.reader("new", 100L).commit(101L)
        j.reader("new", 100L).checkpoint()
        j.close()

        JournalFile.openReader(new File(folderName, "test.read.new"), null, Duration.MaxValue).toList mustEqual List(
          JournalFile.Record.ReadHead(101L),
          JournalFile.Record.ReadDone(Array[Long]())
        )
      }
    }

    "start with an empty journal" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { timeMutator =>
          val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)
          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          j.put("hi".getBytes, Time.now, None)() mustEqual 1L
          j.close()

          val file = new File(folderName, "test." + Time.now.inMilliseconds)
          val jf = JournalFile.openWriter(file, null, Duration.MaxValue)
          jf.readNext() mustEqual
            Some(JournalFile.Record.Put(QueueItem(1L, roundedTime, None, "hi".getBytes)))
          jf.close()
        }
      }
    }

    "append new items to the end of the last journal" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { timeMutator =>
          val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)
          val file1 = new File(folderName, "test.1")
          val jf1 = JournalFile.createWriter(file1, null, Duration.MaxValue)
          jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
          jf1.close()
          val file2 = new File(folderName, "test.2")
          val jf2 = JournalFile.createWriter(file2, null, Duration.MaxValue)
          jf2.put(QueueItem(102L, Time.now, None, "102".getBytes))
          jf2.close()

          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          j.put("hi".getBytes, Time.now, None)() mustEqual 103L
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
    }

    "truncate corrupted journal" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { timeMutator =>
          val roundedTime = Time.fromMilliseconds(Time.now.inMilliseconds)

          // write 2 valid entries, but truncate the last one to make it corrupted.
          val file = new File(folderName, "test.1")
          val jf = JournalFile.createWriter(file, null, Duration.MaxValue)
          jf.put(QueueItem(101L, Time.now, None, "101".getBytes))
          jf.put(QueueItem(102L, Time.now, None, "102".getBytes))
          jf.close()

          val raf = new RandomAccessFile(file, "rw")
          raf.setLength(file.length - 1)
          raf.close()

          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          j.put("hi".getBytes, Time.now, None)() mustEqual 102L
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
    }
  }

  "Journal#Reader" should {
    "write a checkpoint" in {
      withTempFolder {
        val file = new File(folderName, "a1")
        val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        val reader = new j.Reader(file)
        reader.head = 123L
        reader.commit(125L)
        reader.commit(130L)
        reader.checkpoint()

        JournalFile.openReader(file, null, Duration.MaxValue).toList mustEqual List(
          JournalFile.Record.ReadHead(123L),
          JournalFile.Record.ReadDone(Array(125L, 130L))
        )
      }
    }

    "read a checkpoint" in {
      withTempFolder {
        val file = new File(folderName, "a1")
        val jf = JournalFile.createReader(file, null, Duration.MaxValue)
        jf.readHead(900L)
        jf.readDone(Array(902L, 903L))
        jf.close()

        val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        val reader = new j.Reader(file)
        reader.readState()
        reader.head mustEqual 900L
        reader.doneSet.toList.sorted mustEqual List(902L, 903L)
      }
    }

    "track committed items" in {
      withTempFolder {
        val file = new File(folderName, "a1")
        val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        val reader = new j.Reader(file)
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
    }

    "read-behind" in {
      "start" in {
        withTempFolder {
          val jf = JournalFile.createWriter(new File(folderName, "test.1"), null, Duration.MaxValue)
          jf.put(QueueItem(100L, Time.now, None, "100".getBytes))
          jf.put(QueueItem(101L, Time.now, None, "101".getBytes))
          jf.close()

          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          val reader = j.reader("client1", 100L)
          reader.startReadBehind(100L)
          val item = reader.nextReadBehind()
          item.id mustEqual 101L
          new String(item.data) mustEqual "101"
        }
      }

      "start at file edge" in {
        withTempFolder {
          val jf1 = JournalFile.createWriter(new File(folderName, "test.1"), null, Duration.MaxValue)
          jf1.put(QueueItem(100L, Time.now, None, "100".getBytes))
          jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
          jf1.close()

          val jf2 = JournalFile.createWriter(new File(folderName, "test.2"), null, Duration.MaxValue)
          jf2.put(QueueItem(102L, Time.now, None, "102".getBytes))
          jf2.put(QueueItem(103L, Time.now, None, "103".getBytes))
          jf2.close()

          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          val reader = j.reader("client", 101L)
          reader.startReadBehind(101L)
          reader.nextReadBehind().id mustEqual 102L
        }
      }

      "across journal files" in {
        withTempFolder {
          val jf1 = JournalFile.createWriter(new File(folderName, "test.1"), null, Duration.MaxValue)
          jf1.put(QueueItem(100L, Time.now, None, "100".getBytes))
          jf1.put(QueueItem(101L, Time.now, None, "101".getBytes))
          jf1.close()

          val jf2 = JournalFile.createWriter(new File(folderName, "test.2"), null, Duration.MaxValue)
          jf2.put(QueueItem(102L, Time.now, None, "102".getBytes))
          jf2.put(QueueItem(103L, Time.now, None, "103".getBytes))
          jf2.close()

          val jf3 = JournalFile.createWriter(new File(folderName, "test.3"), null, Duration.MaxValue)
          jf3.put(QueueItem(104L, Time.now, None, "104".getBytes))
          jf3.put(QueueItem(105L, Time.now, None, "105".getBytes))
          jf3.close()

          val j = new Journal(new File(folderName), "test", null, Duration.MaxValue)
          val reader = j.reader("client", 102L)
          reader.startReadBehind(102L)
          reader.nextReadBehind().id mustEqual 103L

          val item = reader.nextReadBehind()
          item.id mustEqual 104L
          new String(item.data) mustEqual "104"
        }
      }
    }
  }
}
