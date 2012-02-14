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
import com.twitter.io.Files
import com.twitter.util._
import java.io._
import java.nio.ByteBuffer
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

object FileHelper {
  def readFile(file: File, limit: Int = Int.MaxValue) = {
    val f = new FileInputStream(file).getChannel
    val length = limit min file.length.toInt
    val bytes = new Array[Byte](length)
    val buffer = ByteBuffer.wrap(bytes)
    var x = 0
    do {
      x = f.read(buffer)
    } while (buffer.position < buffer.limit && x > 0)
    f.close()
    bytes
  }

  def writeFile(file: File, data: Array[Byte]) {
    val f = new FileOutputStream(file).getChannel
    val buffer = ByteBuffer.wrap(data)
    do {
      f.write(buffer)
    } while (buffer.position < buffer.limit)
    f.close()
  }

  def hex(bytes: Array[Byte]) = {
    bytes.map { b => "%x".format(b) }.mkString(" ")
  }

  def unhex(s: String): Array[Byte] = {
    s.split(" ").map { x => Integer.parseInt(x, 16).toByte }.toArray
  }
}

class JournalFileSpec extends ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
  import FileHelper._

  describe("JournalFile") {
    describe("put") {
      val putData = "27 64 26 3 86 5 0 0 0 1 0 0 0 64 0 0 0 0 0 0 0 ff 0 0 0 0 0 0 0 68 65 6c 6c 6f"
      val putItem = QueueItem(100, Time.fromMilliseconds(255), None, ByteBuffer.wrap("hello".getBytes), 1)

      it("write") {
        val testFile = new File(testFolder, "a1")
        val j = JournalFile.create(testFile, null, Duration.MaxValue, 16.kilobytes)
        j.put(putItem)
        j.close()
        assert(hex(readFile(testFile)) === putData)
      }

      it("read") {
        val testFile = new File(testFolder, "a1")
        writeFile(testFile, unhex(putData))
        val j = JournalFile.open(testFile)
        assert(j.readNext() === Some(Record.Put(putItem)))
        j.close()
      }

      it("read corrupted") {
        val testFile = new File(testFolder, "a1")
        val data = unhex(putData)
        (0 until data.size).foreach { size =>
          writeFile(testFile, data.slice(0, size))
          if (size != 4) {
            intercept[IOException] {
              val j = JournalFile.open(testFile)
              try {
                j.readNext()
              } finally {
                j.close()
              }
            }
          }
        }
      }

      it("read several") {
        val headerData = "27 64 26 3"
        val putData1 = "86 5 0 0 0 3 0 0 0 64 0 0 0 0 0 0 0 ff 0 0 0 0 0 0 0 68 65 6c 6c 6f"
        val putItem1 = QueueItem(100, Time.fromMilliseconds(255), None, ByteBuffer.wrap("hello".getBytes), 3)
        val putData2 = "86 7 0 0 0 2 0 0 0 65 0 0 0 0 0 0 0 84 3 0 0 0 0 0 0 67 6f 6f 64 62 79 65"
        val putItem2 = QueueItem(101, Time.fromMilliseconds(900), None, ByteBuffer.wrap("goodbye".getBytes), 2)

        val testFile = new File(testFolder, "a1")
        writeFile(testFile, unhex(headerData + " " + putData1 + " " + putData2))
        val j = JournalFile.open(testFile)
        assert(j.readNext() === Some(Record.Put(putItem1)))
        assert(j.readNext() === Some(Record.Put(putItem2)))
        assert(j.readNext() === None)
        j.close()
      }

      it("append") {
        val headerData = "27 64 26 3"
        val putData1 = "86 5 0 0 0 0 1 0 0 64 0 0 0 0 0 0 0 ff 0 0 0 0 0 0 0 68 65 6c 6c 6f"
        val putItem1 = QueueItem(100, Time.fromMilliseconds(255), None, ByteBuffer.wrap("hello".getBytes), 256)
        val putData2 = "86 7 0 0 0 0 0 0 0 65 0 0 0 0 0 0 0 84 3 0 0 0 0 0 0 67 6f 6f 64 62 79 65"
        val putItem2 = QueueItem(101, Time.fromMilliseconds(900), None, ByteBuffer.wrap("goodbye".getBytes), 0)

        val testFile = new File(testFolder, "a1")
        val j = JournalFile.create(testFile, null, Duration.MaxValue, 16.kilobytes)
        j.put(putItem1)
        j.close()
        val j2 = JournalFile.append(testFile, null, Duration.MaxValue, 16.kilobytes)
        j2.put(putItem2)
        j2.close()

        assert(hex(readFile(testFile)) === headerData + " " + putData1 + " " + putData2)
      }
    }

    it("refuse to deal with items too large") {
      val testFile = new File(testFolder, "a1")
      writeFile(testFile, unhex("27 64 26 3 81 ff ff ff 7f"))

      val j = JournalFile.open(testFile)
      val e = intercept[CorruptedJournalException] { j.readNext() }
      assert(e.message === "item too large")
      j.close()

      val item = QueueItem(100, Time.fromMilliseconds(0), None,
        ByteBuffer.allocate(Record.LARGEST_DATA.inBytes.toInt + 1))
      val j2 = JournalFile.create(testFile, null, Duration.MaxValue, 16.kilobytes)
      val e2 = intercept[IOException] { j2.put(item) }
      assert(e2.getMessage === "item too large")
      j2.close()
    }
  }
}
