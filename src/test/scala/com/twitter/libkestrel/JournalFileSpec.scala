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

class JournalFileSpec extends Specification with TempFolder {
  def readFile(file: File) = {
    val f = new FileInputStream(file).getChannel
    val bytes = new Array[Byte](file.length.toInt)
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

  "JournalFile" should {
    "put" in {
      val putData = "27 64 26 3 85 5 0 0 0 64 0 0 0 0 0 0 0 ff 0 0 0 0 0 0 0 68 65 6c 6c 6f"
      val putItem = QueueItem(100, Time.fromMilliseconds(255), None, "hello".getBytes)

      "write" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          val j = JournalFile.createWriter(testFile, null, Duration.MaxValue)
          j.put(putItem)
          j.close()
          hex(readFile(testFile)) mustEqual putData
        }
      }

      "read" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          writeFile(testFile, unhex(putData))
          val j = JournalFile.openWriter(testFile, null, Duration.MaxValue)
          j.readNext() mustEqual Some(JournalFile.Record.Put(putItem))
        }
      }

      "read corrupted" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          val data = unhex(putData)
          (0 until data.size).foreach { size =>
            writeFile(testFile, data.slice(0, size))
            if (size != 4) {
              JournalFile.openWriter(testFile, null, Duration.MaxValue).readNext() must throwA[IOException]
            }
          }
        }
      }

      "read several" in {
        withTempFolder {
          val headerData = "27 64 26 3"
          val putData1 = "85 5 0 0 0 64 0 0 0 0 0 0 0 ff 0 0 0 0 0 0 0 68 65 6c 6c 6f"
          val putItem1 = QueueItem(100, Time.fromMilliseconds(255), None, "hello".getBytes)
          val putData2 = "85 7 0 0 0 65 0 0 0 0 0 0 0 84 3 0 0 0 0 0 0 67 6f 6f 64 62 79 65"
          val putItem2 = QueueItem(101, Time.fromMilliseconds(900), None, "goodbye".getBytes)

          val testFile = new File(folderName, "a1")
          writeFile(testFile, unhex(headerData + " " + putData1 + " " + putData2))
          val j = JournalFile.openWriter(testFile, null, Duration.MaxValue)
          j.readNext() mustEqual Some(JournalFile.Record.Put(putItem1))
          j.readNext() mustEqual Some(JournalFile.Record.Put(putItem2))
          j.readNext() mustEqual None
        }
      }

      "append" in {
        withTempFolder {
          val headerData = "27 64 26 3"
          val putData1 = "85 5 0 0 0 64 0 0 0 0 0 0 0 ff 0 0 0 0 0 0 0 68 65 6c 6c 6f"
          val putItem1 = QueueItem(100, Time.fromMilliseconds(255), None, "hello".getBytes)
          val putData2 = "85 7 0 0 0 65 0 0 0 0 0 0 0 84 3 0 0 0 0 0 0 67 6f 6f 64 62 79 65"
          val putItem2 = QueueItem(101, Time.fromMilliseconds(900), None, "goodbye".getBytes)

          val testFile = new File(folderName, "a1")
          val j = JournalFile.createWriter(testFile, null, Duration.MaxValue)
          j.put(putItem1)
          j.close()
          val j2 = JournalFile.append(testFile, null, Duration.MaxValue)
          j2.put(putItem2)
          j2.close()

          hex(readFile(testFile)) mustEqual headerData + " " + putData1 + " " + putData2
        }
      }
    }

    "readHead" in {
      val readHeadData = "26 3c 26 3 2 ff 1 0 0 0 0 0 0"
      val readHead = JournalFile.Record.ReadHead(511)

      "write" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          val j = JournalFile.createReader(testFile, null, Duration.MaxValue)
          j.readHead(511)
          j.close()
          hex(readFile(testFile)) mustEqual readHeadData
        }
      }

      "read" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          writeFile(testFile, unhex(readHeadData))
          val j = JournalFile.openReader(testFile, null, Duration.MaxValue)
          j.readNext() mustEqual Some(readHead)
        }
      }

      "read corrupted" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          val data = unhex(readHeadData)
          (0 until data.size).foreach { size =>
            writeFile(testFile, data.slice(0, size))
            if (size != 4) {
              JournalFile.openReader(testFile, null, Duration.MaxValue).readNext() must throwA[IOException]
            }
          }
        }
      }
    }

    "readDone" in {
      val readDoneData = "26 3c 26 3 91 18 0 0 0 a 0 0 0 0 0 0 0 14 0 0 0 0 0 0 0 1e 0 0 0 0 0 0 0"
      val readDone = JournalFile.Record.ReadDone(Array(10, 20, 30))

      "write" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          val j = JournalFile.createReader(testFile, null, Duration.MaxValue)
          j.readDone(List(10, 20, 30))
          j.close()
          hex(readFile(testFile)) mustEqual readDoneData
        }
      }

      "read" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          writeFile(testFile, unhex(readDoneData))
          val j = JournalFile.openReader(testFile, null, Duration.MaxValue)
          j.readNext() mustEqual Some(readDone)
        }
      }

      "read corrupted" in {
        withTempFolder {
          val testFile = new File(folderName, "a1")
          val data = unhex(readDoneData)
          (0 until data.size).foreach { size =>
            writeFile(testFile, data.slice(0, size))
            if (size != 4) {
              JournalFile.openReader(testFile, null, Duration.MaxValue).readNext() must throwA[IOException]
            }
          }
        }
      }
    }

    "whole read file" in {
      withTempFolder {
        val fileData = "26 3c 26 3 91 8 0 0 0 2 0 1 0 0 0 0 0 2 0 0 1 0 0 0 0 0"
        val testFile = new File(folderName, "a1")
        writeFile(testFile, unhex(fileData))

        val j = JournalFile.openReader(testFile, null, Duration.MaxValue)
        j.readNext() mustEqual Some(JournalFile.Record.ReadDone(Array(65538)))
        j.readNext() mustEqual Some(JournalFile.Record.ReadHead(65536))
        j.readNext() mustEqual None
      }
    }

    "refuse to deal with items too large" in {
      withTempFolder {
        val testFile = new File(folderName, "a1")
        writeFile(testFile, unhex("27 64 26 3 85 ff ff ff 7f"))

        val j = JournalFile.openWriter(testFile, null, Duration.MaxValue)
        j.readNext() must throwA[IOException].like {
          case e: CorruptedJournalException => e.message == "item too large"
        }

        val item = QueueItem(100, Time.fromMilliseconds(0), None,
          new Array[Byte](JournalFile.LARGEST_DATA.inBytes.toInt + 1))
        val j2 = JournalFile.createWriter(testFile, null, Duration.MaxValue)
        j2.put(item) must throwA[IOException].like {
          case e: IOException => e.getMessage == "item too large"
        }
      }
    }

    "be okay with commands it doesn't know" in {
      withTempFolder {
        val fileData = "26 3c 26 3 f1 4 0 0 0 ff ff ff ff 2 0 40 0 0 0 0 0 0"
        val testFile = new File(folderName, "a1")
        writeFile(testFile, unhex(fileData))

        val j = JournalFile.openReader(testFile, null, Duration.MaxValue)
        j.readNext() mustEqual Some(JournalFile.Record.Unknown(15))
        j.readNext() mustEqual Some(JournalFile.Record.ReadHead(16384))
        j.readNext() mustEqual None
      }
    }
  }
}
