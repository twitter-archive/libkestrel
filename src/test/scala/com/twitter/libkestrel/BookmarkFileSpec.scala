/*
 * Copyright 2012 Twitter, Inc.
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
import org.scalatest.{AbstractSuite, FunSpec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class BookmarkFileSpec extends FunSpec with ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
  import FileHelper._

  describe("BookmarkFile") {
    describe("readHead") {
      val readHeadData = "26 3c 26 3 2 ff 1 0 0 0 0 0 0"
      val readHead = Record.ReadHead(511)

      it("write") {
        val testFile = new File(testFolder, "a1")
        val j = BookmarkFile.create(testFile)
        j.readHead(511)
        j.close()
        assert(hex(readFile(testFile)) === readHeadData)
      }

      it("read") {
        val testFile = new File(testFolder, "a1")
        writeFile(testFile, unhex(readHeadData))
        val j = BookmarkFile.open(testFile)
        assert(j.readNext() === Some(readHead))
      }

      it("read corrupted") {
        val testFile = new File(testFolder, "a1")
        val data = unhex(readHeadData)
        (0 until data.size).foreach { size =>
          writeFile(testFile, data.slice(0, size))
          if (size != 4) {
            intercept[IOException] {
              BookmarkFile.open(testFile).readNext()
            }
          }
        }
      }
    }

    describe("readDone") {
      val readDoneData = "26 3c 26 3 91 18 0 0 0 a 0 0 0 0 0 0 0 14 0 0 0 0 0 0 0 1e 0 0 0 0 0 0 0"
      val readDone = Record.ReadDone(Array(10, 20, 30))

      it("write") {
        val testFile = new File(testFolder, "a1")
        val j = BookmarkFile.create(testFile)
        j.readDone(List(10, 20, 30))
        j.close()
        assert(hex(readFile(testFile)) === readDoneData)
      }

      it("read") {
        val testFile = new File(testFolder, "a1")
        writeFile(testFile, unhex(readDoneData))
        val j = BookmarkFile.open(testFile)
        assert(j.readNext() === Some(readDone))
      }

      it("read corrupted") {
        val testFile = new File(testFolder, "a1")
        val data = unhex(readDoneData)
        (0 until data.size).foreach { size =>
          writeFile(testFile, data.slice(0, size))
          if (size != 4) {
            intercept[IOException] {
              BookmarkFile.open(testFile).readNext()
            }
          }
        }
      }
    }

    it("whole read file") {
      val fileData = "26 3c 26 3 91 8 0 0 0 2 0 1 0 0 0 0 0 2 0 0 1 0 0 0 0 0"
      val testFile = new File(testFolder, "a1")
      writeFile(testFile, unhex(fileData))

      val j = BookmarkFile.open(testFile)
      assert(j.readNext() === Some(Record.ReadDone(Array(65538))))
      assert(j.readNext() === Some(Record.ReadHead(65536)))
      assert(j.readNext() === None)
    }

    it("be okay with commands it doesn't know") {
      val fileData = "26 3c 26 3 f3 4 0 0 0 1 1 1 1 2 2 2 2 ff ff ff ff 2 0 40 0 0 0 0 0 0"
      val testFile = new File(testFolder, "a1")
      writeFile(testFile, unhex(fileData))

      val j = BookmarkFile.open(testFile)
      assert(j.readNext() === Some(Record.Unknown(15)))
      assert(j.readNext() === Some(Record.ReadHead(16384)))
      assert(j.readNext() === None)
    }
  }
}
