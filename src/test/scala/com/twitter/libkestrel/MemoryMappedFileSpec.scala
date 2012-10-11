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
import com.twitter.util.StorageUnit
import java.io.{File, IOException}
import java.nio.ByteBuffer
import org.scalatest.FunSpec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class MemoryMappedFileSpec extends FunSpec with ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging {
  import FileHelper._

  val mmapSize = 16.kilobytes

  def writeMappedFile(file: File, size: StorageUnit, data: Int) {
    val mmap = MemoryMappedFile.map(file, size)
    writeMappedFile(mmap, data)
  }

  def writeMappedFile(mmap: MemoryMappedFile, data: Int, limitOption: Option[StorageUnit] = None) {
    var limit = limitOption map { _.inBytes.toInt } orElse { Some(Int.MaxValue) } get
    val buffer = mmap.buffer()
    try {
      while (buffer.remaining >= 4 && limit > 0) {
        buffer.putInt(data)
        limit -= 4
      }
      mmap.force()
    } finally {
      mmap.close()
    }
  }

  describe("MemoryMappedFile") {
    describe("read/write") {
      it("mapping can be created") {
        val testFile = new File(testFolder, "create")
        val mmap = MemoryMappedFile.map(testFile, mmapSize)
        try {
          testFile.exists should equal(true)
          testFile.length should equal(mmapSize.inBytes)
        } finally {
          mmap.close()
        }
      }

      it("mapping can be modified") {
        val testFile = new File(testFolder, "modify")
        writeMappedFile(testFile, mmapSize, 0xdeadbeef)

        val bytes = readFile(testFile)
        val readBuffer = ByteBuffer.wrap(bytes)
        readBuffer.remaining should equal(mmapSize.inBytes)
        while (readBuffer.remaining >= 4) readBuffer.getInt() should equal(0xdeadbeef)
      }

      it("truncates existing files") {
        val testFile = new File(testFolder, "truncate")
        writeMappedFile(testFile, mmapSize, 0xdeadbeef)
        testFile.length should equal(mmapSize.inBytes)

        val mmap = MemoryMappedFile.map(testFile, 8.kilobytes, true)
        writeMappedFile(mmap, 0xabadcafe)

        val bytes = readFile(testFile)
        val readBuffer = ByteBuffer.wrap(bytes)
        readBuffer.remaining should equal(8.kilobytes.inBytes)
        while (readBuffer.remaining >= 4) readBuffer.getInt() should equal(0xabadcafe)
      }

      it("leaves unwritten space zeroed") {
        val testFile = new File(testFolder, "zero")
        val mmap = MemoryMappedFile.map(testFile, mmapSize)
        writeMappedFile(mmap, 0xdeadbeef, Some(8.kilobytes))

        val bytes = readFile(testFile)
        val readBuffer = ByteBuffer.wrap(bytes)
        readBuffer.remaining should equal(mmapSize.inBytes)
        val deadbeefSlice = readBuffer.slice()
        deadbeefSlice.limit(8.kilobytes.inBytes.toInt)
        while (deadbeefSlice.remaining >= 4) deadbeefSlice.getInt() should equal(0xdeadbeef)

        val zeroedSlice = readBuffer.slice()
        zeroedSlice.position(8.kilobytes.inBytes.toInt)
        while (zeroedSlice.remaining >= 4) zeroedSlice.getInt() should equal(0)
      }

      it("should return different mapping for a file opened at different times") {
        val testFile = new File(testFolder, "open-close-open")

        val mmap1 = MemoryMappedFile.map(testFile, mmapSize)
        mmap1.close()

        val mmap2 = MemoryMappedFile.map(testFile, mmapSize)

        try {
          mmap1 should not be theSameInstanceAs (mmap2)
        } finally {
          mmap2.close()
        }
      }

      it("should throw if the same file is mapped twice") {
        val testFile = new File(testFolder, "multi-open")
        val mmap = MemoryMappedFile.map(testFile, mmapSize)

        try {
          evaluating { MemoryMappedFile.map(testFile, 8.kilobytes) } should produce [IOException]
        } finally {
          mmap.close()
        }
      }

      it("should throw if buffer is accessed after close") {
        val testFile = new File(testFolder, "closed")
        val mmap = MemoryMappedFile.map(testFile, mmapSize)
        mmap.close()

        evaluating { mmap.buffer() } should produce [NullPointerException]
      }

      it("should throw when closing the same file multiple times") {
        val testFile = new File(testFolder, "double-close")
        val mmap = MemoryMappedFile.map(testFile, mmapSize)
        mmap.close()

        evaluating { mmap.close() } should produce [IOException]
      }

      it("should preventing mapping files larger than 2 GB") {
        val testFile = new File(testFolder, "huge")
        val size = (2.gigabytes.inBytes + 1).bytes
        evaluating { MemoryMappedFile.map(testFile, size) } should produce [IOException]
      }
    }

    describe("read-only maps") {
      def createTestFile(name: String, size: StorageUnit): File = {
        val testFile = new File(testFolder, name)
        val bytes = new Array[Byte](size.inBytes.toInt)
        Some(ByteBuffer.wrap(bytes)).foreach { b => while (b.remaining >= 4) b.putInt(0xdeadbeef) }
        writeFile(testFile, bytes)
        testFile
      }

      it("can be created and read") {
        val testFile = createTestFile("create-ro", mmapSize)
        val mmap = MemoryMappedFile.readOnlyMap(testFile)
        val buffer = mmap.buffer()
        try {
          buffer.remaining should equal(mmapSize.inBytes)
          while (buffer.remaining >= 4) buffer.getInt() should equal(0xdeadbeef)
        } finally {
          mmap.close()
        }
      }

      it("should not return the same mapping for a file mapped by multiple readers") {
        val testFile = createTestFile("create-ro-multi", mmapSize)
        val mmap1 = MemoryMappedFile.readOnlyMap(testFile)
        val mmap2 = MemoryMappedFile.readOnlyMap(testFile)

        try {
          mmap1 should not be theSameInstanceAs (mmap2)
        } finally {
          mmap1.close()
          mmap2.close()
        }
      }

      it("should throw if buffer is accessed after close") {
        val testFile = createTestFile("closed-ro", mmapSize)
        val mmap = MemoryMappedFile.readOnlyMap(testFile)
        mmap.close()

        evaluating { mmap.buffer() } should produce [NullPointerException]
      }

      it("should throw when closing the same file multiple times") {
        val testFile = createTestFile("double-close-ro", mmapSize)
        val mmap = MemoryMappedFile.readOnlyMap(testFile)
        mmap.close()

        evaluating { mmap.close() } should produce [IOException]
      }

      it("should allow a file to be mapped for writing and reading simultaneously") {
        val testFile = createTestFile("read-write", mmapSize)
        val writeMapping = MemoryMappedFile.map(testFile, mmapSize)
        try {
          val writeBuffer = writeMapping.buffer()
          val readMapping = MemoryMappedFile.readOnlyMap(testFile)
          try {
            val readBuffer = readMapping.buffer()
            readBuffer.getInt(0) should equal(0xdeadbeef)

            writeBuffer.putInt(0x55555555)
            writeMapping.force()

            readBuffer.getInt(0) should equal(0x55555555)
          } finally {
            readMapping.close()
          }
        } finally {
          writeMapping.close()
        }
      }

      it("should throw if the file is opened for read before write") {
        val testFile = createTestFile("read-before-write", mmapSize)
        val readMapping = MemoryMappedFile.readOnlyMap(testFile)
        try {
          evaluating { MemoryMappedFile.map(testFile, mmapSize) } should produce [IOException]
        } finally {
          readMapping.close()
        }
      }
    }
  }
}
