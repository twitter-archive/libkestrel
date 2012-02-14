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

import java.io.{File, FileInputStream, IOException}
import java.nio.ByteBuffer

object BookmarkFile {
  val HEADER_BOOKMARK = 0x263C2603

  def open(file: File) = {
    val reader = new BookmarkFileReader(file)
    if (reader.open() != HEADER_BOOKMARK) {
      reader.close()
      throw new IOException("Not a bookmark file")
    }
    reader
  }

  def create(file: File) = {
    val writer = new BookmarkFileWriter(file)
    writer.create(HEADER_BOOKMARK)
    writer
  }
}

class BookmarkFileReader(val file: File) extends RecordReader {
  var reader: ByteBuffer = null

  def open(): Int = {
    val channel = new FileInputStream(file).getChannel
    if (channel.size > Int.MaxValue.toLong) throw new IOException("Bookmark file too large")

    reader = ByteBuffer.allocate(channel.size.toInt)
    channel.read(reader)
    channel.close
    reader.flip()

    readMagic()
  }

  def close() {
    reader = null
  }
}

class BookmarkFileWriter(val file: File) extends RecordWriter {
  protected[this] val writer = new SimpleFile(file)

  def create(magic: Int) {
    writeMagic(magic)
  }
}
