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

import com.twitter.util.Time
import java.io.File
import java.nio.{ByteBuffer, ByteOrder}

abstract class RecordReader extends Iterable[Record] {
  import Record._

  def file: File
  def reader: ByteBuffer

  def readMagic(): Int = {
    if (reader.remaining < 4) throw new CorruptedJournalException(0, file, "No header in journal file: " + file)
    reader.order(ByteOrder.BIG_ENDIAN)
    val magic = reader.getInt
    reader.order(ByteOrder.LITTLE_ENDIAN)
    magic
  }

  def readNext(): Option[Record] = {
    val lastPosition = reader.position

    if (reader.remaining < 1) {
      return None
    }
    val commandAndHeaderSize = reader.get
    if (commandAndHeaderSize == 0) {
      reader.position(lastPosition)
      return None
    }

    val command = (commandAndHeaderSize >> 4) & 0xf
    val headerSize = (commandAndHeaderSize & 0xf)

    if (reader.remaining < headerSize * 4) {
      throw new CorruptedJournalException(lastPosition, file, "truncated header: %d left".format(reader.remaining))
    }

    var dataBuffer: ByteBuffer = null
    var dataSize = 0
    if (command >= 8) {
      dataSize = reader.getInt
      if (dataSize > LARGEST_DATA.inBytes) {
        throw new CorruptedJournalException(lastPosition, file, "item too large")
      }

      val remainingHeaderSize = (headerSize - 1) * 4
      if (reader.remaining < dataSize + remainingHeaderSize) {
        throw new CorruptedJournalException(lastPosition, file, "truncated entry")
      }
    }

    Some(command match {
      case READ_HEAD => {
        if (reader.remaining < 8) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted READ_HEAD")
        }
        val id = reader.getLong()
        Record.ReadHead(id)
      }
      case PUT => {
        if (reader.remaining < 20) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted PUT")
        }
        val errorCount = reader.getInt()
        val id = reader.getLong()
        val addTime = reader.getLong()
        val expireTime = if (headerSize > 6) Some(reader.getLong()) else None

        // record current position; modify limit to the end of data; make slice; restore reader
        reader.limit(reader.position + dataSize)
        dataBuffer = reader.slice()
        reader.limit(reader.capacity)

        // advance reader past data
        reader.position(reader.position + dataSize)
        Record.Put(QueueItem(id, Time.fromMilliseconds(addTime), expireTime.map { t =>
          Time.fromMilliseconds(t)
        }, dataBuffer, errorCount))
      }
      case READ_DONE => {
        if (dataSize % 8 != 0) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted READ_DONE")
        }
        val ids = new Array[Long](dataSize / 8)
        for (i <- 0 until ids.size) { ids(i) = reader.getLong() }
        Record.ReadDone(ids)
      }
      case _ => {
        // this is okay. we can skip the ones we don't know.
        reader.position(lastPosition + headerSize * 4 + dataSize + 1)
        Record.Unknown(command)
      }
    })
  }

  def iterator: Iterator[Record] = {
    def next(): Stream[Record] = {
      readNext() match {
        case Some(entry) => new Stream.Cons(entry, next())
        case None => Stream.Empty
      }
    }
    next().iterator
  }

  def position = reader.position
}

