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
import com.twitter.util._
import java.io.{File, FileInputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel

case class CorruptedJournalException(lastValidPosition: Long, file: File, message: String) extends IOException(message)

object JournalFile {
  private[this] val BUFFER_SIZE = 128

  val HEADER_WRITER = 0x27642603
  val HEADER_READER = 0x263C2603

  // throw an error if any queue item is larger than this:
  val LARGEST_DATA = 16.megabytes

  private[this] def newBuffer(size: Int) = {
    val buffer = new Array[Byte](size)
    val byteBuffer = ByteBuffer.wrap(buffer)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer
  }

  private[this] val localBuffer = new ThreadLocal[ByteBuffer] {
    override def initialValue: ByteBuffer = newBuffer(BUFFER_SIZE)
  }

  def buffer: ByteBuffer = buffer(BUFFER_SIZE)

  // thread-local for buffering journal items
  def buffer(size: Int): ByteBuffer = {
    val b = localBuffer.get()
    if (b.limit < size) {
      localBuffer.set(newBuffer(size))
      buffer(size)
    } else {
      b.clear()
      b
    }
  }

  def append(file: File, timer: Timer, syncJournal: Duration) = {
    val journal = new JournalFile(file, timer, syncJournal)
    journal.openForAppend()
    journal
  }

  def createWriter(file: File, timer: Timer, syncJournal: Duration) = {
    val journal = new JournalFile(file, timer, syncJournal)
    journal.create(HEADER_WRITER)
    journal
  }

  def createReader(file: File, timer: Timer, syncJournal: Duration) = {
    val journal = new JournalFile(file, timer, syncJournal)
    journal.create(HEADER_READER)
    journal
  }

  def openWriter(file: File, timer: Timer, syncJournal: Duration) = {
    val journal = new JournalFile(file, timer, syncJournal)
    if (journal.openForRead() != HEADER_WRITER) throw new IOException("Not a writer journal file")
    journal
  }

  def openReader(file: File, timer: Timer, syncJournal: Duration) = {
    val journal = new JournalFile(file, timer, syncJournal)
    if (journal.openForRead() != HEADER_READER) throw new IOException("Not a reader journal file")
    journal
  }

  // returned from journal replay
  sealed trait Record
  object Record {
    case class Put(item: QueueItem) extends Record
    case class ReadHead(id: Long) extends Record
    case class ReadDone(ids: Seq[Long]) extends Record
    case class Unknown(command: Int) extends Record
  }
}

class JournalFile(file: File, timer: Timer, syncJournal: Duration)
  extends Iterable[JournalFile.Record]
{
  import JournalFile._

  private[this] var writer: PeriodicSyncFile = null
  private[this] var reader: FileChannel = null

  def openForAppend() {
    writer = new PeriodicSyncFile(file, timer, syncJournal)
  }

  def openForRead() = {
    reader = new FileInputStream(file).getChannel
    val b = buffer
    b.limit(4)
    b.order(ByteOrder.BIG_ENDIAN)
    var x = 0
    do {
      x = reader.read(b)
    } while (b.position < b.limit && x >= 0)
    if (x < 0) throw new CorruptedJournalException(0, file, "No header in journal file: " + file)
    b.flip()
    b.getInt()
  }

  def create(header: Int) {
    writer = new PeriodicSyncFile(file, timer, syncJournal)
    writer.position = 0
    val b = buffer(4)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putInt(header)
    b.flip()
    writer.write(b)
  }

  def close() {
    if (writer ne null) writer.close()
    if (reader ne null) reader.close()
  }

  private[this] val READ_HEAD = 0
  private[this] val PUT = 8
  private[this] val READ_DONE = 9

  def put(item: QueueItem): Future[Unit] = {
    if (item.data.size > LARGEST_DATA.inBytes) {
      throw new IOException("item too large")
    }
    val b = buffer(item.data.size + (7 * 4) + 1)
    val size = if (item.expireTime.isDefined) 7 else 5
    b.put((PUT << 4 | size).toByte)
    b.putInt(item.data.size)
    b.putLong(item.id)
    b.putLong(item.addTime.inMilliseconds)
    item.expireTime.foreach { t => b.putLong(t.inMilliseconds) }
    b.put(item.data)
    write(b)
  }

  def readHead(id: Long): Future[Unit] = {
    val b = buffer(9)
    b.put((READ_HEAD << 4 | 2).toByte)
    b.putLong(id)
    write(b)
  }

  def readDone(ids: Seq[Long]): Future[Unit] = {
    val b = buffer(8 * ids.size + 5)
    b.put((READ_DONE << 4 | 1).toByte)
    b.putInt(8 * ids.size)
    ids.foreach { b.putLong(_) }
    write(b)
  }

  private[this] def write(b: ByteBuffer) = {
    b.flip()
    writer.write(b)
  }

  def readNext(): Option[Record] = {
    val lastPosition = reader.position
    val b = buffer
    b.limit(1)
    var x: Int = 0
    try {
      do {
        x = reader.read(b)
      } while (b.position < b.limit && x >= 0)
    } catch {
      case e: IOException =>
        throw new CorruptedJournalException(lastPosition, file, e.toString)
    }
    if (x < 0) return None
    val command = (b.get(0) >> 4) & 0xf
    val headerSize = (b.get(0) & 0xf)
    b.clear()
    b.limit(headerSize * 4)
    try {
      do {
        x = reader.read(b)
      } while (b.position < b.limit && x >= 0)
    } catch {
      case e: IOException =>
        throw new CorruptedJournalException(lastPosition, file, e.toString)
    }
    b.rewind()

    var data: Array[Byte] = null
    if (command >= 8) {
      val dataSize = b.getInt()
      if (dataSize > LARGEST_DATA.inBytes) {
        throw new CorruptedJournalException(lastPosition, file, "item too large")
      }

      data = new Array[Byte](dataSize)
      val dataBuffer = ByteBuffer.wrap(data)
      try {
        do {
          x = reader.read(dataBuffer)
        } while (dataBuffer.position < dataBuffer.limit && x >= 0)
      } catch {
        case e: IOException =>
          throw new CorruptedJournalException(lastPosition, file, e.toString)
      }
    }

    if (x < 0) throw new CorruptedJournalException(lastPosition, file, "truncated entry")

    Some(command match {
      case READ_HEAD => {
        if (b.limit < 8) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted READ_HEAD")
        }
        val id = b.getLong()
        Record.ReadHead(id)
      }
      case PUT => {
        if (b.limit < 20) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted PUT")
        }
        val id = b.getLong()
        val addTime = b.getLong()
        val expireTime = if (b.position + 4 <= b.limit) Some(b.getLong()) else None
        Record.Put(QueueItem(id, Time.fromMilliseconds(addTime), expireTime.map { t =>
          Time.fromMilliseconds(t)
        }, data))
      }
      case READ_DONE => {
        if (data.size % 8 != 0) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted READ_DONE")
        }
        val ids = new Array[Long](data.size / 8)
        val b = ByteBuffer.wrap(data)
        b.order(ByteOrder.LITTLE_ENDIAN)
        for (i <- 0 until ids.size) { ids(i) = b.getLong() }
        Record.ReadDone(ids)
      }
      case _ => {
        // this is okay. we can skip the ones we don't know.
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

  def position = if (reader ne null) reader.position else writer.position
}
