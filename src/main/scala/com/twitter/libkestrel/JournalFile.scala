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
import java.util.concurrent.ScheduledExecutorService

case class CorruptedJournalException(lastValidPosition: Long, file: File, message: String) extends IOException(message)

object JournalFile {
  private[this] val BUFFER_SIZE = 128

  val HEADER_WRITER = 0x27642603
  val HEADER_READER = 0x263C2603

  // throw an error if any queue item is larger than this:
  val LARGEST_DATA = 16.megabytes

  private[this] def newBuffer(size: Int) = {
    val byteBuffer = ByteBuffer.allocate(size)
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

  def append(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal)
    journal.openForAppend()
    journal
  }

  def createWriter(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal)
    journal.create(HEADER_WRITER)
    journal
  }

  def createReader(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal)
    journal.create(HEADER_READER)
    journal
  }

  def openWriter(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal)
    if (journal.openForRead() != HEADER_WRITER) throw new IOException("Not a writer journal file")
    journal
  }

  def openReader(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal)
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

class JournalFile(val file: File, scheduler: ScheduledExecutorService, syncJournal: Duration)
  extends Iterable[JournalFile.Record]
{
  import JournalFile._

  private[this] var writer: PeriodicSyncFile = null
  private[this] var reader: FileChannel = null

  def openForAppend() {
    writer = new PeriodicSyncFile(file, scheduler, syncJournal)
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
    writer = new PeriodicSyncFile(file, scheduler, syncJournal)
    writer.position = 0
    writer.truncate()
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
    if (item.dataSize > LARGEST_DATA.inBytes) {
      throw new IOException("item too large")
    }
    val b = buffer(item.dataSize + (8 * 4) + 1)
    val size = if (item.expireTime.isDefined) 8 else 6
    b.put((PUT << 4 | size).toByte)
    b.putInt(item.dataSize)
    b.putInt(item.errorCount)
    b.putLong(item.id)
    b.putLong(item.addTime.inMilliseconds)
    item.expireTime.foreach { t => b.putLong(t.inMilliseconds) }
    item.data.mark
    b.put(item.data)
    item.data.reset
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

    var dataBuffer: ByteBuffer = null
    if (command >= 8) {
      val dataSize = b.getInt()
      if (dataSize > LARGEST_DATA.inBytes) {
        throw new CorruptedJournalException(lastPosition, file, "item too large")
      }

      dataBuffer = ByteBuffer.allocate(dataSize)
      try {
        do {
          x = reader.read(dataBuffer)
        } while (dataBuffer.position < dataBuffer.limit && x >= 0)
      } catch {
        case e: IOException =>
          throw new CorruptedJournalException(lastPosition, file, e.toString)
      }
      dataBuffer.rewind
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
        if (b.limit < 24) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted PUT")
        }
        val errorCount = b.getInt()
        val id = b.getLong()
        val addTime = b.getLong()
        val expireTime = if (b.position + 4 <= b.limit) Some(b.getLong()) else None
        Record.Put(QueueItem(id, Time.fromMilliseconds(addTime), expireTime.map { t =>
          Time.fromMilliseconds(t)
        }, dataBuffer, errorCount))
      }
      case READ_DONE => {
        if (dataBuffer.remaining % 8 != 0) {
          throw new CorruptedJournalException(lastPosition, file, "corrupted READ_DONE")
        }
        val ids = new Array[Long](dataBuffer.remaining / 8)
        val b = dataBuffer
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
