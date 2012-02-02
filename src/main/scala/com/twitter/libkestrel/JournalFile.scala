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
import java.nio.{ByteBuffer, ByteOrder, MappedByteBuffer}
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

  def append(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
             maxFileSize: StorageUnit) = {
    val position = file.length()
//println("open " + file + " for append @ " + position)
    val journal = new JournalFile(file, scheduler, syncJournal, Some(maxFileSize))
    journal.openForAppend(position)
    journal
  }

  def createWriter(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
                   maxFileSize: StorageUnit) = {
    val journal = new JournalFile(file, scheduler, syncJournal, Some(maxFileSize))
    journal.create(HEADER_WRITER)
    journal
  }

  def createReader(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal, None)
    journal.create(HEADER_READER)
    journal
  }

  def openWriter(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal, None)
    if (journal.openForRead() != HEADER_WRITER) throw new IOException("Not a writer journal file")
    journal
  }

  def openReader(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration) = {
    val journal = new JournalFile(file, scheduler, syncJournal, None)
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

class JournalFile(val file: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
                  val maxFileSize: Option[StorageUnit])
  extends Iterable[JournalFile.Record]
{
  import JournalFile._

  private[this] var writer: PeriodicSyncFile = null
  private[this] var reader: MappedByteBuffer = null

  def openForAppend(position: Long) {
    writer = new PeriodicSyncFile(file, scheduler, syncJournal, maxFileSize)
    writer.position = position
  }

  def openForRead() = {
    val channel = new FileInputStream(file).getChannel
    reader = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size)
    channel.close()
//println("reading from " + file)
    if (reader.remaining < 4) throw new CorruptedJournalException(0, file, "No header in journal file: " + file)
    reader.order(ByteOrder.BIG_ENDIAN)
    val magic = reader.getInt
    reader.order(ByteOrder.LITTLE_ENDIAN)
    magic
  }

  def create(header: Int) {
    writer = new PeriodicSyncFile(file, scheduler, syncJournal, maxFileSize)
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
    if (reader ne null) reader = null
  }

  private[this] val READ_HEAD = 0
  private[this] val PUT = 8
  private[this] val READ_DONE = 9

  def storageSizeOf(item: QueueItem): Int = {
    (if (item.expireTime.isDefined) 8 else 6) * 4 + 1 + item.dataSize
  }

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
//println("reader: " + reader + "; remaining: " + reader.remaining)
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

//println("%x %x %d".format(commandAndHeaderSize, command, headerSize))
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
        reader.position(reader.position + dataSize)
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
