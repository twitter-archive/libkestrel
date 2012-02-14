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
import java.io.{File, FileInputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.util.concurrent.ScheduledExecutorService

case class CorruptedJournalException(lastValidPosition: Long, file: File, message: String) extends IOException(message)

object JournalFile {
  val HEADER_WRITER = 0x27642603

  def append(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
             maxFileSize: StorageUnit) = {
    val position = file.length()
    val writer = new JournalFileWriter(file, scheduler, syncJournal, maxFileSize)
    writer.openForAppend(position)
    writer
  }

  def create(file: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
                   maxFileSize: StorageUnit) = {
    val writer = new JournalFileWriter(file, scheduler, syncJournal, maxFileSize)
    writer.create(HEADER_WRITER)
    writer
  }

  def open(file: File) = {
    val reader = new JournalFileReader(file)
    if (reader.open() != HEADER_WRITER) {
      reader.close()
      throw new IOException("Not a journal file")
    }

    reader
  }
}

class JournalFileReader(val file: File) extends RecordReader
{
  private[this] val memMappedFile = MemoryMappedFile.readOnlyMap(file)
  private[this] var _reader: ByteBuffer = memMappedFile.buffer()

  def open(): Int = {
    try {
      readMagic()
    } catch {
      case e => close()
    }
  }

  def reader = _reader

  def close() {
    _reader = null
    memMappedFile.close()
  }
}

class JournalFileWriter(val file: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
                        val maxFileSize: StorageUnit)
extends RecordWriter
{
  protected[this] var writer: WritableFile = null

  def openForAppend(position: Long) {
    writer = PeriodicSyncFile.append(file, scheduler, syncJournal, maxFileSize)
    writer.position = position
  }

  def create(header: Int) {
    writer = PeriodicSyncFile.create(file, scheduler, syncJournal, maxFileSize)
    writer.position = 0

    val b = buffer(4)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putInt(header)
    b.flip()
    writer.write(b)
  }
}
