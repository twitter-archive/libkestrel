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

import com.twitter.util.Future
import java.io.{File, IOException}
import java.nio.{ByteBuffer, ByteOrder}

abstract class RecordWriter {
  import Record._

  def file: File
  protected def writer: WritableFile

  private[this] val BUFFER_SIZE = 128

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

  def writeMagic(header: Int) {
    writer.position = 0
    val b = buffer(4)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putInt(header)
    b.flip()
    writer.write(b)
  }

  def put(item: QueueItem): Future[Unit] = {
    if (item.dataSize > LARGEST_DATA.inBytes) {
      throw new IOException("item too large")
    }
    val b = buffer(storageSizeOf(item))
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

  def position = writer.position
  def close() {
    writer.flush()
    writer.close()
  }

  def storageSizeOf(item: QueueItem): Int = {
    (if (item.expireTime.isDefined) 8 else 6) * 4 + 1 + item.dataSize
  }
}
