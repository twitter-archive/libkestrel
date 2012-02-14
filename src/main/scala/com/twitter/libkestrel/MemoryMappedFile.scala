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
import java.io.{File, IOException, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap
import scala.ref.WeakReference

object MemoryMappedFile {
  private val mappedFiles = new HashMap[String, WeakReference[WritableMemoryMappedFile]]

  def map(file: File, size: StorageUnit, truncate: Boolean = false): MemoryMappedFile = {
    getOrCreateMappedByteBuffer(file, size, truncate = truncate)
  }

  def readOnlyMap(file: File): MemoryMappedFile = {
    getOrCreateMappedByteBuffer(file, file.length.bytes, truncate = false, readOnly = true)
  }

  // for testing
  def openFiles() = {
    mappedFiles.synchronized {
      mappedFiles.keys.toList
    }
  }

  // for testing
  def reset() = {
    mappedFiles.synchronized {
      mappedFiles.clear()
    }
  }

  private[this] def getOrCreateMappedByteBuffer[A](file: File,
                                                   size: StorageUnit,
                                                   truncate: Boolean = false,
                                                   readOnly: Boolean = false): MemoryMappedFile = {
    val canonicalFile = file.getCanonicalFile
    val canonicalPath = canonicalFile.getPath
    mappedFiles.synchronized {
      val weakRef = mappedFiles get(canonicalPath) flatMap { _.get }
      weakRef match {
        case Some(mappedFile) =>
          if (readOnly) new ReadOnlyMemoryMappedFileView(mappedFile)
          else throw new IOException("multiple writers on " + file)
        case None =>
          val mappedFile = new WritableMemoryMappedFile(canonicalFile, size, truncate)
          mappedFiles(canonicalPath) = new WeakReference(mappedFile)
          if (readOnly) {
            val readOnlyMapping = new ReadOnlyMemoryMappedFileView(mappedFile)
            mappedFile.close()
            readOnlyMapping
          } else {
            mappedFile
          }
      }
    }
  }
}

trait MemoryMappedFile {
  def file: File
  def size: StorageUnit

  val MMAP_LIMIT = 2.gigabytes

  protected[this] var _buffer: ByteBuffer = null

  def buffer() = _buffer.slice()

  def force(): Unit

  def close(): Unit

  protected[this] def mappedFiles = MemoryMappedFile.mappedFiles

  protected def open(mode: FileChannel.MapMode, truncate: Boolean): MappedByteBuffer = {
    val modeString = mode match {
      case FileChannel.MapMode.READ_WRITE => "rw"
      case FileChannel.MapMode.READ_ONLY if truncate => throw new IOException("cannot truncate read-only mapping")
      case FileChannel.MapMode.READ_ONLY => "r"
      case _ => throw new IllegalArgumentException("unknown map mode: " + mode)
    }

    if (size > MMAP_LIMIT) throw new IOException("exceeded %s mmap limit".format(MMAP_LIMIT.toHuman))

    val channel = new RandomAccessFile(file, modeString).getChannel
    try {
      if (truncate) channel.truncate(0)
      channel.map(mode, 0, size.inBytes)
    } finally {
      channel.close
    }
  }

  protected override def finalize() {
    if (_buffer != null) {
      close()
    }
  }
}

class WritableMemoryMappedFile(val file: File, val size: StorageUnit, truncate: Boolean) extends MemoryMappedFile {
  _buffer = open(FileChannel.MapMode.READ_WRITE, truncate)

  private[this] val refs = new AtomicInteger(1)

  def force() {
    _buffer.asInstanceOf[MappedByteBuffer].force()
  }

  def close() {
    mappedFiles.synchronized {
      if (removeReference) {
        mappedFiles.remove(file.getPath)
      }
    }
  }

  def addReference() {
    refs.incrementAndGet
  }

  def removeReference(): Boolean = {
    if (_buffer == null) {
      throw new IOException("already closed")
    }

    if (refs.decrementAndGet() == 0) {
      _buffer = null
      true
    } else {
      false
    }
  }

}

trait MemoryMappedFileView extends MemoryMappedFile {
  protected[this] def underlyingMap: MemoryMappedFile

  def file = underlyingMap.file
  def size = underlyingMap.size

  def close() {
    if (_buffer == null) throw new IOException("already closed")

    _buffer = null
    underlyingMap.close()
  }
}

class WritableMemoryMappedFileView(val underlyingMap: WritableMemoryMappedFile) extends MemoryMappedFileView {
  _buffer = underlyingMap.buffer()
  underlyingMap.addReference()

  def force() { underlyingMap.force() }
}

class ReadOnlyMemoryMappedFileView(val underlyingMap: WritableMemoryMappedFile) extends MemoryMappedFileView {
  _buffer = underlyingMap.buffer().asReadOnlyBuffer()
  underlyingMap.addReference()

  def force() {
    throw new UnsupportedOperationException("read-only memory mapped file")
  }
}
