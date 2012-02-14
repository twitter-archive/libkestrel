package com.twitter.libkestrel

import com.twitter.util._
import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer


trait WritableFile {
  def write(buffer: ByteBuffer): Future[Unit]

  def flush(): Unit
  def close(): Unit

  def position: Long
  def position_=(p: Long): Unit
}

class SimpleFile(file: File) extends WritableFile {
  private val writer = new FileOutputStream(file, false).getChannel

  def position = writer.position
  def position_=(p: Long) { writer.position(p) }

  def write(buffer: ByteBuffer) = {
    do {
      writer.write(buffer)
    } while(buffer.position < buffer.limit)

    Future.Unit
  }

  def flush() { writer.force(false) }

  def close() { writer.close() }
}
