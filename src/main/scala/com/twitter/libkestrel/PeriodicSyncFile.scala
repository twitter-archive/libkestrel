package com.twitter.libkestrel

import com.twitter.conversions.time._
import com.twitter.util._
import java.io.{IOException, File, FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledExecutorService, ScheduledFuture, TimeUnit}

abstract class PeriodicSyncTask(val scheduler: ScheduledExecutorService, initialDelay: Duration, period: Duration)
extends Runnable {
  @volatile private[this] var scheduledFsync: Option[ScheduledFuture[_]] = None

  def start() {
    synchronized {
      if (scheduledFsync.isEmpty && period > 0.seconds) {
        val handle = scheduler.scheduleWithFixedDelay(this, initialDelay.inMilliseconds, period.inMilliseconds,
                                                      TimeUnit.MILLISECONDS)
        scheduledFsync = Some(handle)
      }
    }
  }

  def stop() {
    synchronized { _stop() }
  }

  def stopIf(f: => Boolean) {
    synchronized {
      if (f) _stop()
    }
  }

  private[this] def _stop() {
    scheduledFsync.foreach { _.cancel(false) }
    scheduledFsync = None
  }
}

object PeriodicSyncFile {
  // FIXME
  // override me to track fsync delay metrics
  val addTiming: Duration => Unit = { _ => }
}

trait SyncFileWriter {
  def force(): Unit
  def position: Long
  def position(p: Long): Unit
  def truncate(onClose: Boolean): Unit
  def write(buffer: ByteBuffer): Unit
  def close(): Unit
}

class StreamSyncFileWriter(file: File) extends SyncFileWriter {
  val writer = new FileOutputStream(file, true).getChannel

  def force() { writer.force(false) }

  def position = writer.position
  def position(p: Long) { writer.position(p) }
  def truncate(onClose: Boolean) { writer.truncate(writer.position) }

  def write(buffer: ByteBuffer) {
    do {
      writer.write(buffer)
    } while(buffer.position < buffer.limit)
  }

  def close() { writer.close() }
}

class MMappedSyncFileWriter(file: File, size: StorageUnit) extends SyncFileWriter {
  var writer = {
    val channel = new RandomAccessFile(file, "rw").getChannel
    val map = channel.map(FileChannel.MapMode.READ_WRITE, 0, size.inBytes)
    channel.close
    map
  }

  def force() { writer.force() }

  def position = writer.position.toLong

  // TODO: make it impossible to overflow this position
  def position(p: Long) { writer.position(p.toInt) }

  def write(buffer: ByteBuffer) {
    writer.put(buffer)
  }

  def truncate(onClose: Boolean) {
    if (onClose) {
      Some(new RandomAccessFile(file, "rw")).foreach { f =>
//println("truncating " + file + " on close to " + writer.position)
        f.setLength(writer.position)
        f.close()
      }
    } else {
//println("truncating " + file + " to " + writer.position)
      // TODO: fugly; truncate is only used on create so get rid of it
      writer.mark()
      val size = 16 * 1024
      val trunc = ByteBuffer.wrap(new Array[Byte](size))
      while(writer.remaining >= size) {
        writer.put(trunc)
        trunc.flip()
      }
      if (writer.remaining > 0) {
        trunc.limit(writer.remaining)
        writer.put(trunc)
      }
      writer.reset()
    }
  }

  def close() {
    // force unmap -- illegal to access writer after this point (the JVM *will* segfault or you'll be accessing
    // some other file recently mapped to the same memory space)
    writer.asInstanceOf[sun.nio.ch.DirectBuffer].cleaner().clean()
    writer = null
  }
}

/**
 * Open a file for writing, and fsync it on a schedule. The period may be 0 to force an fsync
 * after every write, or `Duration.MaxValue` to never fsync.
 */
class PeriodicSyncFile(file: File, scheduler: ScheduledExecutorService, period: Duration, maxFileSize: Option[StorageUnit]) {
  // pre-completed future for writers who are behaving synchronously.
  private final val DONE = Future(())

  case class TimestampedPromise(val promise: Promise[Unit], val time: Time)

  val writer: SyncFileWriter = {
    val someWriter =
      maxFileSize map { size =>
        new MMappedSyncFileWriter(file, size)
      } orElse {
        Some(new StreamSyncFileWriter(file))
      }
    someWriter.get
  }

  val promises = new ConcurrentLinkedQueue[TimestampedPromise]()
  val periodicSyncTask = new PeriodicSyncTask(scheduler, period, period) {
    override def run() {
      if (!closed && !promises.isEmpty) fsync()
    }
  }

  @volatile var closed = false

  private def fsync() {
    // FIXME wut
    // "fsync needs to be synchronized since the timer thread could be running at the same time as a journal rotation."
    synchronized {
      // race: we could underestimate the number of completed writes. that's okay.
      val completed = promises.size
      val fsyncStart = Time.now
      try {
        writer.force()
      } catch {
        case e: IOException => {
          for (i <- 0 until completed) {
            promises.poll().promise.setException(e)
          }
          return
        }
      }

      for (i <- 0 until completed) {
        val timestampedPromise = promises.poll()
        timestampedPromise.promise.setValue(())
        val delaySinceWrite = fsyncStart - timestampedPromise.time
        val durationBehind = if (delaySinceWrite > period) delaySinceWrite - period else 0.seconds
        PeriodicSyncFile.addTiming(durationBehind)
      }

      periodicSyncTask.stopIf { promises.isEmpty }
    }
  }

  def write(buffer: ByteBuffer): Future[Unit] = {
//println("writing to " + file + " @ " + writer.position)
    writer.write(buffer)
    if (period == 0.seconds) {
      try {
        writer.force()
        DONE
      } catch {
        case e: IOException =>
          Future.exception(e)
      }
    } else if (period == Duration.MaxValue) {
      // not fsync'ing.
      DONE
    } else {
      val promise = new Promise[Unit]()
      promises.add(TimestampedPromise(promise, Time.now))
      periodicSyncTask.start()
      promise
    }
  }

  /**
   * No locking is done here. Be sure you aren't doing concurrent writes or they may be lost
   * forever, and nobody will cry.
   */
  def close() {
    closed = true
    periodicSyncTask.stop()
    fsync()
    writer.truncate(true)
    writer.close
  }

  def position: Long = writer.position

  def position_=(p: Long) {
    writer.position(p)
  }

  def truncate() {
    writer.truncate(false)
  }
}
