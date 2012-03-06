package com.twitter.libkestrel

import com.twitter.conversions.storage._
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
  def create(file: File, scheduler: ScheduledExecutorService, period: Duration, maxFileSize: StorageUnit) = {
    new PeriodicSyncFile(file, scheduler, period, maxFileSize, true)
  }

  def append(file: File, scheduler: ScheduledExecutorService, period: Duration, maxFileSize: StorageUnit) = {
    new PeriodicSyncFile(file, scheduler, period, maxFileSize, false)
  }

  // override me to track fsync delay metrics
  var addTiming: Duration => Unit = { _ => }
}

class MMappedSyncFileWriter(file: File, size: StorageUnit, truncate: Boolean) {
  private[this] val memMappedFile = MemoryMappedFile.map(file, size, truncate)
  var writer = memMappedFile.buffer()

  def force() { memMappedFile.force() }

  def position = writer.position.toLong

  // overflow -> IllegalArgumentException
  def position(p: Long) { writer.position(p.toInt) }

  def write(buffer: ByteBuffer) {
    // Write the first byte last so that readers using the same memory mapped
    // file will not see a non-zero first byte until the remainder of the
    // buffer has been written.
    val startPos = writer.position
    writer.put(0.toByte)
    val startByte = buffer.get
    writer.put(buffer)
    writer.put(startPos, startByte)
  }

  def truncate() {
    val f = new RandomAccessFile(file, "rw")
    f.setLength(writer.position)
    f.close()
  }

  def close() {
    writer = null
    memMappedFile.close()
  }
}

/**
 * Open a file for writing, and fsync it on a schedule. The period may be 0 to force an fsync
 * after every write, or `Duration.MaxValue` to never fsync.
 */
class PeriodicSyncFile(file: File, scheduler: ScheduledExecutorService, period: Duration, maxFileSize: StorageUnit, truncate: Boolean)
extends WritableFile {
  // pre-completed future for writers who are behaving synchronously.
  private final val DONE = Future(())

  case class TimestampedPromise(val promise: Promise[Unit], val time: Time)

  val writer = new MMappedSyncFileWriter(file, maxFileSize, truncate)

  val promises = new ConcurrentLinkedQueue[TimestampedPromise]()
  val periodicSyncTask = new PeriodicSyncTask(scheduler, period, period) {
    override def run() {
      if (!closed && !promises.isEmpty) fsync()
    }
  }

  @volatile var closed = false

  private def fsync() {
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

  def write(buffer: ByteBuffer): Future[Unit] = {
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

  def flush() {
    fsync()
  }

  /**
   * No locking is done here. Be sure you aren't doing concurrent writes or they may be lost
   * forever, and nobody will cry.
   */
  def close() {
    closed = true
    periodicSyncTask.stop()
    fsync()
    writer.truncate()
    writer.close()
  }

  def position: Long = writer.position

  def position_=(p: Long) {
    writer.position(p)
  }
}
