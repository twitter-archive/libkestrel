package com.twitter.libkestrel

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import com.twitter.conversions.time._
import com.twitter.util._
import java.io.{IOException, FileOutputStream, File}

object PeriodicSyncFile {
  // FIXME
  // override me to track fsync delay metrics
  val addTiming: Duration => Unit = { _ => }
}

/**
 * Open a file for writing, and fsync it on a schedule. The period may be 0 to force an fsync
 * after every write, or `Duration.MaxValue` to never fsync.
 */
class PeriodicSyncFile(file: File, timer: Timer, period: Duration) {
  // pre-completed future for writers who are behaving synchronously.
  private final val DONE = Future(())

  case class TimestampedPromise(val promise: Promise[Unit], val time: Time)

  val writer = new FileOutputStream(file, true).getChannel
  val promises = new ConcurrentLinkedQueue[TimestampedPromise]()
  @volatile var periodicSyncTask: TimerTask = null

  @volatile var closed = false

  def startSync() {
    if (periodicSyncTask eq null) {
      synchronized {
        if (periodicSyncTask eq null) {
          periodicSyncTask = timer.schedule(period.fromNow, period) {
            if (!closed && !promises.isEmpty) fsync()
          }
        }
      }
    }
  }

  private def fsync() {
    // FIXME wut
    // "fsync needs to be synchronized since the timer thread could be running at the same time as a journal rotation."
    synchronized {
      // race: we could underestimate the number of completed writes. that's okay.
      val completed = promises.size
      val fsyncStart = Time.now
      try {
        writer.force(false)
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

      if (promises.isEmpty) {
        synchronized {
          val task = periodicSyncTask
          if (task ne null) periodicSyncTask.cancel()
          periodicSyncTask = null
        }
      }
    }
  }

  def write(buffer: ByteBuffer): Future[Unit] = {
    do {
      writer.write(buffer)
    } while (buffer.position < buffer.limit)
    if (period == 0.seconds) {
      try {
        writer.force(false)
        DONE
      } catch {
        case e: IOException => Future.exception(e)
      }
    } else if (period == Duration.MaxValue) {
      // not fsync'ing.
      DONE
    } else {
      val promise = new Promise[Unit]()
      promises.add(TimestampedPromise(promise, Time.now))
      startSync()
      promise
    }
  }

  /**
   * No locking is done here. Be sure you aren't doing concurrent writes or they may be lost
   * forever, and nobody will cry.
   */
  def close() {
    closed = true
    fsync()
    writer.close()
  }

  def position: Long = writer.position()

  def position_=(p: Long) {
    writer.position(p)
  }

  def truncate() {
    writer.truncate(position)
  }
}
