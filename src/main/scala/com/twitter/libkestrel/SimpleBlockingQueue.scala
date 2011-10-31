package com.twitter.libkestrel

import java.util.LinkedHashSet
import scala.collection.mutable
import scala.collection.JavaConverters._
import com.twitter.conversions.time._
import com.twitter.util.{Duration, Future, Promise, Time, TimeoutException, Timer, TimerTask}

object SimpleBlockingQueue {
  def apply[A <: AnyRef](implicit timer: Timer) = {
    new SimpleBlockingQueue[A](Long.MaxValue, ConcurrentBlockingQueue.FullPolicy.RefusePuts, timer)
  }

  def apply[A <: AnyRef](maxItems: Long, fullPolicy: ConcurrentBlockingQueue.FullPolicy)(implicit timer: Timer) = {
    new SimpleBlockingQueue[A](maxItems, fullPolicy, timer)
  }
}

/**
 * Simple reproduction of the queue from kestrel 2.1.
 */
final class SimpleBlockingQueue[A <: AnyRef](
  maxItems: Long,
  fullPolicy: ConcurrentBlockingQueue.FullPolicy,
  timer: Timer
) extends BlockingQueue[A] {
  private var queue = new mutable.Queue[A]
  private val waiters = new DeadlineWaitQueue(timer)

  /**
   * Add a value to the end of the queue, transactionally.
   */
  def put(item: A): Boolean = {
    synchronized {
      while (queue.size >= maxItems) {
        if (fullPolicy == ConcurrentBlockingQueue.FullPolicy.RefusePuts) return false
        get()
      }
      queue += item
    }
    waiters.trigger()
    true
  }

  def size: Int = queue.size

  def get(): Future[A] = get(500.days)

  def get(timeout: Duration): Future[A] = {
    val promise = new Promise[A]
    waitFor(promise, timeout.fromNow)
    promise
  }

  private def waitFor(promise: Promise[A], deadline: Time) {
    val item = poll()
    item match {
      case Some(x) =>
        promise.setValue(x)
      case None =>
        waiters.add(
          deadline,
          { () => waitFor(promise, deadline) },
          { () => promise.setException(new TimeoutException("timeout")) }
        )
    }
  }

  def poll(): Option[A] = {
    synchronized {
      if (queue.isEmpty) None else Some(queue.dequeue())
    }
  }

  def pollIf(predicate: A => Boolean): Option[A] = {
    synchronized {
      if (queue.isEmpty || !predicate(queue.head)) None else Some(queue.dequeue())
    }
  }

  def toDebug: String = {
    synchronized {
      "<SimpleBlockingQueue size=%d waiters=%d>".format(queue.size, waiters.size)
    }
  }
}

/**
 * A wait queue where each item has a timeout.
 * On each `trigger()`, one waiter is awoken (the awaken function is called). If the timeout is
 * triggered by the Timer, the timeout function will be called instead. The queue promises that
 * exactly one of the functions will be called, never both.
 */
final class DeadlineWaitQueue(timer: Timer) {
  case class Waiter(var timerTask: TimerTask, awaken: () => Unit)
  private val queue = new LinkedHashSet[Waiter].asScala

  def add(deadline: Time, awaken: () => Unit, onTimeout: () => Unit) {
    val waiter = Waiter(null, awaken)
    val timerTask = timer.schedule(deadline) {
      if (synchronized { queue.remove(waiter) }) onTimeout()
    }
    waiter.timerTask = timerTask
    synchronized { queue.add(waiter) }
  }

  def trigger() {
    synchronized {
      queue.headOption.map { waiter =>
        queue.remove(waiter)
        waiter
      }
    }.foreach { _.awaken() }
  }

  def triggerAll() {
    synchronized {
      val rv = queue.toArray
      queue.clear()
      rv
    }.foreach { _.awaken() }
  }

  def size = {
    synchronized { queue.size }
  }
}
