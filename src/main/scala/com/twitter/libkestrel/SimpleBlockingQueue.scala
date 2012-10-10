package com.twitter.libkestrel

import java.util.LinkedHashSet
import scala.collection.mutable
import scala.collection.JavaConverters._
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
 *
 * Puts and gets are done within synchronized blocks, and a DeadlineWaitQueue is used to arbitrate
 * timeouts and handoffs.
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

  def putHead(item: A) {
    synchronized {
      item +=: queue
    }
    waiters.trigger()
  }

  def size: Int = queue.size

  def get(): Future[Option[A]] = get(Forever)

  def get(deadline: Deadline): Future[Option[A]] = {
    val promise = new Promise[Option[A]]
    waitFor(promise, deadline)
    promise
  }

  private def waitFor(promise: Promise[Option[A]], deadline: Deadline) {
    val item = poll()()
    item match {
      case s @ Some(x) => promise.setValue(s)
      case None => {
        val w = waiters.add(
          deadline,
          { () => waitFor(promise, deadline) },
          { () => promise.setValue(None) }
        )
        promise.onCancellation { waiters.remove(w) }
      }
    }
  }

  def poll(): Future[Option[A]] = {
    synchronized {
      Future.value(if (queue.isEmpty) None else Some(queue.dequeue()))
    }
  }

  def pollIf(predicate: A => Boolean): Future[Option[A]] = {
    synchronized {
      Future.value(if (queue.isEmpty || !predicate(queue.head)) None else Some(queue.dequeue()))
    }
  }

  def flush() {
    synchronized {
      queue.clear()
    }
  }

  def toDebug: String = {
    synchronized {
      "<SimpleBlockingQueue size=%d waiters=%d>".format(queue.size, waiters.size)
    }
  }

  def close() {
    queue.clear()
    waiters.triggerAll()
  }

  /**
   * Return the number of consumers waiting for an item.
   */
  def waiterCount: Int = waiters.size

  def evictWaiters() {
    waiters.evictAll()
  }
}

/**
 * A wait queue where each item has a timeout.
 * On each `trigger()`, one waiter is awoken (the awaken function is called). If the timeout is
 * triggered by the Timer, the timeout function will be called instead. The queue promises that
 * exactly one of the functions will be called, never both.
 */
final class DeadlineWaitQueue(timer: Timer) {
  case class Waiter(var timerTask: Option[TimerTask], awaken: () => Unit, timeout: () => Unit)
  private val queue = new LinkedHashSet[Waiter].asScala

  def add(deadline: Deadline, awaken: () => Unit, timeout: () => Unit) = {
    val waiter = Waiter(None, awaken, timeout)
    deadline match {
      case Before(time) =>
        val timerTask = timer.schedule(time) {
          if (synchronized { queue.remove(waiter) }) waiter.timeout()
        }
        waiter.timerTask = Some(timerTask)
      case Forever => ()
    }
    synchronized { queue.add(waiter) }
    waiter
  }

  def trigger() {
    synchronized {
      queue.headOption.map { waiter =>
        queue.remove(waiter)
        waiter
      }
    }.foreach { waiter =>
      waiter.timerTask.foreach { _.cancel() }
      waiter.awaken()
    }
  }

  def triggerAll() {
    synchronized {
      val rv = queue.toArray
      queue.clear()
      rv
    }.foreach { waiter =>
      waiter.timerTask.foreach { _.cancel() }
      waiter.awaken()
    }
  }

  def evictAll() {
    synchronized {
      val rv = queue.toArray
      queue.clear()
      rv
    }.foreach { waiter =>
      waiter.timerTask.foreach { _.cancel() }
      waiter.timeout()
    }
  }

  def remove(waiter: Waiter) {
    synchronized { queue.remove(waiter) }
    waiter.timerTask.foreach { _.cancel() }
  }

  def size = {
    synchronized { queue.size }
  }
}
