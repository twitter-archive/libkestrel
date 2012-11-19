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
 * Simple reproduction of the queue from kestrel 2.x.
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
