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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

object ConcurrentBlockingQueue {
  /** What to do when the queue is full and a `put` is attempted (for the constructor). */
  abstract sealed class FullPolicy

  object FullPolicy {
    /** When the queue is full, a `put` attempt returns `false`. */
    case object RefusePuts extends FullPolicy

    /** When the queue is full, a `put` attempt will throw away the head item. */
    case object DropOldest extends FullPolicy
  }

  /**
   * Make a queue with no effective size limit.
   */
  def apply[A <: AnyRef](implicit timer: Timer) = {
    new ConcurrentBlockingQueue[A](Long.MaxValue, FullPolicy.RefusePuts, timer)
  }

  /**
   * Make a queue with a fixed maximum item count and a policy for what to do when the queue is
   * full and a `put` is attempted.
   */
  def apply[A <: AnyRef](maxItems: Long, fullPolicy: FullPolicy)(implicit timer: Timer) = {
    new ConcurrentBlockingQueue[A](maxItems, fullPolicy, timer)
  }
}

/**
 * A lock-free blocking queue that supports timeouts.
 *
 * It works by having one `ConcurrentLinkedQueue` for the queue itself, and
 * another one to track waiting consumers. Each time an item is put into the
 * queue, it's handed off to the next waiting consumer, like an airport taxi
 * line.
 *
 * The handoff occurs in a serialized block (like the `Serialized` trait in
 * util-core), so when there's no contention, a new item is handed directly from
 * the producer to the consumer. When there is contention, producers increment a
 * counter representing how many pent-up handoffs there are, and the producer
 * that got into the serialized block first will do each handoff in order until
 * the count is zero again. This way, no producer is ever blocked.
 *
 * Consumers receive a future that will eventually be fulfilled either with
 * `Some(item)` if an item arrived before the requested timeout, or `None` if the
 * request timed out. If an item was available immediately, the future will be
 * fulfilled before the consumer receives it. This way, no consumer is ever
 * blocked.
 *
 * @param maxItems maximum allowed size of the queue (use `Long.MaxValue` for infinite size)
 * @param fullPolicy what to do when the queue is full and a `put` is attempted
 * @param timer a Timer to use for triggering timeouts
 */
final class ConcurrentBlockingQueue[A <: AnyRef](
  maxItems: Long,
  fullPolicy: ConcurrentBlockingQueue.FullPolicy,
  timer: Timer
) extends BlockingQueue[A] {
  import ConcurrentBlockingQueue._

  /**
   * The actual queue of items.
   * We assume that normally there are more readers than writers, so the queue is normally empty.
   * But when nobody is waiting, we degenerate into a non-blocking queue, and this queue comes
   * into play.
   */
  private[this] val queue = new ConcurrentLinkedQueue[A]

  /**
   * Items "returned" to the head of this queue. Usually this has zero or only a few items.
   */
  private[this] val headQueue = new ConcurrentLinkedQueue[A]

  /**
   * A queue of readers, some waiting with a timeout, others polling.
   * `consumers` tracks the order for fairness, but `waiterSet` and `pollerSet` are
   * the definitive sets: a waiter/poller may be the queue, but not in the set, which
   * just means that they had a timeout set and gave up or were rejected due to an
   * empty queue.
   */
  abstract sealed class Consumer {
    def promise: Promise[Option[A]]
    def apply(item: A): Boolean
  }
  private[this] val consumers = new ConcurrentLinkedQueue[Consumer]

  /**
   * A queue of readers waiting to retrieve an item. See `consumers`.
   */
  case class Waiter(promise: Promise[Option[A]], timerTask: Option[TimerTask]) extends Consumer {
    def apply(item: A) = {
      timerTask.foreach { _.cancel() }
      promise.setValue(Some(item))
      true
    }
  }
  private[this] val waiterSet = new ConcurrentHashMap[Promise[Option[A]], Promise[Option[A]]]

  /**
   * A queue of pollers just checking in to see if anything is immediately available.
   * See `consumers`.
   */
  case class Poller(promise: Promise[Option[A]], predicate: A => Boolean) extends Consumer {
    def apply(item: A) = {
      if (predicate(item)) {
        promise.setValue(Some(item))
        true
      } else {
        promise.setValue(None)
        false
      }
    }
  }
  private[this] val truth: A => Boolean = { _ => true }
  private[this] val pollerSet = new ConcurrentHashMap[Promise[Option[A]], Promise[Option[A]]]

  /**
   * An estimate of the queue size, tracked for each put/get.
   */
  private[this] val elementCount = new AtomicInteger(0)

  /**
   * Sequential lock used to serialize access to handoffOne().
   */
  private[this] val triggerLock = new AtomicInteger(0)

  /**
   * Count of items dropped because the queue was full.
   */
  val droppedCount = new AtomicInteger(0)

  /**
   * Inserts the specified element into this queue if it is possible to do so immediately without
   * violating capacity restrictions, returning `true` upon success and `false` if no space is
   * currently available.
   */
  def put(item: A): Boolean = {
    if (elementCount.incrementAndGet() > maxItems && fullPolicy == FullPolicy.RefusePuts) {
      elementCount.decrementAndGet()
      false
    } else {
      queue.add(item)
      handoff()
      true
    }
  }

  /**
   * Inserts the specified element into this queue at the head, without checking capacity
   * restrictions. This is used to "return" items to a queue.
   */
  def putHead(item: A) {
    headQueue.add(item)
    elementCount.incrementAndGet()
    handoff()
  }

  /**
   * Return the size of the queue as it was at some (recent) moment in time.
   */
  def size: Int = elementCount.get()

  /**
   * Return the number of consumers waiting for an item.
   */
  def waiterCount: Int = waiterSet.size

  /**
   * Get the next item from the queue, waiting forever if necessary.
   */
  def get(): Future[Option[A]] = get(None)

  /**
   * Get the next item from the queue if it arrives before a timeout.
   */
  def get(deadline: Time): Future[Option[A]] = get(Some(deadline))

  /**
   * Get the next item from the queue if one is immediately available.
   */
  def poll(): Future[Option[A]] = pollIf(truth)

  /**
   * Get the next item from the queue if it satisfies a predicate.
   */
  def pollIf(predicate: A => Boolean): Future[Option[A]] = {
    if (queue.isEmpty && headQueue.isEmpty) {
      Future.value(None)
    } else {
      val promise = new Promise[Option[A]]
      pollerSet.put(promise, promise)
      consumers.add(Poller(promise, predicate))
      handoff()
      promise
    }
  }

  def flush() {
    queue.clear()
    headQueue.clear()
  }

  private def get(deadline: Option[Time]): Future[Option[A]] = {
    val promise = new Promise[Option[A]]
    waiterSet.put(promise, promise)
    val timerTask = deadline.map { d =>
      timer.schedule(d) {
        if (waiterSet.remove(promise) ne null) {
          promise.setValue(None)
        }
      }
    }
    consumers.add(Waiter(promise, timerTask))
    promise.onCancellation {
      waiterSet.remove(promise)
      timerTask.foreach { _.cancel() }
    }
    if (!queue.isEmpty || !headQueue.isEmpty) handoff()
    promise
  }

  /**
   * This is the only code path allowed to remove an item from `queue` or `consumers`.
   */
  private[this] def handoff() {
    if (triggerLock.getAndIncrement() == 0) {
      do {
        handoffOne()
      } while (triggerLock.decrementAndGet() > 0)
    }
  }

  private[this] def handoffOne() {
    if (fullPolicy == FullPolicy.DropOldest) {
      // make sure we aren't over the max queue size.
      while (elementCount.get > maxItems) {
        droppedCount.getAndIncrement()
        queue.poll()
        elementCount.decrementAndGet()
      }
    }

    var fromHead = false
    val item = {
      val x = headQueue.peek()
      if (x ne null) {
        fromHead = true
        x
      } else queue.peek()
    }
    if (item ne null) {
      var consumer: Consumer = null
      var invalid = true
      do {
        consumer = consumers.poll()
        invalid = consumer match {
          case null => false
          case Waiter(promise, _) => waiterSet.remove(promise) eq null
          case Poller(promise, _) => pollerSet.remove(promise) eq null
        }
      } while (invalid)

      if ((consumer ne null) && consumer(item)) {
        if (fromHead) headQueue.poll() else queue.poll()
        if (elementCount.decrementAndGet() == 0) {
          dumpPollerSet()
        }
      }
    } else {
      // empty -- dump outstanding pollers
      dumpPollerSet()
    }
  }

  private[this] def dumpPollerSet() {
    pollerSet.keySet.asScala.toArray.foreach { poller =>
      poller.setValue(None)
      pollerSet.remove(poller)
    }
  }

  def toDebug: String = {
    "<ConcurrentBlockingQueue size=%d waiters=%d get=%d poll=%d>".format(
      elementCount.get, consumers.size, waiterSet.size, pollerSet.size)
  }

  def close() {
    queue.clear()
    headQueue.clear()
    waiterSet.asScala.keys.foreach { _.setValue(None) }
    pollerSet.asScala.keys.foreach { _.setValue(None) }
  }
}
