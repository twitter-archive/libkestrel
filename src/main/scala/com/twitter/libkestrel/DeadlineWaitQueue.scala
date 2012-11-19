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

import java.util.LinkedHashSet
import scala.collection.JavaConversions
import com.twitter.util.{Time, Timer, TimerTask}

sealed trait Waiter {
  def awaken: () => Unit
  def timeout: () => Unit
  def cancel(): Unit
}

case class InfiniteWaiter(val awaken: () => Unit, val timeout: () => Unit) extends Waiter {
  def cancel() { }
}

case class DeadlineWaiter(var timerTask: TimerTask, val awaken: () => Unit, val timeout: () => Unit)
extends Waiter {
  def cancel() {
    if (timerTask ne null) timerTask.cancel()
  }
}

/**
 * A wait queue where each item has a timeout.
 * On each `trigger()`, one waiter is awoken (the awaken function is called). If the timeout is
 * triggered by the Timer, the timeout function will be called instead. The queue promises that
 * exactly one of the functions will be called, never both.
 */
final class DeadlineWaitQueue(timer: Timer) {

  private val queue = JavaConversions.asScalaSet(new LinkedHashSet[Waiter])

  def add(deadline: Deadline, awaken: () => Unit, timeout: () => Unit) = {
    val waiter: Waiter =
      deadline match {
        case Before(time) =>
          val deadlineWaiter = DeadlineWaiter(null, awaken, timeout)
          val timerTask = timer.schedule(time) {
            if (synchronized { queue.remove(deadlineWaiter) }) deadlineWaiter.timeout()
          }
          deadlineWaiter.timerTask = timerTask
          deadlineWaiter
        case Forever => InfiniteWaiter(awaken, timeout)
      }

    synchronized { queue.add(waiter) }
    waiter
  }

  def remove(waiter: Waiter) {
    synchronized { queue.remove(waiter) }
    waiter.cancel()
  }

  def trigger() {
    synchronized {
      queue.headOption.map { waiter =>
        queue.remove(waiter)
        waiter
      }
    }.foreach { waiter =>
      waiter.cancel()
      waiter.awaken()
    }
  }

  def triggerAll() {
    synchronized {
      val rv = queue.toArray
      queue.clear()
      rv
    }.foreach { waiter =>
      waiter.cancel()
      waiter.awaken()
    }
  }

  def evictAll() {
    synchronized {
      val rv = queue.toArray
      queue.clear()
      rv
    }.foreach { waiter =>
      waiter.cancel()
      waiter.timeout()
    }
  }

  def size() = {
    synchronized { queue.size }
  }
}
