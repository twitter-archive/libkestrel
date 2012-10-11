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

import com.twitter.conversions.time._
import com.twitter.util.{MockTimer, Time, TimeControl}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.{BeforeAndAfter, FunSpec}

class DeadlineWaitQueueSpec extends FunSpec with BeforeAndAfter {
  describe("DeadlineWaitQueue") {
    val timer = new MockTimer
    var timeouts = new AtomicInteger(0)
    var awakens = new AtomicInteger(0)

    def newQueue() = new DeadlineWaitQueue(timer)

    before {
      timeouts.set(0)
      awakens.set(0)
    }

    def defaultAwakens() {
      awakens.incrementAndGet()
    }

    def defaultTimeout() {
      timeouts.incrementAndGet()
    }

    it("should invoke timeout function when deadline expires") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)

        tc.advance(5.seconds)
        timer.tick()
        assert(timeouts.get === 0)
        assert(awakens.get === 0)

        tc.advance(5.seconds + 1.millisecond)
        timer.tick()
        assert(timeouts.get === 1)
        assert(awakens.get === 0)
      }
    }

    it("should remove waiters after timeout") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(deadlineWaitQueue.size === 1)
        tc.advance(11.seconds)
        timer.tick()
        assert(deadlineWaitQueue.size === 0)
      }
    }

    it("should invoke the awakens function when triggered") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)

        tc.advance(5.seconds)
        timer.tick()
        assert(timeouts.get === 0)
        assert(awakens.get === 0)

        deadlineWaitQueue.trigger
        assert(timeouts.get === 0)
        assert(awakens.get === 1)
      }
    }

    it("should remove waiters after trigger") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(deadlineWaitQueue.size === 1)
        deadlineWaitQueue.trigger
        assert(deadlineWaitQueue.size === 0)
      }
    }

    it("should awaken only a single waiter at a time") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(timeouts.get === 0)
        assert(awakens.get === 0)
        assert(deadlineWaitQueue.size === 2)

        deadlineWaitQueue.trigger
        assert(timeouts.get === 0)
        assert(awakens.get === 1)
        assert(deadlineWaitQueue.size === 1)


        deadlineWaitQueue.trigger
        assert(timeouts.get === 0)
        assert(awakens.get === 2)
        assert(deadlineWaitQueue.size === 0)
      }
    }

    it("should awaken all waiters when requested") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(timeouts.get === 0)
        assert(awakens.get === 0)

        deadlineWaitQueue.triggerAll
        assert(timeouts.get === 0)
        assert(awakens.get === 2)
      }
    }

    it("should remove waiters after triggering all") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(deadlineWaitQueue.size === 2)
        deadlineWaitQueue.triggerAll
        assert(deadlineWaitQueue.size === 0)
      }
    }

    it("should explicitly remove a waiter without awakening or timing out") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        val waiter = deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(deadlineWaitQueue.size === 1)
        assert(timeouts.get === 0)
        assert(awakens.get === 0)
        deadlineWaitQueue.remove(waiter)
        assert(deadlineWaitQueue.size === 0)
        assert(timeouts.get === 0)
        assert(awakens.get === 0)
      }
    }

    it("should evict waiters and cancel their timer tasks") {
      Time.withCurrentTimeFrozen { tc =>
        val deadlineWaitQueue = newQueue()
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(Before(10.seconds.fromNow), defaultAwakens, defaultTimeout)
        assert(deadlineWaitQueue.size === 2)
        assert(timeouts.get === 0)
        assert(awakens.get === 0)

        deadlineWaitQueue.evictAll()
        assert(deadlineWaitQueue.size === 0)
        assert(timeouts.get === 2)
        assert(awakens.get === 0)

        tc.advance(11.seconds)
        timer.tick()
        assert(deadlineWaitQueue.size === 0)
        assert(timeouts.get === 2)
        assert(awakens.get === 0)
      }
    }
  }
}
