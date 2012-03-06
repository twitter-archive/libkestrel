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

import com.twitter.conversions.time._
import com.twitter.util.Duration
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.{AbstractSuite, BeforeAndAfter, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class PeriodicSyncFileSpec extends Spec with ResourceCheckingSuite with ShouldMatchers with TempFolder with TestLogging with BeforeAndAfter {
  describe("PeriodicSyncTask") {
    var scheduler: ScheduledThreadPoolExecutor = null
    var invocations: AtomicInteger = null
    var syncTask: PeriodicSyncTask = null

    before {
      scheduler = new ScheduledThreadPoolExecutor(4)
      invocations = new AtomicInteger(0)
      syncTask = new PeriodicSyncTask(scheduler, 0.milliseconds, 20.milliseconds) {
        override def run() {
          invocations.incrementAndGet
        }
      }
    }

    after {
      scheduler.shutdown()
      scheduler.awaitTermination(5, TimeUnit.SECONDS)
    }

    it("only starts once") {
      val (_, duration) = Duration.inMilliseconds {
        syncTask.start()
        syncTask.start()
        Thread.sleep(100)
        syncTask.stop()
      }

      val expectedInvocations = duration.inMilliseconds / 20
      assert(invocations.get <= expectedInvocations * 3 / 2)
    }

    it("stops") {
      syncTask.start()
      Thread.sleep(100)
      syncTask.stop()
      val invocationsPostTermination = invocations.get
      Thread.sleep(100)
      assert(invocations.get === invocationsPostTermination)
    }

    it("stop given a condition") {
      syncTask.start()
      Thread.sleep(100)

      val invocationsPreStop = invocations.get
      syncTask.stopIf { false }
      Thread.sleep(100)

      val invocationsPostIgnoredStop = invocations.get
      syncTask.stopIf { true }
      Thread.sleep(100)

      val invocationsPostStop = invocations.get
      Thread.sleep(100)

      assert(invocationsPreStop > 0)                            // did something
      assert(invocationsPostIgnoredStop > invocationsPreStop)   // kept going
      assert(invocationsPostStop >= invocationsPostIgnoredStop) // maybe did more
      assert(invocations.get === invocationsPostStop)           // stopped
    }
  }
}
