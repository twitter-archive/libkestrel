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
import com.twitter.util._
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}
import scala.collection.mutable

class ConcurrentBlockingQueueSpec extends Spec with ShouldMatchers with TempFolder with TestLogging {
  implicit var timer: MockTimer = null

  trait QueueBuilder {
    def newQueue(): BlockingQueue[String]
    def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy): BlockingQueue[String]
  }

  def tests(builder: QueueBuilder) {
    import builder._

    it("add and remove items") {
      val queue = newQueue()
      assert(queue.size === 0)
      assert(queue.put("first"))
      assert(queue.size === 1)
      assert(queue.put("second"))
      assert(queue.size === 2)
      assert(queue.get()() === Some("first"))
      assert(queue.size === 1)
      assert(queue.get()() === Some("second"))
      assert(queue.size === 0)
    }

    it("poll items") {
      val queue = newQueue()
      assert(queue.size === 0)
      assert(queue.poll()() === None)
      assert(queue.put("first"))
      assert(queue.size === 1)
      assert(queue.poll()() === Some("first"))
      assert(queue.poll()() === None)
    }

    it("conditionally poll items") {
      val queue = newQueue()
      assert(queue.size === 0)
      assert(queue.poll()() === None)
      assert(queue.put("first") === true)
      assert(queue.put("second") === true)
      assert(queue.put("third") === true)
      assert(queue.size === 3)
      assert(queue.pollIf(_ contains "t")() === Some("first"))
      assert(queue.pollIf(_ contains "t")() === None)
      assert(queue.pollIf(_ contains "c")() === Some("second"))
      assert(queue.pollIf(_ contains "t")() === Some("third"))
      assert(queue.pollIf(_ contains "t")() === None)
    }

    describe("putHead") {
      it("with items") {
        val queue = newQueue()
        assert(queue.size === 0)
        assert(queue.put("hi"))
        assert(queue.size === 1)
        queue.putHead("bye")
        assert(queue.size === 2)
        assert(queue.get()() == Some("bye"))
        assert(queue.size === 1)
        assert(queue.get()() == Some("hi"))
        assert(queue.size === 0)
      }

      it("get with no items") {
        val queue = newQueue()
        assert(queue.size === 0)
        queue.putHead("foo")
        assert(queue.size === 1)
        assert(queue.get()() == Some("foo"))
        assert(queue.size === 0)
      }

      it("poll with no items") {
        val queue = newQueue()
        assert(queue.size === 0)
        queue.putHead("foo")
        assert(queue.size === 1)
        assert(queue.poll()() == Some("foo"))
        assert(queue.size === 0)
      }
    }

    describe("honor the max size") {
      it("by refusing new puts") {
        val queue = newQueue(5, ConcurrentBlockingQueue.FullPolicy.RefusePuts)
        (0 until 5).foreach { i =>
          assert(queue.put(i.toString))
        }
        assert(queue.size === 5)
        assert(!queue.put("5"))
        assert(queue.size === 5)
        (0 until 5).foreach { i =>
          assert(queue.get()() === Some(i.toString))
        }
      }

      it("by dropping old items") {
        val queue = newQueue(5, ConcurrentBlockingQueue.FullPolicy.DropOldest)
        (0 until 5).foreach { i =>
          assert(queue.put(i.toString))
        }
        assert(queue.size === 5)
        assert(queue.put("5"))
        assert(queue.size === 5)
        (0 until 5).foreach { i =>
          assert(queue.get()() === Some((i + 1).toString))
        }
      }
    }

    it("fill in empty promises as items arrive") {
      val queue = newQueue()
      val futures = (0 until 10).map { i => queue.get() }.toList
      futures.foreach { f => assert(!f.isDefined) }

      (0 until 10).foreach { i =>
        if (i % 2 == 0) queue.put(i.toString) else queue.putHead(i.toString)
      }
      (0 until 10).foreach { i =>
        assert(futures(i).isDefined)
        assert(futures(i)() === Some(i.toString))
      }
    }

    it("timeout") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val queue = newQueue()
        val future = queue.get(10.milliseconds.fromNow)

        timeMutator.advance(10.milliseconds)
        timer.tick()

        assert(future.isDefined)
        assert(future() === None)
      }
    }

    it("fulfill gets before they timeout") {
      Time.withCurrentTimeFrozen { timeMutator =>
        val queue = newQueue()
        val future1 = queue.get(10.milliseconds.fromNow)
        val future2 = queue.get(10.milliseconds.fromNow)
        queue.put("surprise!")

        timeMutator.advance(10.milliseconds)
        timer.tick()

        assert(future1.isDefined)
        assert(future2.isDefined)
        assert(future1() === Some("surprise!"))
        assert(future2() === None)
      }
    }

    describe("really long timeout is cancelled") {
      val deadline = 7.days.fromNow

      it("when an item arrives") {
        val queue = newQueue()
        val future = queue.get(deadline)
        assert(timer.tasks.size === 1)

        queue.put("hello!")
        assert(future() === Some("hello!"))
        timer.tick()
        assert(timer.tasks.size === 0)
      }

      it("when the future is cancelled") {
        val queue = newQueue()
        val future = queue.get(deadline)
        assert(timer.tasks.size === 1)

        future.cancel()
        timer.tick()
        assert(timer.tasks.size === 0)
      }
    }

    it("remain calm in the presence of a put-storm") {
      val count = 100
      val queue = newQueue()
      val futures = (0 until count).map { i => queue.get() }.toList
      val threads = (0 until count).map { i =>
        new Thread() {
          override def run() {
            queue.put(i.toString)
          }
        }
      }.toList
      threads.foreach { _.start() }
      threads.foreach { _.join() }

      val collected = new mutable.HashSet[String]
      futures.foreach { f =>
        collected += f().get
      }
      assert((0 until count).map { _.toString }.toSet === collected)
    }

  }

  describe("ConcurrentBlockingQueue") {
    tests(new QueueBuilder {
      def newQueue() = {
        timer = new MockTimer()
        ConcurrentBlockingQueue[String]
      }

      def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy) = {
        timer = new MockTimer()
        ConcurrentBlockingQueue[String](maxItems, fullPolicy)
      }
    })
  }

  describe("SimpleBlockingQueue") {
    tests(new QueueBuilder {
      def newQueue() = {
        timer = new MockTimer()
        SimpleBlockingQueue[String]
      }

      def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy) = {
        timer = new MockTimer()
        SimpleBlockingQueue[String](maxItems, fullPolicy)
      }
    })
  }
}
