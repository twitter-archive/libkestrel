package com.twitter.libkestrel

import com.twitter.conversions.time._
import com.twitter.util._
import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}
import scala.collection.mutable

class ConcurrentBlockingQueueSpec extends Spec with ShouldMatchers with TempFolder with TestLogging2 {
  implicit val javaTimer: Timer = new JavaTimer()

  trait QueueBuilder {
    def newQueue(): BlockingQueue[String]
    def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy): BlockingQueue[String]
  }

  def eventually(f: => Boolean): Boolean = {
    val deadline = 5.seconds.fromNow
    while (deadline > Time.now) {
      if (f) return true
      Thread.sleep(10)
    }
    false
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
      assert(queue.poll() === None)
      assert(queue.put("first"))
      assert(queue.size === 1)
      assert(queue.poll() === Some("first"))
      assert(queue.poll() === None)
    }

    it("conditionally poll items") {
      val queue = newQueue()
      assert(queue.size === 0)
      assert(queue.poll() === None)
      assert(queue.put("first") === true)
      assert(queue.put("second") === true)
      assert(queue.put("third") === true)
      assert(queue.size === 3)
      assert(queue.pollIf(_ contains "t") === Some("first"))
      assert(queue.pollIf(_ contains "t") === None)
      assert(queue.pollIf(_ contains "c") === Some("second"))
      assert(queue.pollIf(_ contains "t") === Some("third"))
      assert(queue.pollIf(_ contains "t") === None)
    }

    it("putHead") {
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
      val queue = newQueue()
      val future = queue.get(10.milliseconds.fromNow)
      assert(eventually(future.isDefined))
      assert(future() === None)
    }

    it("fulfill gets before they timeout") {
      val queue = newQueue()
      val future1 = queue.get(10.milliseconds.fromNow)
      val future2 = queue.get(10.milliseconds.fromNow)
      queue.put("surprise!")
      assert(eventually(future1.isDefined))
      assert(eventually(future2.isDefined))
      assert(future1() === Some("surprise!"))
      assert(future2() === None)
    }

    it("get an item or throw a timeout exception, but not both") {
      var ex = 0
      (0 until 100).foreach { i =>
        val queue = newQueue()
        val future = queue.get(10.milliseconds.fromNow)
        Thread.sleep(10)
        // the future will throw an exception if it's set twice.
        queue.put("ahoy!")
        future() match {
          case Some(x) => assert(x === "ahoy!")
          case None => ex += 1
        }
      }
      if (ex == 0 || ex == 100) println("WARNING: Not really enough timer jitter to make this test valid.")
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
      def newQueue() = ConcurrentBlockingQueue[String]
      def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy) =
        ConcurrentBlockingQueue[String](maxItems, fullPolicy)
    })
  }

  describe("SimpleBlockingQueue") {
    tests(new QueueBuilder {
      def newQueue() = SimpleBlockingQueue[String]
      def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy) =
        SimpleBlockingQueue[String](maxItems, fullPolicy)
    })
  }
}
