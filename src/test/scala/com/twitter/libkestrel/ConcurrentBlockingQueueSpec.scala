package com.twitter.libkestrel

import scala.collection.mutable
import org.specs.Specification
import com.twitter.conversions.time._
import com.twitter.util.{TimeoutException, Timer, JavaTimer}

object ConcurrentBlockingQueueSpec extends Specification {
  implicit val javaTimer: Timer = new JavaTimer()

  trait QueueBuilder {
    def newQueue(): BlockingQueue[String]
    def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy): BlockingQueue[String]
  }

  def tests(builder: QueueBuilder) {
    import builder._

    "add and remove items" in {
      val queue = newQueue()
      queue.size mustEqual 0
      queue.put("first") mustEqual true
      queue.size mustEqual 1
      queue.put("second") mustEqual true
      queue.size mustEqual 2
      queue.get()() mustEqual Some("first")
      queue.size mustEqual 1
      queue.get()() mustEqual Some("second")
      queue.size mustEqual 0
    }

    "poll items" in {
      val queue = newQueue()
      queue.size mustEqual 0
      queue.poll() mustEqual None
      queue.put("first") mustEqual true
      queue.size mustEqual 1
      queue.poll() mustEqual Some("first")
      queue.poll() mustEqual None
    }

    "conditionally poll items" in {
      val queue = newQueue()
      queue.size mustEqual 0
      queue.poll() mustEqual None
      queue.put("first") mustEqual true
      queue.put("second") mustEqual true
      queue.put("third") mustEqual true
      queue.size mustEqual 3
      queue.pollIf(_ contains "t") mustEqual Some("first")
      queue.pollIf(_ contains "t") mustEqual None
      queue.pollIf(_ contains "c") mustEqual Some("second")
      queue.pollIf(_ contains "t") mustEqual Some("third")
      queue.pollIf(_ contains "t") mustEqual None
    }

    "honor the max size" in {
      "by refusing new puts" in {
        val queue = newQueue(5, ConcurrentBlockingQueue.FullPolicy.RefusePuts)
        (0 until 5).foreach { i =>
          queue.put(i.toString) mustEqual true
        }
        queue.size mustEqual 5
        queue.put("5") mustEqual false
        queue.size mustEqual 5
        (0 until 5).foreach { i =>
          queue.get()() mustEqual Some(i.toString)
        }
      }

      "by dropping old items" in {
        val queue = newQueue(5, ConcurrentBlockingQueue.FullPolicy.DropOldest)
        (0 until 5).foreach { i =>
          queue.put(i.toString) mustEqual true
        }
        queue.size mustEqual 5
        queue.put("5") mustEqual true
        queue.size mustEqual 5
        (0 until 5).foreach { i =>
          queue.get()() mustEqual Some((i + 1).toString)
        }
      }
    }

    "fill in empty promises as items arrive" in {
      val queue = newQueue()
      val futures = (0 until 10).map { i => queue.get() }.toList
      futures.foreach { f => f.isDefined mustEqual false }

      (0 until 10).foreach { i => queue.put(i.toString) }
      (0 until 10).foreach { i =>
        futures(i).isDefined mustEqual true
        futures(i)() mustEqual Some(i.toString)
      }
    }

    "timeout" in {
      val queue = newQueue()
      val future = queue.get(10.milliseconds.fromNow)
      future.isDefined must eventually(be_==(true))
      future() mustEqual None
    }

    "fulfill gets before they timeout" in {
      val queue = newQueue()
      val future1 = queue.get(10.milliseconds.fromNow)
      val future2 = queue.get(10.milliseconds.fromNow)
      queue.put("surprise!")
      future1.isDefined must eventually(be_==(true))
      future2.isDefined must eventually(be_==(true))
      future1() mustEqual Some("surprise!")
      future2() mustEqual None
    }

    "get an item or throw a timeout exception, but not both" in {
      var ex = 0
      (0 until 100).foreach { i =>
        val queue = newQueue()
        val future = queue.get(10.milliseconds.fromNow)
        Thread.sleep(10)
        // the future will throw an exception if it's set twice.
        queue.put("ahoy!")
        future() match {
          case Some(x) => x mustEqual "ahoy!"
          case None => ex += 1
        }
      }
      if (ex == 0 || ex == 100) println("WARNING: Not really enough timer jitter to make this test valid.")
    }

    "remain calm in the presence of a put-storm" in {
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
      (0 until count).map { _.toString }.toSet mustEqual collected
    }
  }

  "ConcurrentBlockingQueue" should {
    tests(new QueueBuilder {
      def newQueue() = ConcurrentBlockingQueue[String]
      def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy) =
        ConcurrentBlockingQueue[String](maxItems, fullPolicy)
    })
  }

  "SimpleBlockingQueue" should {
    tests(new QueueBuilder {
      def newQueue() = SimpleBlockingQueue[String]
      def newQueue(maxItems: Int, fullPolicy: ConcurrentBlockingQueue.FullPolicy) =
        ConcurrentBlockingQueue[String](maxItems, fullPolicy)
    })
  }
}
