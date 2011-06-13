package com.twitter.libkestrel

import scala.collection.mutable
import org.specs.Specification
import com.twitter.conversions.time._
import com.twitter.util.{TimeoutException, Timer, JavaTimer}

object ConcurrentBlockingQueueSpec extends Specification {
  implicit val javaTimer: Timer = new JavaTimer()

  "ConcurrentBlockingQueue" should {
    "add and remove items" in {
      val queue = ConcurrentBlockingQueue[String]
      queue.size mustEqual 0
      queue.put("first") mustEqual true
      queue.size mustEqual 1
      queue.put("second") mustEqual true
      queue.size mustEqual 2
      queue.get()() mustEqual "first"
      queue.size mustEqual 1
      queue.get()() mustEqual "second"
      queue.size mustEqual 0
    }

    "honor the max size" in {
      "by refusing new puts" in {
        val queue = ConcurrentBlockingQueue[String](5, ConcurrentBlockingQueue.FullPolicy.RefusePuts)
        (0 until 5).foreach { i =>
          queue.put(i.toString) mustEqual true
        }
        queue.size mustEqual 5
        queue.put("5") mustEqual false
        queue.size mustEqual 5
        (0 until 5).foreach { i =>
          queue.get()() mustEqual i.toString
        }
      }

      "by dropping old items" in {
        val queue = ConcurrentBlockingQueue[String](5, ConcurrentBlockingQueue.FullPolicy.DropOldest)
        (0 until 5).foreach { i =>
          queue.put(i.toString) mustEqual true
        }
        queue.size mustEqual 5
        queue.put("5") mustEqual true
        queue.size mustEqual 5
        (0 until 5).foreach { i =>
          queue.get()() mustEqual (i + 1).toString
        }
      }
    }

    "fill in empty promises as items arrive" in {
      val queue = ConcurrentBlockingQueue[String]
      val futures = (0 until 10).map { i => queue.get() }.toList
      futures.foreach { f => f.isDefined mustEqual false }

      (0 until 10).foreach { i => queue.put(i.toString) }
      (0 until 10).foreach { i =>
        futures(i).isDefined mustEqual true
        futures(i)() mustEqual i.toString
      }
    }

    "timeout" in {
      val queue = ConcurrentBlockingQueue[String]
      val future = queue.get(10.milliseconds)
      future.isDefined must eventually(be_==(true))
      future() must throwA[TimeoutException]
    }

    "fulfill gets before they timeout" in {
      val queue = ConcurrentBlockingQueue[String]
      val future1 = queue.get(10.milliseconds)
      val future2 = queue.get(10.milliseconds)
      queue.put("surprise!")
      future1.isDefined must eventually(be_==(true))
      future2.isDefined must eventually(be_==(true))
      future1() mustEqual "surprise!"
      future2() must throwA[TimeoutException]
    }

    "get an item or throw a timeout exception, but not both" in {
      var ex = 0
      (0 until 100).foreach { i =>
        val queue = ConcurrentBlockingQueue[String]
        val future = queue.get(10.milliseconds)
        Thread.sleep(10)
        // the future will throw an exception if it's set twice.
        queue.put("ahoy!")
        (try {
          future() == "ahoy!"
        } catch {
          case e: TimeoutException =>
            ex += 1
            true
        }) mustEqual true
      }
      if (ex == 0 || ex == 100) println("WARNING: Not really enough timer jitter to make this test valid.")
    }

    "remain calm in the presence of a put-storm" in {
      val count = 100
      val queue = ConcurrentBlockingQueue[String]
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
        collected += f()
      }
      (0 until count).map { _.toString }.toSet mustEqual collected
    }
  }
}
