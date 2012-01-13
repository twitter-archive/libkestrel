package com.twitter.libkestrel

import com.twitter.util.{Future, Time}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

final class JBlockingQueue[A <: AnyRef](maxItems: Long) extends BlockingQueue[A] {
  val queue = new LinkedBlockingQueue[A](maxItems.toInt)

  def put(item: A): Boolean = {
    try {
      queue.put(item)
    } catch { case e => e.printStackTrace() }
    true
  }

  def putHead(item: A) {
    throw new RuntimeException("unsupported operation")
  }

  def size: Int = {
    queue.size()
  }

  def get(): Future[Option[A]] = {
    Future(Option(queue.poll()))
  }

  def get(deadline: Time): Future[Option[A]] = {
    Future(Option(queue.poll((Time.now - deadline).inMilliseconds, TimeUnit.MILLISECONDS)))
  }

  def poll(): Future[Option[A]] = {
    Future(Option(queue.peek()))
  }

  def pollIf(predicate: A => Boolean): Future[Option[A]] = {
    throw new RuntimeException("unsupported operation")
  }

  def flush() {
    queue.clear()
  }

  def toDebug: String = {
    "<JBlockingQueue(%d) size %d>".format(maxItems, size)
  }

  def close() {
    // nothing
  }
}
