package com.twitter.libkestrel

import com.twitter.util.{Duration, Future}

trait BlockingQueue[A <: AnyRef] {
  def put(item: A): Boolean
  def size: Int
  def get(): Future[A]
  def get(timeout: Duration): Future[A]
  def poll(): Option[A]
  def toDebug: String
}
