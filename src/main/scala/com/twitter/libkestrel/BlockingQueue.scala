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

import com.twitter.util.{Future, Time}

sealed abstract class Deadline
case class Before(deadline: Time) extends Deadline
case object Forever extends Deadline

trait BlockingQueue[A <: AnyRef] {
  def put(item: A): Boolean
  def putHead(item: A)
  def size: Int
  def get(): Future[Option[A]]
  def get(deadline: Deadline): Future[Option[A]]
  def poll(): Future[Option[A]]
  def pollIf(predicate: A => Boolean): Future[Option[A]]
  def flush()
  def toDebug: String
  def close()
  def waiterCount: Int
  def evictWaiters()
}

trait Transaction[A <: AnyRef] {
  def item: A
  def commit(): Unit
  def rollback(): Unit
}

trait TransactionalBlockingQueue[A <: AnyRef] {
  def put(item: A): Boolean
  def size: Int
  def get(): Future[Option[Transaction[A]]]
  def get(deadline: Deadline): Future[Option[Transaction[A]]]
  def poll(): Future[Option[Transaction[A]]]
  def flush()
  def toDebug: String
  def close()
  def waiterCount: Int
  def evictWaiters()
}
