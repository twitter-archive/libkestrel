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
import com.twitter.util.{Future, Time}
import java.nio.ByteBuffer

trait Codec[A] {
  def encode(item: A): ByteBuffer
  def decode(data: ByteBuffer): A
}

private trait JournaledBlockingQueueMixin[A] {
  def queue: JournaledQueue
  def codec: Codec[A]

  val reader = queue.reader("")

  def put(item: A) = {
    val rv = queue.put(codec.encode(item), Time.now, None)
    rv.isDefined && { rv.get.get(); true }
  }

  def putHead(item: A) {
    throw new Exception("Unsupported operation")
  }

  def size: Int = reader.items

  def flush() {
    queue.flush()
  }

  def toDebug: String = queue.toDebug

  def close() {
    queue.close()
  }
}

private[libkestrel] class JournaledBlockingQueue[A <: AnyRef](val queue: JournaledQueue, val codec: Codec[A])
    extends BlockingQueue[A] with JournaledBlockingQueueMixin[A] {

  def get(): Future[Option[A]] = get(100.days.fromNow)

  def get(deadline: Time): Future[Option[A]] = {
    reader.get(Some(deadline)).map { optItem =>
      optItem.map { item =>
        reader.commit(item.id)
        codec.decode(item.data)
      }
    }
  }

  def poll(): Future[Option[A]] = {
    reader.get(None).map { optItem =>
      optItem.map { item =>
        reader.commit(item.id)
        codec.decode(item.data)
      }
    }
  }

  def pollIf(predicate: A => Boolean): Future[Option[A]] = {
    throw new Exception("Unsupported operation")
  }
}

private[libkestrel] class TransactionalJournaledBlockingQueue[A <: AnyRef](
    val queue: JournaledQueue, val codec: Codec[A])
    extends TransactionalBlockingQueue[A] with JournaledBlockingQueueMixin[A] {

  def get(): Future[Option[Transaction[A]]] = get(100.days.fromNow)

  def get(deadline: Time): Future[Option[Transaction[A]]] = {
    reader.get(Some(deadline)).map { optItem =>
      optItem.map { queueItem =>
        new Transaction[A] {
          val item = codec.decode(queueItem.data)
          def commit() { reader.commit(queueItem.id) }
          def rollback() { reader.unget(queueItem.id) }
        }
      }
    }
  }

  def poll(): Future[Option[Transaction[A]]] = {
    reader.get(None).map { optItem =>
      optItem.map { queueItem =>
        new Transaction[A] {
          val item = codec.decode(queueItem.data)
          def commit() { reader.commit(queueItem.id) }
          def rollback() { reader.unget(queueItem.id) }
        }
      }
    }
  }
}
