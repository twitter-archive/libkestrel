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

import com.twitter.util.Time
import java.nio.ByteBuffer

case class QueueItem(
  id: Long,
  addTime: Time,
  expireTime: Option[Time],
  data: ByteBuffer,
  errorCount: Int = 0
) {
  val dataSize = data.limit - data.position

  override def equals(other: Any) = {
    other match {
      case x: QueueItem => {
        (x eq this) ||
          (x.id == id && x.addTime == addTime && x.expireTime == expireTime &&
           x.data.duplicate.rewind == data.duplicate.rewind)
      }
      case _ => false
    }
  }

  override def toString = {
    val limit = data.limit min (data.position + 64)
    val hex = (data.position until limit).map { i => "%x".format(data.get(i)) }.mkString(" ")
    "QueueItem(id=%d, addTime=%s, expireTime=%s, data=%s, errors=%s)".format(
      id, addTime.inNanoseconds, expireTime.map { _.inNanoseconds }, hex, errorCount
    )
  }
}
