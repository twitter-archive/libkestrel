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

case class QueueItem(
  id: Long,
  addTime: Time,
  expireTime: Option[Time],
  data: Array[Byte]
) {
  override def equals(other: Any) = {
    other match {
      case x: QueueItem => {
        (x eq this) ||
          (x.id == id && x.addTime == addTime && x.expireTime == expireTime && x.data.toList == data.toList)
      }
      case _ => false
    }
  }

  override def toString = {
    val hex = data.slice(0, data.size min 64).map { b => "%x".format(b) }.mkString(" ")
    "QueueItem(id=%d, addTime=%s, expireTime=%s, data=%s)".format(
      id, addTime.inNanoseconds, expireTime.map { _.inNanoseconds }, hex
    )
  }
}
