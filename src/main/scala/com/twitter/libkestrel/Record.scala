/*
 * Copyright 2012 Twitter, Inc.
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

import com.twitter.conversions.storage._

sealed trait Record
object Record {
  // throw an error if any queue item is larger than this:
  val LARGEST_DATA = 16.megabytes

  private[libkestrel] val READ_HEAD = 0
  private[libkestrel] val PUT = 8
  private[libkestrel] val READ_DONE = 9

  case class Put(item: QueueItem) extends Record
  case class ReadHead(id: Long) extends Record
  case class ReadDone(ids: Seq[Long]) extends Record
  case class Unknown(command: Int) extends Record
}
