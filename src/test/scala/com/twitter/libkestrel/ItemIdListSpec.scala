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

import org.specs.Specification

class ItemIdListSpec extends Specification {
  "ItemIdList" should {
    "add and pop" in {
      val x = new ItemIdList()
      x.add(Seq(5L, 4L))
      x.size mustEqual 2
      x.pop() mustEqual Some(5L)
      x.pop() mustEqual Some(4L)
      x.pop() mustEqual None
    }

    "remove from the middle" in {
      val x = new ItemIdList()
      x.add(Seq(7L, 6L, 5L, 4L, 3L, 2L))
      x.pop() mustEqual Some(7L)
      x.remove(Set(5L, 4L, 2L)) mustEqual Set(5L, 4L, 2L)
      x.popAll() mustEqual Seq(6L, 3L)
    }

    "remove and pop combined" in {
      val x = new ItemIdList()
      x.add(Seq(7L, 6L, 5L, 4L, 3L, 2L))
      x.remove(Set(6L)) mustEqual Set(6L)
      x.pop() mustEqual Some(7L)
      x.pop() mustEqual Some(5L)
      x.popAll() mustEqual Seq(4L, 3L, 2L)
    }

    "remove individually" in {
      val x = new ItemIdList()
      x.add(Seq(7L, 6L, 5L, 4L, 3L, 2L))
      x.remove(6L) mustEqual true
      x.remove(4L) mustEqual true
      x.remove(4L) mustEqual false
      x.size mustEqual 4
      x.toSeq.toList mustEqual List(7L, 5L, 3L, 2L)

      x.remove(5L) mustEqual true
      x.remove(7L) mustEqual true
      x.size mustEqual 2
      x.toSeq.toList mustEqual List(3L, 2L)
    }
  }
}
