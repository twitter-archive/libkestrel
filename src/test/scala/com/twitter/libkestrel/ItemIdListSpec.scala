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

import org.scalatest.{AbstractSuite, Spec, Suite}
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class ItemIdListSpec extends Spec {
  describe("ItemIdList") {
    it("add and pop") {
      val x = new ItemIdList()
      x.add(Seq(5L, 4L))
      assert(x.size === 2)
      assert(x.pop() === Some(5L))
      assert(x.pop() === Some(4L))
      assert(x.pop() === None)
    }

    it("remove from the middle") {
      val x = new ItemIdList()
      x.add(Seq(7L, 6L, 5L, 4L, 3L, 2L))
      assert(x.pop() === Some(7L))
      assert(x.remove(Set(5L, 4L, 2L)) === Set(5L, 4L, 2L))
      assert(x.popAll() === Seq(6L, 3L))
    }

    it("remove and pop combined") {
      val x = new ItemIdList()
      x.add(Seq(7L, 6L, 5L, 4L, 3L, 2L))
      assert(x.remove(Set(6L)) === Set(6L))
      assert(x.pop() === Some(7L))
      assert(x.pop() === Some(5L))
      assert(x.popAll() === Seq(4L, 3L, 2L))
    }

    it("remove individually") {
      val x = new ItemIdList()
      x.add(Seq(7L, 6L, 5L, 4L, 3L, 2L))
      assert(x.remove(6L) === true)
      assert(x.remove(4L) === true)
      assert(x.remove(4L) === false)
      assert(x.size === 4)
      assert(x.toSeq.toList === List(7L, 5L, 3L, 2L))

      assert(x.remove(5L) === true)
      assert(x.remove(7L) === true)
      assert(x.size === 2)
      assert(x.toSeq.toList === List(3L, 2L))
    }
  }
}
