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

import org.scalatest.{BeforeAndAfter, Spec}

class ItemIdListSpec extends Spec with ResourceCheckingSuite with BeforeAndAfter{
  describe("ItemIdList") {
    var iil = new ItemIdList()

    before {
      iil = new ItemIdList()
    }

    it("should add an Integer to the list") {
      iil.add(3L)
      assert(iil.size === 1)
    }

    it("should add a sequence of Integers to the list") {
      iil.add(Seq(1L, 2L, 3L, 4L))
      assert(iil.size === 4)
    }

    it("should pop one item at a time") {
      iil.add(Seq(90L, 99L))
      assert(iil.pop() === Some(90L))
      assert(iil.pop() === Some(99L))
    }

    it("should pop None when there's nothing to pop") {
      assert(iil.pop() === None)
    }

    it("should pop all items from an index upward") {
      iil.add(Seq(100L, 200L, 300L, 400L))
      val expected = Seq(100L, 200L)
      val actual = iil.pop(2)
      assert(expected === actual)
    }

    it("should pop all items from the list") {
      val seq = Seq(12L, 13L, 14L)
      iil.add(seq)
      assert(seq === iil.popAll())
    }

    it("should return empty seq when pop's count is invalid") {
      assert(iil.pop(1) === Seq())
    }

    it("should remove a set of items from the list") {
      iil.add(Seq(19L, 7L, 20L, 22L))
      val expected = Set(7L, 20L, 22L)
      assert(expected === iil.remove(expected))
    }
  }
}
