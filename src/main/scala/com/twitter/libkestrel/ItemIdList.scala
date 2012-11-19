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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.Set

/**
 * Set of ids that maintains insert order.
 *
 * The "set" property is not enforced but is assumed because these are meant to be item ids.
 * Good performance assumes that operations are usually add, pop, popAll, or a remove of most
 * or all of the items. (Remove is O(n) so if there are many items and only a few are being
 * removed, performance will be bad.) This is tuned toward the general case where a client will
 * have either very few items open, or will have many items open but will remove them all at
 * once.
 */
class ItemIdList {
  private[this] var _ids = new Array[Long](16)
  private[this] var head = 0
  private[this] var tail = 0

  def pop(count: Int): Seq[Long] = {
    if (count > tail - head) {
      Seq()
    } else {
      val rv = _ids.slice(head, head + count)
      head += count
      rv
    }
  }

  def pop(): Option[Long] = pop(1).headOption

  def add(id: Long) {
    if (head > 0) {
      compact()
    }
    if (tail == _ids.size) {
      val bigger = new Array[Long](_ids.size * 4)
      System.arraycopy(_ids, 0, bigger, 0, _ids.size)
      _ids = bigger
    }
    _ids(tail) = id
    tail += 1
  }

  def add(ids: Seq[Long]) {
    ids.foreach(add)
  }

  def size: Int = tail - head

  def popAll(): Seq[Long] = {
    val rv = _ids.slice(head, tail)
    head = 0
    tail = 0
    rv
  }

  def remove(ids: Set[Long]): Set[Long] = {
    var n = head
    val removed = new mutable.HashSet[Long]
    while (n < tail) {
      if (ids contains _ids(n)) {
        removed += _ids(n)
        _ids(n) = 0
      }
      n += 1
    }
    compact()
    removed.toSet
  }

  private[this] def find(id: Long, remove: Boolean): Boolean = {
    var n = head
    while (n < tail) {
      if (_ids(n) == id) {
        if (remove) {
          _ids(n) = 0
          compact()
        }
        return true
      }
      n += 1
    }
    false
  }

  def remove(id: Long) = find(id, true)

  def contains(id: Long) = find(id, false)

  // for tests:
  def toSeq: Seq[Long] = _ids.slice(head, tail)

  def compact() { compact(0, head) }

  override def toString() = "<ItemIdList: %s>".format(toSeq.sorted)

  @tailrec
  private[this] def compact(start: Int, h1: Int) {
    if (h1 == tail) {
      // string of zeros. flatten tail and done.
      head = 0
      tail = start
    } else {
      // now find string of filled items.
      var h2 = h1
      while (h2 < tail && _ids(h2) != 0) h2 += 1
      if (h1 != start && h2 != h1) System.arraycopy(_ids, h1, _ids, start, h2 - h1)
      val newStart = start + h2 - h1
      while (h2 < tail && _ids(h2) == 0) h2 += 1
      compact(newStart, h2)
    }
  }
}
