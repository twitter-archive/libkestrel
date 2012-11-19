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

import java.util.concurrent.locks.ReentrantReadWriteLock

trait ReadWriteLock {
  private[this] val lock = new ReentrantReadWriteLock(true)

  def withWriteLock[A](f: => A) = {
    val writeLock = lock.writeLock
    writeLock.lock()
    try {
      f
    } finally {
      writeLock.unlock()
    }
  }

  def withDowngradeableWriteLock[A](f: (Function0[Unit]) => A) = {
    val writeLock = lock.writeLock
    val readLock = lock.readLock
    val downgrade = () => {
      readLock.lock()
      writeLock.unlock()
    }

    writeLock.lock()
    try {
      f(downgrade)
    } finally {
      if (writeLock.isHeldByCurrentThread) {
        // did not downgrade
        writeLock.unlock()
      } else {
        readLock.unlock()
      }
    }
  }

  def withReadLock[A](f: => A) = {
    val readLock = lock.readLock
    readLock.lock()
    try {
      f
    } finally {
      readLock.unlock()
    }
  }
}
