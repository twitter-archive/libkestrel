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

import com.twitter.concurrent.Serialized
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util._
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable
import scala.collection.JavaConverters._

case class JournaledQueueReaderConfig(
  maxItems: Int = Int.MaxValue,
  maxSize: StorageUnit = Long.MaxValue.bytes,
  maxMemorySize: StorageUnit = 128.megabytes,
  maxAge: Option[Duration] = None,
  fullPolicy: ConcurrentBlockingQueue.FullPolicy = ConcurrentBlockingQueue.FullPolicy.RefusePuts,
  processExpiredItem: (QueueItem) => Unit = { _ => },
  maxExpireSweep: Int = Int.MaxValue,

  // counters
  incrExpiredCount: () => Unit = { () => }
)

case class JournaledQueueConfig(
  name: String,
  maxItemSize: StorageUnit = Long.MaxValue.bytes,
  journaled: Boolean = true,
  journalSize: StorageUnit = 16.megabytes,
  syncJournal: Duration = Duration.MaxValue,
  saveArchivedJournals: Option[File] = None,

  readerConfigs: Map[String, JournaledQueueReaderConfig] = Map.empty,
  defaultReaderConfig: JournaledQueueReaderConfig = new JournaledQueueReaderConfig()
)

class JournaledQueue[A](config: JournaledQueueConfig, path: File, timer: Timer) {
  private[this] val log = Logger.get(getClass)

  private[this] val journal = if (config.journaled) {
    Some(new Journal(path, config.name, config.journalSize, timer, config.syncJournal))
  } else {
    None
  }

  @volatile private[this] var closed = false

  @volatile private[this] var readerMap = immutable.Map.empty[String, Reader]

  journal.foreach { j =>
    j.readerMap.foreach { case (name, _) => reader(name) }
  }

  def reader(name: String) = {
    readerMap.get(name).getOrElse {
      synchronized {
        readerMap.get(name).getOrElse {
          val readerConfig = config.readerConfigs.get(name).getOrElse(config.defaultReaderConfig)
          val reader = new Reader(name, readerConfig)
          journal.foreach { j => reader.loadFromJournal(j.reader(name)) }
          readerMap += (name -> reader)
          reader
        }
      }
    }
  }

  /*
  def put(data: Array[Byte], addTime: Time, expireTime: Option[Time]): Future[Boolean] = {
    if (closed || data.size > config.maxItemSize.inBytes) return Future(false)
    // if any reader is full, the put is rejected.
    if (readerMap.values.exists { r => !r.canPut }) return Future(false)
    journal.map { j =>
      j.put(data, addTime, expireTime).map { case (item, syncFuture) =>

      }
    }.getOrElse {
      // no journal

    }
  }
*/

  class Reader(name: String, readerConfig: JournaledQueueReaderConfig) extends Serialized {
    val journalReader = journal.map { _.reader(name) }
    private[this] val queue = ConcurrentBlockingQueue[QueueItem](timer)

    @volatile var items = 0
    @volatile var bytes = 0L
    @volatile var memoryBytes = 0L
    @volatile var age = 0.nanoseconds

    private val openReads = new ConcurrentHashMap[Long, QueueItem]()

    // visibility into how many items (and bytes) are in open reads
    def openItems = openReads.values.size
    def openBytes = openReads.values.asScala.foldLeft(0L) { _ + _.data.size }

    /*
     * in order to reload the contents of a queue at startup, we need to:
     *   - read the last saved state (head id, done ids)
     *   - start at the head, and read in items (ignoring ones already done) until we hit the
     *     memory limit for read-behind. if we hit that limit:
     *       - count the items left in the current journal file
     *       - add in the summarized counts from the remaining journal files, if any.
     */
    private[libkestrel] def loadFromJournal(j: Journal#Reader) {
      log.info("Replaying contents of %s+%s", config.name, name)
      j.readState()
      val scanner = new j.Scanner(j.head)

      var inReadBehind = false
      var lastId = 0L
      var optItem = scanner.next()
      while (optItem.isDefined) {
        val item = optItem.get
        lastId = item.id
        if (!inReadBehind) {
          queue.put(item)
          memoryBytes += item.data.size
        }
        items += 1
        bytes += item.data.size
        if (bytes >= readerConfig.maxMemorySize.inBytes && !inReadBehind) {
          j.startReadBehind(item.id)
          inReadBehind = true
        }
        optItem = scanner.next()
      }
      scanner.end()
      if (inReadBehind) {
        // count items/bytes from remaining journals.
        journal.get.fileInfosAfter(lastId).foreach { info =>
          items += info.items
          bytes += info.bytes
        }
      }

      log.info("Replaying contents of %s+%s done: %d items, %s, %s in memory",
        config.name, name, items, bytes.bytes.toHuman, memoryBytes.bytes.toHuman)
    }

    def checkpoint() {
      journalReader.foreach { _.checkpoint() }
    }

    /*
     * x if the queue is full, discard or fail.
     * - check if we should enter read-behind.
     * - _add:
     *   - discard any expired items
     *   - add to memory if not in read-behind
     *   - incr totalItems, queueSize, queueLength
     * - wake up any waiters
     */

    def canPut: Boolean = {
      readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest ||
        (items < readerConfig.maxItems && bytes < readerConfig.maxSize.inBytes)
    }

    /*
    def put(item: QueueItem): Boolean = {
      serialized {
        // we've already checked canPut by here, but we may still drop the oldest item.
        if (readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest &&
            (items >= readerConfig.maxItems || bytes >= readerConfig.maxSize.inBytes)) {
          // FIXME remove.
        }



              log.info("Entering read-behind for %s+%s", queueName, name)

      }


    }
    */

    private[this] def hasExpired(startTime: Time, expireTime: Option[Time], now: Time): Boolean = {
      val adjusted = if (readerConfig.maxAge.isDefined) {
        val maxExpiry = startTime + readerConfig.maxAge.get
        if (expireTime.isDefined) Some(expireTime.get min maxExpiry) else Some(maxExpiry)
      } else {
        expireTime
      }
      adjusted.isDefined && adjusted.get < now
    }

    // check the in-memory portion of the queue and discard anything that's expired.
    def discardExpired(max: Int): Int = {
      val now = Time.now
      var removedItems = 0
      var removedBytes = 0
      val removedIds = new ItemIdList()
      var item = queue.pollIf { item => hasExpired(item.addTime, item.expireTime, now) }
      while (item.isDefined && removedItems < max) {
        readerConfig.processExpiredItem(item.get)
        readerConfig.incrExpiredCount()
        removedItems += 1
        removedBytes += item.get.data.size
        removedIds.add(item.get.id)
        item = queue.pollIf { item => hasExpired(item.addTime, item.expireTime, now) }
      }
      if (removedItems > 0) {
        serialized {
          items -= removedItems
          bytes -= removedBytes
          memoryBytes -= removedBytes
          journalReader.foreach { j =>
            removedIds.popAll().foreach { id =>
              j.commit(id)
            }
          }
          fillReadBehind()
        }
      }
      removedItems
    }

    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    // always call this from within a serialized block.
    private[this] def fillReadBehind() {
      journalReader.foreach { j =>
        while (j.inReadBehind && memoryBytes < readerConfig.maxMemorySize.inBytes) {
          j.nextReadBehind().foreach { item =>
            queue.put(item)
            memoryBytes += item.data.size
          }
        }
      }
    }

    /**
     * Remove and return an item from the queue, if there is one.
     * If no deadline is given, an item is only returned if one is immediately available.
     */
    def get(deadline: Option[Time]): Future[Option[QueueItem]] = {
      if (closed) return Future.value(None)
      discardExpired(readerConfig.maxExpireSweep)
      val future = deadline match {
        case Some(d) => queue.get(d)
        case None => Future.value(queue.poll())
      }
      future.flatMap { optItem =>
        optItem match {
          case None => Future.value(None)
          case s @ Some(item) => {
            if (hasExpired(item.addTime, item.expireTime, Time.now)) {
              // try again.
              get(deadline)
            } else {
              age = Time.now - item.addTime
              openReads.put(item.id, item)
              Future.value(s)
            }
          }
        }
      }
    }

    /**
     * Commit an item that was previously fetched via `get`.
     * This is required to actually remove an item from the queue.
     */
    def commit(id: Long) {
      if (closed) return
      val item = openReads.remove(id)
      if (item eq null) {
        log.error("Tried to commit unknown item %d on %s+%s", id, config.name, name)
        return
      }

      serialized {
        journalReader.foreach { _.commit(item.id) }
        items -= 1
        bytes -= item.data.size
        memoryBytes -= item.data.size
        fillReadBehind()
      }
    }

    /**
     * Return an uncommited "get" to the head of the queue and give it to someone else.
     */
    def unget(id: Long) {
      if (closed) return
      val item = openReads.remove(id)
      if (item eq null) {
        log.error("Tried to uncommit unknown item %d on %s+%s", id, config.name, name)
        return
      }
      queue.putHead(item)
    }

    /**
     * Peek at the head item in the queue, if there is one.
     */
    def peek(): Future[Option[QueueItem]] = {
      val future = get(None)
      future.map { optItem =>
        if (optItem.isDefined) unget(optItem.get.id)
      }
      future
    }
  }




/*
 * x bail if queue closed or item too large.
 * - if any reader is too full, discard or fail.
 * - check rotate journal.
 * - check if any readers should enter read-behind.
 * - _add:
 *   - discard any expired items
 *   - add to memory if not in read-behind
 *   - incr totalItems, queueSize, queueLength
 * - journal.add
 * - wake up any waiters
 *

  def add(value: Array[Byte], expiry: Option[Time], xid: Option[Int]): Boolean = {
    val future = synchronized {

            if (closed || value.size > config.maxItemSize.inBytes) return false
      if (config.fanoutOnly && !isFanout) return true
      while (queueLength >= config.maxItems || queueSize >= config.maxSize.inBytes) {
        if (!config.discardOldWhenFull) return false
        _remove(false, None)
        totalDiscarded.incr()
        if (config.keepJournal) journal.remove()
      }

      val item = QItem(addTime, adjustExpiry(Time.now, expiry), value, 0)
      if (config.keepJournal) {
        checkRotateJournal()
        if (!journal.inReadBehind && (queueSize >= config.maxMemorySize.inBytes)) {
          log.info("Dropping to read-behind for queue '%s' (%s)", name, queueSize.bytes.toHuman())
          journal.startReadBehind()
        }
      }

      val now = Time.now
      val item = QItem(now, adjustExpiry(now, expiry), value, 0)
      if (xid != None) openTransactions.remove(xid.get)
      _add(item)
      if (config.keepJournal) {
        xid match {
          case None => journal.add(item)
          case Some(xid) => journal.continue(xid, item)
        }
      } else {
        Future.void()
      }
    }
    waiters.trigger()
    // for now, don't wait:
    //future()
    true
  }
  */
}
