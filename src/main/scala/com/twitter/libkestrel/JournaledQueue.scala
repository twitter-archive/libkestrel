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
import java.util.concurrent.atomic.AtomicLong

trait Codec[A] {
  def encode(item: A): Array[Byte]
  def decode(data: Array[Byte]): A
}

case class JournaledQueueReaderConfig(
  maxItems: Int = Int.MaxValue,
  maxSize: StorageUnit = Long.MaxValue.bytes,
  maxMemorySize: StorageUnit = 128.megabytes,
  maxAge: Option[Duration] = None,
  fullPolicy: ConcurrentBlockingQueue.FullPolicy = ConcurrentBlockingQueue.FullPolicy.RefusePuts,
  processExpiredItem: (QueueItem) => Unit = { _ => },
  maxExpireSweep: Int = Int.MaxValue,

  // counters
  incrExpiredCount: () => Unit = { () => },
  incrDiscardedCount: () => Unit = { () => },
  incrPutCount: () => Unit = { () => }
)

case class JournaledQueueConfig(
  name: String,
  maxItemSize: StorageUnit = Long.MaxValue.bytes,
  journaled: Boolean = true,
  journalSize: StorageUnit = 16.megabytes,
  syncJournal: Duration = Duration.MaxValue,
  saveArchivedJournals: Option[File] = None,
  checkpointTimer: Duration = 1.second,

  readerConfigs: Map[String, JournaledQueueReaderConfig] = Map.empty,
  defaultReaderConfig: JournaledQueueReaderConfig = new JournaledQueueReaderConfig()
)

class JournaledQueue(config: JournaledQueueConfig, path: File, timer: Timer) extends Serialized {
  private[this] val log = Logger.get(getClass)

  private[this] val journal = if (config.journaled) {
    Some(new Journal(path, config.name, config.journalSize, timer, config.syncJournal,
      config.saveArchivedJournals))
  } else {
    None
  }

  @volatile private[this] var closed = false

  @volatile private[this] var readerMap = immutable.Map.empty[String, Reader]

  @volatile private[this] var nonJournalId = 0L

  journal.foreach { j =>
    j.readerMap.foreach { case (name, _) => reader(name) }
  }

  // checkpoint readers on a schedule.
  timer.schedule(config.checkpointTimer) { checkpoint() }

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

  /**
   * Save the state of all readers.
   */
  def checkpoint() {
    journal.foreach { _.checkpoint() }
  }

  /**
   * Close this queue. Any further operations will fail.
   */
  def close() {
    closed = true
    readerMap.values.foreach { _.close() }
  }

  /**
   * Close this queue and also erase any journal files.
   */
  def erase() {
    close()
    journal.foreach { _.erase() }
  }

  /**
   * Put an item into the queue. If the put fails, `None` is returned. On success, the item has
   * been added to every reader, and the returned future will trigger when the journal has been
   * written to disk.
   */
  def put(data: Array[Byte], addTime: Time, expireTime: Option[Time]): Option[Future[Unit]] = {
    if (closed || data.size > config.maxItemSize.inBytes) return None
    // if any reader is rejecting puts, the put is rejected.
    if (readerMap.values.exists { r => !r.canPut }) return None

    Some(journal.map { j =>
      j.put(data, addTime, expireTime, { item =>
        readerMap.values.foreach { _.put(item) }
      }).flatMap { case (item, syncFuture) =>
        syncFuture
      }
    }.getOrElse {
      // no journal
      serialized {
        nonJournalId += 1
        val item = QueueItem(nonJournalId, addTime, expireTime, data)
        readerMap.values.foreach { _.put(item) }
      }
    })
  }

  def toBlockingQueue[A <: AnyRef](implicit codec: Codec[A]): BlockingQueue[A] = {
    val reader = JournaledQueue.this.reader("")

    new BlockingQueue[A] {
      def put(item: A) = {
        val rv = JournaledQueue.this.put(codec.encode(item), Time.now, None)
        rv.isDefined && { rv.get.get(); true }
      }

      def putHead(item: A) {
        throw new Exception("Unsupported operation")
      }

      def size: Int = reader.items

      def get(): Future[Option[A]] = get(100.days.fromNow)

      def get(deadline: Time): Future[Option[A]] = {
        reader.get(Some(deadline)).map { optItem =>
          optItem.map { item =>
            reader.commit(item.id)
            codec.decode(item.data)
          }
        }
      }

      def poll(): Option[A] = {
        reader.get(None).map { optItem =>
          optItem.map { item =>
            reader.commit(item.id)
            codec.decode(item.data)
          }
        }()
      }

      def pollIf(predicate: A => Boolean): Option[A] = {
        throw new Exception("Unsupported operation")
      }

      def toDebug: String = {
        "<JournaledQueue: size=%d bytes=%d age=%s>".format(reader.items, reader.bytes, reader.age)
      }

      def close() {
        JournaledQueue.this.close()
      }
    }
  }


  class Reader(name: String, readerConfig: JournaledQueueReaderConfig) extends Serialized {
    val journalReader = journal.map { _.reader(name) }
    private[this] val queue = ConcurrentBlockingQueue[QueueItem](timer)

    @volatile var items = 0
    @volatile var bytes = 0L
    @volatile var memoryBytes = 0L
    @volatile var age = 0.nanoseconds
    @volatile var discarded = 0L
    @volatile var expired = 0L

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

    def canPut: Boolean = {
      readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest ||
        (items < readerConfig.maxItems && bytes < readerConfig.maxSize.inBytes)
    }

    // this happens within the serialized block of a put.
    private[libkestrel] def put(item: QueueItem) {
      discardExpired(readerConfig.maxExpireSweep)
      serialized {
        // we've already checked canPut by here, but we may still drop the oldest item(s).
        while (readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest &&
               (items >= readerConfig.maxItems || bytes >= readerConfig.maxSize.inBytes)) {
          queue.poll().foreach { item =>
            readerConfig.incrDiscardedCount()
            discarded += 1
            commitItem(item)
          }
        }

        val inReadBehind = journalReader.map { j =>
          if (j.inReadBehind && item.id > j.readBehindId) {
            true
          } else if (!j.inReadBehind && memoryBytes >= readerConfig.maxMemorySize.inBytes) {
            log.info("Dropping to read-behind for queue '%s+%s' (%s)", config.name, name,
              bytes.bytes.toHuman)
            j.startReadBehind(item.id - 1)
            true
          } else {
            false
          }
        }.getOrElse(false)

        if (!inReadBehind) {
          queue.put(item)
          memoryBytes += item.data.size
        }
        items += 1
        bytes += item.data.size
        readerConfig.incrPutCount()
      }
    }

    private[this] def hasExpired(startTime: Time, expireTime: Option[Time], now: Time): Boolean = {
      val adjusted = if (readerConfig.maxAge.isDefined) {
        val maxExpiry = startTime + readerConfig.maxAge.get
        if (expireTime.isDefined) Some(expireTime.get min maxExpiry) else Some(maxExpiry)
      } else {
        expireTime
      }
      adjusted.isDefined && adjusted.get <= now
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
        if (removedItems < max) {
          item = queue.pollIf { item => hasExpired(item.addTime, item.expireTime, now) }
        }
      }
      if (removedItems > 0) {
        serialized {
          items -= removedItems
          expired += removedItems
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

    def inReadBehind = journalReader.map { _.inReadBehind }.getOrElse(false)

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
        commitItem(item)
      }
    }

    // serialized
    private[this] def commitItem(item: QueueItem) {
      journalReader.foreach { _.commit(item.id) }
      items -= 1
      bytes -= item.data.size
      memoryBytes -= item.data.size
      if (items == 0) age = 0.milliseconds
      fillReadBehind()
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

    /**
     * Drain all items from this reader.
     */
    def flush() {
      serialized {
        journalReader.foreach { j =>
          j.flush()
          j.checkpoint()
        }
        while (queue.poll().isDefined) { }
        items = 0
        bytes = 0
        memoryBytes = 0
        age = 0.nanoseconds
      }
    }

    def close() {
      journalReader.foreach { j =>
        j.checkpoint()()
        j.close()
      }
    }
  }
}
