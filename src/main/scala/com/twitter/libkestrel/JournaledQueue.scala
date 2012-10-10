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
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable
import scala.collection.JavaConverters._
import config._

/**
 * An optionally-journaled queue built on top of `ConcurrentBlockingQueue` that may have multiple
 * "readers".
 *
 * When an item is added to a queue, it's journaled and passed on to any readers. There is always
 * at least one reader, and the reader contains the actual in-memory queue. If there are multiple
 * readers, they behave as multiple independent queues, each receiving a copy of each item added,
 * but sharing a single journal. They may have different policies on memory use, queue size
 * limits, and item expiration.
 *
 * Items are read only from readers. When an item is available, it's set aside as an "open read",
 * but not committed to the journal. A separate call is made to either commit the item or abort
 * it. Aborting an item returns it to the head of the queue to be given to the next consumer.
 *
 * Periodically each reader records its state in a separate checkpoint journal. When initialized,
 * if a journal already exists for a queue and its readers, each reader restores itself from this
 * saved state. If the queues were not shutdown cleanly, the state files may be out of date and
 * items may be replayed. Care is taken never to let any of the journal files be corrupted or in a
 * non-recoverable state. In case of error, the choice is always made to possibly replay items
 * instead of losing them.
 *
 * @param config a set of configuration parameters for the queue
 * @param path the folder to store journals in
 * @param timer a Timer to use for triggering timeouts on reads
 * @param scheduler a service to use for scheduling periodic disk syncs
 */
class JournaledQueue(
  val config: JournaledQueueConfig, path: File, timer: Timer, scheduler: ScheduledExecutorService
) extends Serialized {
  private[this] val log = Logger.get(getClass)

  private[this] val NAME_REGEX = """[^A-Za-z0-9:_-]""".r
  if (NAME_REGEX.findFirstIn(config.name).isDefined) {
    throw new Exception("Illegal queue name: " + config.name)
  }

  private[this] val journal = if (config.journaled) {
    Some(new Journal(path, config.name, config.journalSize, scheduler, config.syncJournal,
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

  /**
   * A read-only view of all the current `Reader` objects for this queue.
   */
  def readers = readerMap.values

  /**
   * Total number of items across every reader queue being fed by this queue.
   */
  def items = readerMap.values.foldLeft(0L) { _ + _.items }

  /**
   * Total number of bytes of data across every reader queue being fed by this queue.
   */
  def bytes = readerMap.values.foldLeft(0L) { _ + _.bytes }

  /**
   * Total number of bytes of data used by the on-disk journal.
   */
  def journalBytes = {
    journal map { _.journalSize } getOrElse(0L)
  }

  /**
   * Get the named reader. If this is a normal (single reader) queue, the default reader is named
   * "". If any named reader is created, the default reader is converted to that name and there is
   * no longer a default reader.
   */
  def reader(name: String): Reader = {
    readerMap.get(name).getOrElse {
      synchronized {
        readerMap.get(name).getOrElse {
          if (readerMap.size >= 1 && name == "") {
            throw new Exception("Fanout queues don't have a default reader")
          }
          val readerConfig = config.readerConfigs.get(name).getOrElse(config.defaultReaderConfig)
          log.info("Creating reader queue %s+%s", config.name, name)
          val reader = new Reader(name, readerConfig)
          journal.foreach { j => reader.loadFromJournal(j.reader(name)) }
          readerMap += (name -> reader)
          reader.catchUp()

          if (name != "") {
            readerMap.get("") foreach { r =>
              // kill the default reader.
              readerMap -= ""
              r.close()
              journal.foreach { _.dropReader("") }
            }
          }

          reader
        }
      }
    }
  }

  def dropReader(name: String) {
    readerMap.get(name) foreach { r =>
      synchronized {
        log.info("Destroying reader queue %s+%s", config.name, name)
        readerMap -= name
        r.close()
        journal.foreach { j => j.dropReader(name) }
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
    journal.foreach { _.close() }
  }

  /**
   * Close this queue and also erase any journal files.
   */
  def erase() {
    closed = true
    readerMap.values.foreach { _.close() }
    journal.foreach { _.erase() }
  }

  /**
   * Erase any items in any reader queue.
   */
  def flush() {
    readerMap.values.foreach { _.flush() }
  }

  /**
   * Do a sweep of each reader, and discard any expired items.
   */
  def discardExpired() {
    readerMap.values foreach { _.discardExpired() }
  }

  /**
   * Put an item into the queue. If the put fails, `None` is returned. On success, the item has
   * been added to every reader, and the returned future will trigger when the journal has been
   * written to disk.
   */
  def put(
    data: ByteBuffer, addTime: Time, expireTime: Option[Time] = None, errorCount: Int = 0
  ): Option[Future[Unit]] = {
    if (closed) return None
    if (data.remaining > config.maxItemSize.inBytes) {
      log.debug("Rejecting put to %s: item too large (%s).", config.name, data.remaining.bytes.toHuman)
      return None
    }
    // if any reader is rejecting puts, the put is rejected.
    if (readerMap.values.exists { r => !r.canPut }) {
      log.debug("Rejecting put to %s: reader is full.", config.name)
      return None
    }

    Some(journal.map { j =>
      j.put(data, addTime, expireTime, errorCount, { item =>
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

  /**
   * Put an item into the queue. If the put fails, `None` is returned. On success, the item has
   * been added to every reader, and the returned future will trigger when the journal has been
   * written to disk.
   *
   * This method really just calls the other `put` method with the parts of the queue item,
   * ignoring the given item id. The written item will have a new id.
   */
  def put(item: QueueItem): Option[Future[Unit]] = {
    put(item.data, item.addTime, item.expireTime, item.errorCount)
  }

  /**
   * Generate a debugging string (python-style) for this queue, with key stats.
   */
  def toDebug: String = {
    "<JournaledQueue: name=%s items=%d bytes=%d journalBytes=%s readers=(%s)%s>".format(
      config.name, items, bytes, journalBytes, readerMap.values.map { _.toDebug }.mkString(", "),
      (if (closed) " closed" else ""))
  }

  /**
   * Create a wrapper object for this queue that implements the `BlockingQueue` trait. Not all
   * operations are supported: specifically, `putHead` and `pollIf` throw an exception if called.
   */
  def toBlockingQueue[A <: AnyRef](implicit codec: Codec[A]): BlockingQueue[A] =
    new JournaledBlockingQueue(this, codec)

  /**
   * Create a wrapper object for this queue that implements the `TransactionalBlockingQueue` trait.
   */
  def toTransactionalBlockingQueue[A <: AnyRef](implicit codec: Codec[A]): TransactionalBlockingQueue[A] =
    new TransactionalJournaledBlockingQueue(this, codec)

  /**
   * A reader for this queue, which has its own head pointer and list of open reads.
   */
  class Reader(private[libkestrel] var name: String, val readerConfig: JournaledQueueReaderConfig)
    extends Serialized
  {
    val journalReader = journal.map { _.reader(name) }
    private[libkestrel] val queue = ConcurrentBlockingQueue[QueueItem](timer)

    @volatile var items = 0
    @volatile var bytes = 0L
    @volatile var memoryItems = 0
    @volatile var memoryBytes = 0L
    @volatile var expired = 0L

    private val openReads = new ConcurrentHashMap[Long, QueueItem]()

    /**
     * When was this reader created?
     */
    val createTime = Time.now

    /**
     * Number of open (uncommitted) reads.
     */
    def openItems = openReads.values.size

    /**
     * Byte count of open (uncommitted) reads.
     */
    def openBytes = openReads.values.asScala.foldLeft(0L) { _ + _.dataSize }

    /**
     * Total number of items ever added to this queue.
     */
    val putCount = new AtomicLong(0)

    /**
     * Total number of bytes ever added to this queue.
     */
    val putBytes = new AtomicLong(0)

    /**
     * Total number of items ever successfully fetched from this queue.
     */
    val getHitCount = new AtomicLong(0)

    /**
     * Total number of times a fetch from this queue failed because no item was available.
     */
    val getMissCount = new AtomicLong(0)

    /**
     * Total number of items ever expired from this queue.
     */
    val expiredCount = new AtomicLong(0)

    /**
     * Total number of items ever discarded from this queue.
     */
    val discardedCount = new AtomicLong(0)

    /**
     * Total number of reads opened (transactions) on this queue.
     */
    val openedItemCount = new AtomicLong(0)

    /**
     * Total number of canceled reads (transactions) on this queue.
     */
    val canceledItemCount = new AtomicLong(0)

    /**
     * Number of consumers waiting for an item to arrive.
     */
    def waiterCount: Int = queue.waiterCount

    /**
     * Number of items dropped because the queue was full.
     */
    def droppedCount: Int = queue.droppedCount.get

    /**
     * Number of times this queue has been flushed.
     */
    val flushCount = new AtomicLong(0)

    /**
     * FQDN for this reader, which is usually of the form "queue_name+reader_name", but will just
     * be "queue_name" for the default reader.
     */
    def fullname: String = {
      if (name == "") config.name else (config.name + "+" + name)
    }

    def writer: JournaledQueue = JournaledQueue.this

    /**
     * Age of the oldest item in this queue or 0 if the queue is empty.
     */
    def age: Duration =
      queue.peekOldest match {
        case Some(item) => Time.now - item.addTime
        case None => 0.nanoseconds
      }

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
      val scanner = new j.Scanner(j.head, followFiles = true, logIt = false)

      var inReadBehind = false
      var lastId = 0L
      var optItem = scanner.next()
      while (optItem.isDefined) {
        val item = optItem.get
        lastId = item.id
        if (!inReadBehind) {
          queue.put(item)
          memoryItems += 1
          memoryBytes += item.dataSize
        }
        items += 1
        bytes += item.dataSize
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

    def catchUp() {
      journalReader.foreach { _.catchUp() }
    }

    def checkpoint() {
      journalReader.foreach { _.checkpoint() }
    }

    def canPut: Boolean = {
      readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest ||
        (items < readerConfig.maxItems && bytes < readerConfig.maxSize.inBytes)
    }

    private[libkestrel] def dropOldest() {
      if (readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest &&
          (items > readerConfig.maxItems || bytes > readerConfig.maxSize.inBytes)) {
        queue.poll() map { itemOption =>
          itemOption foreach { item =>
            serialized {
              discardedCount.getAndIncrement()
              commitItem(item)
              dropOldest()
            }
          }
        }
      }
    }

    // this happens within the serialized block of a put.
    private[libkestrel] def put(item: QueueItem) {
      discardExpired()
      serialized {
        val inReadBehind = journalReader.map { j =>
          // if item.id <= j.readBehindId, fillReadBehind already saw this item.
          if (j.inReadBehind && item.id > j.readBehindId) {
            true
          } else if (!j.inReadBehind && memoryBytes >= readerConfig.maxMemorySize.inBytes) {
            log.info("Dropping to read-behind for queue '%s+%s' (%s) @ item %d",
              config.name, name, bytes.bytes.toHuman, item.id - 1)
            j.startReadBehind(item.id - 1)
            true
          } else {
            false
          }
        }.getOrElse(false)

        if (!inReadBehind) {
          queue.put(item.copy(data = item.data.duplicate()))
          memoryItems += 1
          memoryBytes += item.dataSize
        }
        items += 1
        bytes += item.dataSize
        putCount.getAndIncrement()
        putBytes.getAndAdd(item.dataSize)

        // we've already checked canPut by here, but we may still drop the oldest item(s).
        dropOldest()
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

    def discardExpired() {
      discardExpired(readerConfig.maxExpireSweep)
    }

    // check the in-memory portion of the queue and discard anything that's expired.
    def discardExpired(max: Int) {
      if (max == 0) return
      queue.pollIf { item => hasExpired(item.addTime, item.expireTime, Time.now) } map { itemOption =>
        itemOption foreach { item =>
          readerConfig.processExpiredItem(item)
          expiredCount.getAndIncrement()
          serialized {
            items -= 1
            bytes -= item.dataSize
            memoryItems -= 1
            memoryBytes -= item.dataSize
            expired += 1
            journalReader.foreach { _.commit(item.id) }
            fillReadBehind()
          }
          discardExpired(max - 1)
        }
      }
    }

    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    // always call this from within a serialized block.
    private[this] def fillReadBehind() {
      journalReader.foreach { j =>
        while (j.inReadBehind && memoryBytes < readerConfig.maxMemorySize.inBytes) {
          if (bytes < readerConfig.maxMemorySize.inBytes) {
            j.endReadBehind()
          } else {
            j.nextReadBehind().foreach { item =>
              queue.put(item)
              memoryItems += 1
              memoryBytes += item.dataSize
            }
          }
        }
      }
    }

    def inReadBehind = journalReader.map { _.inReadBehind }.getOrElse(false)

    /**
     * Remove and return an item from the queue, if there is one.
     * If no deadline is given, an item is only returned if one is immediately available.
     */
    def get(deadline: Option[Deadline], peeking: Boolean = false): Future[Option[QueueItem]] = {
      if (closed) return Future.value(None)
      discardExpired()
      val startTime = Time.now
      val future = deadline match {
        case Some(d) => queue.get(d)
        case None => queue.poll()
      }
      future.flatMap { optItem =>
        optItem match {
          case None => {
            readerConfig.timeoutLatency(this, Time.now - startTime)
            getMissCount.getAndIncrement()
            Future.value(None)
          }
          case s @ Some(item) => {
            if (hasExpired(item.addTime, item.expireTime, Time.now)) {
              // try again.
              get(deadline, peeking)
            } else {
              readerConfig.deliveryLatency(this, Time.now - item.addTime)
              getHitCount.getAndIncrement()
              if (peeking) {
                queue.putHead(item)
                Future.value(Some(item.copy(data = item.data.duplicate())))
              } else {
                openedItemCount.incrementAndGet
                openReads.put(item.id, item)
                item.data.mark()
                Future.value(s)
              }
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
      bytes -= item.dataSize
      memoryItems -= 1
      memoryBytes -= item.dataSize
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
      canceledItemCount.incrementAndGet
      item.data.reset()
      val newItem = item.copy(errorCount = item.errorCount + 1)
      if (readerConfig.errorHandler(newItem)) {
        commitItem(newItem)
      } else {
        queue.putHead(newItem)
      }
    }

    /**
     * Peek at the head item in the queue, if there is one.
     */
    def peek(deadline: Option[Deadline]): Future[Option[QueueItem]] = {
      get(deadline, true)
    }

    /**
     * Drain all items from this reader.
     */
    def flush(): Future[Unit] = {
      serialized {
        journalReader.foreach { j =>
          j.flush()
          j.checkpoint()
        }
        queue.flush()
        items = 0
        bytes = 0
        memoryItems = 0
        memoryBytes = 0
        flushCount.getAndIncrement()
      }
    }

    def close() {
      journalReader.foreach { j =>
        j.checkpoint() respond {
          case _ => j.close()
        }
      }
    }

    def evictWaiters() {
      queue.evictWaiters()
    }

    /**
     * Check if this Queue is eligible for expiration by way of it being empty
     * and its age being greater than or equal to maxQueueAge
     */
    def isReadyForExpiration: Boolean = {
      readerConfig.maxQueueAge.map { age =>
        Time.now > createTime + age && queue.size == 0
      }.getOrElse(false)
    }

    def toDebug: String = {
      "<JournaledQueue#Reader: name=%s items=%d bytes=%d mem_items=%d mem_bytes=%d age=%s queue=%s open=%s>".format(
        name, items, bytes, memoryItems, memoryBytes, age, queue.toDebug, openReads.keys.asScala.toList.sorted)
    }
  }
}
