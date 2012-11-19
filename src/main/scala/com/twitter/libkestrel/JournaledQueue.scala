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

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util._
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService}
import java.util.concurrent.atomic.{AtomicInteger,AtomicLong}
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.JavaConverters._
import config._

/**
 * A journaled queue built on top of `ConcurrentBlockingQueue` that may have multiple "readers".
 *
 * When an item is added to a queue, it's journaled and passed on to any readers. There is always
 * at least one reader, and the reader contains the actual queue. If there are multiple readers,
 * they behave as multiple independent queues, each receiving a copy of each item added, but
 * sharing a single journal. They may have different policies on memory use, queue size limits,
 * and item expiration.
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
) {
  private[this] val log = Logger.get(getClass)

  private[this] val NAME_REGEX = """[^A-Za-z0-9:_-]""".r
  if (NAME_REGEX.findFirstIn(config.name).isDefined) {
    throw new Exception("Illegal queue name: " + config.name)
  }

  private[JournaledQueue] val journal =
    new Journal(path, config.name, config.journalSize, scheduler, config.syncJournal,
                config.saveArchivedJournals)

  @volatile private[this] var closed = false

  @volatile private[this] var readerMap = immutable.Map.empty[String, Reader]
  journal.readerMap.foreach { case (name, _) => reader(name) }

  // checkpoint readers on a schedule.
  timer.schedule(config.checkpointTimer) { checkpoint() }

  val name = config.name

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
  def journalBytes = journal.journalSize

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
          reader.loadFromJournal(journal.reader(name))
          readerMap += (name -> reader)
          reader.catchUp()

          if (name != "") {
            readerMap.get("") foreach { r =>
              // kill the default reader.
              readerMap -= ""
              journal.dropReader("")
            }
          }

          reader
        }
      }
    }
  }

  def dropReader(name: String) {
    synchronized {
      readerMap.get(name) foreach { r =>
        log.info("Destroying reader queue %s+%s", config.name, name)
        readerMap -= name
        r.close()
        journal.dropReader(name)
      }
    }
  }

  /**
   * Save the state of all readers.
   */
  def checkpoint() {
    journal.checkpoint()
  }

  /**
   * Close this queue. Any further operations will fail.
   */
  def close() {
    synchronized {
      closed = true
      readerMap.values.foreach { _.close() }
      journal.close()
      readerMap = Map()
    }
  }

  /**
   * Close this queue and also erase any journal files.
   */
  def erase() {
    synchronized {
      closed = true
      readerMap.values.foreach { _.close() }
      journal.erase()
      readerMap = Map()
    }
  }

  /**
   * Erase any items in any reader queue.
   */
  def flush() {
    readerMap.values.foreach { _.flush() }
  }

  /**
   * Do a sweep of each reader, discarding all expired items up to the reader's configured
   * `maxExpireSweep`. Returns the total number of items expired across all readers.
   */
  def discardExpired(): Int = discardExpired(true)

  /**
   * Do a sweep of each reader, discarding expired items at the head of the reader's queue. If
   * applyMaxExpireSweep is true, the reader's currently configured `maxExpireSweep` limit is
   * enforced, otherwise expiration continues until there are no more expired items at the head
   * of the queue. Returns the total number of items expired across all readers.
   */
  def discardExpired(applyMaxExpireSweep: Boolean): Int = {
    readerMap.values.foldLeft(0) { _ + _.discardExpired(applyMaxExpireSweep) }
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

    val (_, syncFuture) = journal.put(data, addTime, expireTime, errorCount)

    readerMap.values.foreach { _.postPut() }

    Some(syncFuture)
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
  class Reader(val name: String, val readerConfig: JournaledQueueReaderConfig) {
    val journalReader = journal.reader(name)

    private val openReads = new ConcurrentHashMap[Long, QueueItem]()

    /**
     * The current value of `itemCount`.
     */
    def items = journalReader.itemCount.get

    /**
     * The current value of `byteCount`.
     */
    def bytes = journalReader.byteCount.get

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
    def putCount = journalReader.putCount

    /**
     * Total number of bytes ever added to this queue.
     */
    def putBytes = journalReader.putBytes

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
     * Total number of items ever discarded from this queue. Discards occur when the queue
     * reaches its configured maximum size or maximum number of items.
     */
    val discardedCount = new AtomicLong(0)

    /**
     * Total number of reads opened (transactions) on this queue.
     */
    val openedItemCount = new AtomicLong(0)

    /**
     * Total number of committed reads (transactions) on this queue.
     */
    val committedItemCount = new AtomicLong(0)

    /**
     * Total number of canceled reads (transactions) on this queue.
     */
    val canceledItemCount = new AtomicLong(0)

    /**
     * Number of consumers waiting for an item to arrive.
     */
    def waiterCount: Int = waiters.size

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

    val waiters = new DeadlineWaitQueue(timer)

    /*
     * in order to reload the contents of a queue at startup, we need to:
     *   - read the last saved state (head id, done ids)
     *   - start at the head, and read in items (ignoring ones already done) until we hit the
     *     current item and then:
     *       - count the items left in the current journal file
     *       - add in the summarized counts from the remaining journal files, if any.
     */
    private[libkestrel] def loadFromJournal(j: Journal#Reader) {
      log.info("Restoring state of %s+%s", config.name, name)
      j.open()
    }

    /**
     * Age of the oldest item in this queue or 0 if the queue is empty.
     */
    def age: Duration =
      journalReader.peekOldest() match {
        case Some(item) =>
          Time.now - item.addTime
        case None => 0.nanoseconds
      }

    def catchUp() {
      journalReader.catchUp()
    }

    def checkpoint() {
      journalReader.checkpoint()
    }

    def canPut: Boolean = {
      readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest ||
        (items < readerConfig.maxItems && bytes < readerConfig.maxSize.inBytes)
    }

    @tailrec
    private[libkestrel] final def dropOldest() {
      if (readerConfig.fullPolicy == ConcurrentBlockingQueue.FullPolicy.DropOldest &&
          (items > readerConfig.maxItems || bytes > readerConfig.maxSize.inBytes)) {
        journalReader.next() match {
          case None => ()
          case Some(item) =>
            commitItem(item)
            discardedCount.getAndIncrement()
            dropOldest()
        }
      }
    }

    private [libkestrel] def postPut() {
      dropOldest()
      discardExpiredWithoutLimit()
      waiters.trigger()
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

    def discardExpiredWithoutLimit() = discardExpired(false)

    def discardExpired(applyMaxExpireSweep: Boolean = true): Int = {
      val max = if (applyMaxExpireSweep) readerConfig.maxExpireSweep else Int.MaxValue
      discardExpired(max)
    }

    // check the queue and discard anything that's expired.
    private[this] def discardExpired(max: Int): Int = {
      var numExpired = 0
      var remainingAttempts = max
      while(remainingAttempts > 0) {
        journalReader.nextIf { item => hasExpired(item.addTime, item.expireTime, Time.now) } match {
          case Some(item) =>
            readerConfig.processExpiredItem(item)
            expiredCount.getAndIncrement()
            commitItem(item)
            remainingAttempts -= 1
            numExpired += 1
          case None =>
            remainingAttempts = 0
        }
      }

      numExpired
    }

    private[this] def waitNext(deadline: Deadline, peeking: Boolean): Future[Option[QueueItem]] = {
      val startTime = Time.now
      val promise = new Promise[Option[QueueItem]]
      waitNext(startTime, deadline, promise, peeking)
      promise
    }

    private[this] def waitNext(startTime: Time,
                               deadline: Deadline,
                               promise: Promise[Option[QueueItem]],
                               peeking: Boolean) {
      val item = if (peeking) journalReader.peekOldest() else journalReader.next()
      if (item.isDefined || closed) {
        promise.setValue(item)
      } else {
        // checking future.isCancelled is a race, we assume that the caller will either commit
        // or unget the item if we miss the cancellation
        def onTrigger() = {
          if (promise.isCancelled) {
            promise.setValue(None)
            waiters.trigger()
          } else {
            // if we get woken up, try again with the same deadline.
            waitNext(startTime, deadline, promise, peeking)
          }
        }
        def onTimeout() {
          promise.setValue(None)
        }
        val w = waiters.add(deadline, onTrigger, onTimeout)
        promise.onCancellation { waiters.remove(w) }
      }
    }

    /**
     * Remove and return an item from the queue, if there is one.
     * If no deadline is given, an item is only returned if one is immediately available.
     */
    def get(deadline: Option[Deadline], peeking: Boolean = false): Future[Option[QueueItem]] = {
      if (closed) return Future.value(None)
      discardExpiredWithoutLimit()
      val startTime = Time.now

      val future = deadline match {
        case Some(d) => waitNext(d, peeking)
        case None if !peeking => Future.value(journalReader.next())
        case None => Future.value(journalReader.peekOldest())
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
              if (!peeking) {
                openedItemCount.incrementAndGet
                openReads.put(item.id, item)
                item.data.mark()
              }
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

      commitItem(item)
    }

    private[this] def commitItem(item: QueueItem) {
      journalReader.commit(item)
      committedItemCount.getAndIncrement()
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
        journalReader.unget(newItem)
        waiters.trigger()
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
    def flush() {
      journalReader.flush()
      flushCount.getAndIncrement()
    }

    def close() {
      journalReader.checkpoint()
      journalReader.close()
    }

    def evictWaiters() {
      waiters.evictAll()
    }

    /**
     * Check if this Queue is eligible for expiration by way of it being empty
     * and its age being greater than or equal to maxQueueAge
     */
    def isReadyForExpiration: Boolean = {
      readerConfig.maxQueueAge.map { age =>
        Time.now > createTime + age && journalReader.isCaughtUp
      }.getOrElse(false)
    }

    def toDebug: String = {
      "<JournaledQueue#Reader: name=%s items=%d bytes=%d age=%s open=%s puts=%d discarded=%d, expired=%d>".format(
        name, items, bytes, age, openReads.keys.asScala.toList.sorted, putCount.get, discardedCount.get,
        expiredCount.get)
    }
  }
}
