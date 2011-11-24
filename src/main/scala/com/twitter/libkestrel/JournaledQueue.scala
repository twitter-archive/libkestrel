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
import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable
import scala.collection.JavaConverters._
import config._

trait Codec[A] {
  def encode(item: A): Array[Byte]
  def decode(data: Array[Byte]): A
}

class JournaledQueue(val config: JournaledQueueConfig, path: File, timer: Timer)
  extends Serialized
{
  private[this] val log = Logger.get(getClass)

  private[this] val NAME_REGEX = """[^A-Za-z0-9:_-]""".r
  if (NAME_REGEX.findFirstIn(config.name).isDefined) {
    throw new Exception("Illegal queue name: " + config.name)
  }

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
  }

  /**
   * Close this queue and also erase any journal files.
   */
  def erase() {
    close()
    journal.foreach { _.erase() }
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
  def put(data: Array[Byte], addTime: Time, expireTime: Option[Time]): Option[Future[Unit]] = {
    if (closed) return None
    if (data.size > config.maxItemSize.inBytes) {
      log.debug("Rejecting put to %s: item too large (%s).", config.name, data.size.bytes.toHuman)
      return None
    }
    // if any reader is rejecting puts, the put is rejected.
    if (readerMap.values.exists { r => !r.canPut }) {
      log.debug("Rejecting put to %s: reader is full.", config.name)
      return None
    }

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

  def toDebug: String = {
    "<JournaledQueue: name=%s items=%d bytes=%d journalBytes=%s readers=(%s)%s>".format(
      config.name, items, bytes, journalBytes, readerMap.values.map { _.toDebug }.mkString(", "),
      (if (closed) " closed" else ""))
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

      def poll(): Future[Option[A]] = {
        reader.get(None).map { optItem =>
          optItem.map { item =>
            reader.commit(item.id)
            codec.decode(item.data)
          }
        }
      }

      def pollIf(predicate: A => Boolean): Future[Option[A]] = {
        throw new Exception("Unsupported operation")
      }

      def toDebug: String = JournaledQueue.this.toDebug

      def close() {
        JournaledQueue.this.close()
      }
    }
  }


  class Reader(private[libkestrel] var name: String, val readerConfig: JournaledQueueReaderConfig)
    extends Serialized
  {
    val journalReader = journal.map { _.reader(name) }
    private[libkestrel] val queue = ConcurrentBlockingQueue[QueueItem](timer)

    @volatile var items = 0
    @volatile var bytes = 0L
    @volatile var memoryItems = 0
    @volatile var memoryBytes = 0L
    @volatile var age = 0.nanoseconds
    @volatile var discarded = 0L
    @volatile var expired = 0L

    private val openReads = new ConcurrentHashMap[Long, QueueItem]()

    /**
     * Number of open (uncommitted) reads.
     */
    def openItems = openReads.values.size

    /**
     * Byte count of open (uncommitted) reads.
     */
    def openBytes = openReads.values.asScala.foldLeft(0L) { _ + _.data.size }

    /**
     * Total number of items ever added to this queue.
     */
    val putCount = new AtomicLong(0)

    /**
     * Total number of items ever expired from this queue.
     */
    val expiredCount = new AtomicLong(0)

    /**
     * Total number of items ever discarded from this queue.
     */
    val discardedCount = new AtomicLong(0)

    /**
     * Number of consumers waiting for an item to arrive.
     */
    def waiterCount: Int = queue.waiterCount

    /**
     * FQDN for this reader, which is usually of the form "queue_name+reader_name", but will just
     * be "queue_name" for the default reader.
     */
    def fullname: String = {
      if (name == "") config.name else (config.name + "+" + name)
    }

    def writer: JournaledQueue = JournaledQueue.this

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
              discarded += 1
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
          memoryItems += 1
          memoryBytes += item.data.size
        }
        items += 1
        bytes += item.data.size
        putCount.getAndIncrement()

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
            bytes -= item.data.size
            memoryItems -= 1
            memoryBytes -= item.data.size
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
          j.nextReadBehind().foreach { item =>
            queue.put(item)
            memoryItems += 1
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
      discardExpired()
      val future = deadline match {
        case Some(d) => queue.get(d)
        case None => queue.poll()
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
      memoryItems -= 1
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
    def peek(deadline: Option[Time]): Future[Option[QueueItem]] = {
      val future = get(deadline)
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
        queue.poll() map { itemOption =>
          itemOption match {
            case Some(item) => {
              flush()
            }
            case None => {
              serialized {
                items = 0
                bytes = 0
                memoryItems = 0
                memoryBytes = 0
                age = 0.nanoseconds
              }
            }
          }
        }
      }
    }

    def close() {
      journalReader.foreach { j =>
        j.checkpoint()()
        j.close()
      }
    }

    def toDebug: String = {
      "<JournaledQueue#Reader: name=%s items=%d bytes=%d age=%s queue=%s>".format(
        name, items, bytes, age, queue.toDebug)
    }
  }
}
