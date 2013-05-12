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
import com.twitter.logging.Logger
import com.twitter.util._
import java.io.{File, FileOutputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.{AtomicInteger,AtomicLong}
import java.util.Deque
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable

case class FileInfo(file: File, headId: Long, tailId: Long, items: Int, bytes: Long)

object Journal {
  def getQueueNamesFromFolder(path: File): Set[String] = {
    path.listFiles().filter { file =>
      !file.isDirectory()
    }.map { file =>
      file.getName
    }.filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }.toSet
  }

  def builder(
    queuePath: File, scheduler: ScheduledExecutorService, syncJournal: Duration,
    saveArchivedJournals: Option[File]
  ) = {
    (queueName: String, maxFileSize: StorageUnit) => {
      new Journal(queuePath, queueName, maxFileSize, scheduler, syncJournal, saveArchivedJournals)
    }
  }

  def builder(
    queuePath: File, maxFileSize: StorageUnit, scheduler: ScheduledExecutorService,
    syncJournal: Duration, saveArchivedJournals: Option[File]
  ) = {
    (queueName: String) => {
      new Journal(queuePath, queueName, maxFileSize, scheduler, syncJournal, saveArchivedJournals)
    }
  }

  private[libkestrel] val NoCondition = (_: QueueItem) => true

  //Dynamically load ConcurrentLinkedDeque so we don't have a hard dependence on Java 7
  private[this] val dequeClass: Class[_] = {
    //First try to see java.util.concurrent.ConcurrentLinkedDeque exists
    try {
      Class.forName("java.util.concurrent.ConcurrentLinkedDeque")
    } catch {
      //Now try to see if jsr166y is available on the classpath
      case e:ClassNotFoundException => 
        try {
          Class.forName("jsr166y.ConcurrentLinkedDeque")
        } catch  {
          case e:ClassNotFoundException => 
            throw new ClassNotFoundException("Could not find an appropriate class for ConcurrentLinkedDeque. " +
              "You either need to be running Java 7+ or supply js166y jar.")
        }
    }
  }

  private def newDeque[T]: Deque[T] = dequeClass.newInstance().asInstanceOf[Deque[T]]

}

/**
 * Maintain a set of journal files with the same prefix (`queuePath`/`queueName`):
 *   - list of adds (<prefix>.<timestamp>)
 *   - one state file for each reader (<prefix>.read.<name>)
 * The files filled with adds will be chunked as they reach `maxFileSize` in length.
 */
class Journal(
  queuePath: File,
  queueName: String,
  maxFileSize: StorageUnit,
  scheduler: ScheduledExecutorService,
  syncJournal: Duration,
  saveArchivedJournals: Option[File]
) extends ReadWriteLock {
  private[this] val log = Logger.get(getClass)

  // NOTE ON LOCKING:
  // - take the write lock to modify the journal or data structures related to writing the journal
  // - take the read lock to read the journal or data structures shared by the Journal and its
  //   Reader(s)

  val prefix = new File(queuePath, queueName)

  @volatile var idMap = immutable.TreeMap.empty[Long, FileInfo]
  @volatile var readerMap = immutable.Map.empty[String, Reader]

  @volatile private[this] var _journalFile: JournalFileWriter = null

  // last item added to the journal.
  // in theory, we should support rollover, but given 63 bits of ID space, even if a queue is
  // handling 1M items/second, it doesn't need to support rollover for about 200,000 years.
  @volatile private[this] var _tailId = 0L

  // items & bytes in the current journal file so far:
  @volatile private[this] var currentItems = 0
  @volatile private[this] var currentBytes = 0L

  removeTemporaryFiles()
  buildIdMap()
  openJournal()
  buildReaderMap()

  // make sure there's always at least a default reader.
  if (readerMap.isEmpty) reader("")

  // and there's no default reader if there's at least one other one.
  if (readerMap.size > 1) {
    readerMap.get("").foreach { r =>
      r.file.delete()
      readerMap = readerMap - ""
    }
  }

  /**
   * Scan timestamp files for this queue, and build a map of (item id -> file) for the first id
   * seen in each file. This lets us quickly find the right file when we look for an item id.
   */
  private[this] def buildIdMap() {
    var newMap = immutable.TreeMap.empty[Long, FileInfo]
    writerFiles().foreach { file =>
      scanJournalFile(file).foreach { fileInfo =>
        newMap += (fileInfo.headId -> fileInfo)
      }
    }
    idMap = newMap
  }

  def removeTemporaryFiles() {
    queuePath.list().foreach { name =>
      if (name contains "~~") new File(queuePath, name).delete()
    }
  }

  def writerFiles() = {
    queuePath.list().filter { name =>
      name.startsWith(queueName + ".") &&
        !name.contains("~") &&
        !name.split("\\.")(1).find { !_.isDigit }.isDefined
    }.map { name =>
      new File(queuePath, name)
    }
  }

  def readerFiles() = {
    queuePath.list().filter { name =>
      name.startsWith(queueName + ".read.") && !name.contains("~")
    }.map { name =>
      new File(queuePath, name)
    }
  }

  def fileInfoForId(id: Long): Option[FileInfo] = {
    idMap.to(id).lastOption.map { case (k, v) => v }
  }

  def fileInfosAfter(id: Long): Seq[FileInfo] = {
    idMap.from(id).values.toSeq
  }

  private[this] def buildReaderMap() {
    var newMap = immutable.HashMap.empty[String, Reader]
    readerFiles().foreach { file =>
      val name = file.getName.split("\\.", 3)(2)
      try {
        val reader = new Reader(name, file)
        reader.readState()
        newMap = newMap + (name -> reader)
      } catch {
        case e: IOException => log.warning("Skipping corrupted reader file: %s", file)
      }
    }
    readerMap = newMap
  }

  // find the earliest possible head id
  private[this] def earliestHead = {
    idMap.headOption match {
      case Some((id, file)) => id
      case None => 0L
    }
  }

  // scan a journal file to pull out the # items, # bytes, and head & tail ids.
  private[this] def scanJournalFile(file: File): Option[FileInfo] = {
    var firstId: Option[Long] = None
    var tailId = 0L
    var items = 0
    var bytes = 0L
    val journalFile = try {
      JournalFile.open(file)
    } catch {
      case e: IOException => {
        log.error(e, "Unable to open journal %s; aborting!", file)
        return None
      }
    }

    try {
      log.info("Scanning journal '%s' file %s", queueName, file)
      journalFile.foreach { entry =>
        val position = journalFile.position
        entry match {
          case Record.Put(item) => {
            if (firstId == None) firstId = Some(item.id)
            items += 1
            bytes += item.dataSize
            tailId = item.id
          }
          case _ =>
        }
      }
      journalFile.close()
    } catch {
      case e @ CorruptedJournalException(position, file, message) => {
        log.error("Corrupted journal %s at position %d; truncating. DATA MAY HAVE BEEN LOST!",
          file, position)
        journalFile.close()
        val trancateWriter = new FileOutputStream(file, true).getChannel
        try {
          trancateWriter.truncate(position)
        } finally {
          trancateWriter.close()
        }
        // try again on the truncated file.
        return scanJournalFile(file)
      }
    }
    if (firstId == None) {
      // not a single thing in this journal file.
      log.info("Empty journal file %s -- erasing.", file)
      file.delete()
      None
    } else {
      firstId.map { id => FileInfo(file, id, tailId, items, bytes) }
    }
  }

  private[this] def openJournal() {
    idMap.lastOption match {
      case Some((id, fileInfo)) =>
        try {
          _journalFile = JournalFile.append(fileInfo.file, scheduler, syncJournal, maxFileSize)
          _tailId = fileInfo.tailId
          currentItems = fileInfo.items
          currentBytes = fileInfo.bytes
        } catch {
          case e: IOException => {
            log.error("Unable to open journal %s; aborting!", fileInfo.file)
            throw e
          }
        }
      case None =>
        log.info("No transaction journal for '%s'; starting with empty queue.", queueName)
        rotate()
    }
  }

  private[this] def uniqueFile(prefix: File): File = {
    var file = new File(prefix.getAbsolutePath + Time.now.inMilliseconds)
    while (!file.createNewFile()) {
      Thread.sleep(1)
      file = new File(prefix.getAbsolutePath + Time.now.inMilliseconds)
    }
    file
  }

  // delete any journal files that are unreferenced.
  private[this] def checkOldFiles() {
    val minHead = readerMap.values.foldLeft(tail) { (n, r) => n min (r.head + 1) }
    // all the files that start off with unreferenced ids, minus the last. :)
    withWriteLock {
      idMap.takeWhile { case (id, fileInfo) => id <= minHead }.dropRight(1).foreach { case (id, fileInfo) =>
        log.info("Erasing unused journal file for '%s': %s", queueName, fileInfo.file)
        idMap = idMap - id
        if (saveArchivedJournals.isDefined) {
          val archiveFile = new File(saveArchivedJournals.get, "archive~" + fileInfo.file.getName)
          fileInfo.file.renameTo(archiveFile)
        } else {
          fileInfo.file.delete()
        }
      }
    }
  }

  private[this] def rotate() {
    withWriteLock {
      if (_journalFile ne null) {
        // fix up id map to have the new item/byte count
        idMap.last match { case (id, info) =>
          idMap += (id -> FileInfo(_journalFile.file, id, _tailId, currentItems, currentBytes))
        }
      }
      // open new file
      var newFile = uniqueFile(new File(queuePath, queueName + "."))
      val oldJournalFile = _journalFile
      _journalFile = JournalFile.create(newFile, scheduler, syncJournal, maxFileSize)
      if (oldJournalFile eq null) {
        log.info("Rotating %s to %s", queueName, newFile)
      } else {
        log.info("Rotating %s from %s (%s) to %s", queueName, oldJournalFile.file,
          oldJournalFile.position.bytes.toHuman, newFile)
        oldJournalFile.close()
      }
      currentItems = 0
      currentBytes = 0
      idMap += (_tailId + 1 -> FileInfo(newFile, _tailId + 1, 0, 0, 0L))
      checkOldFiles()
    }
  }

  def reader(name: String): Reader = {
    readerMap.get(name).getOrElse {
      // grab a lock so only one thread does this potentially slow thing at once
      withWriteLock {
        readerMap.get(name).getOrElse {
          val file = new File(queuePath, queueName + ".read." + name)
          val reader = readerMap.get("") match {
            case Some(r) => {
              // move the default reader over to our new one.
              val oldFile = r.file
              r.file = file
              r.name = name
              r.forceCheckpoint()
              oldFile.delete()
              readerMap = readerMap - ""
              r
            }
            case None => {
              val reader = new Reader(name, file)
              reader.head = _tailId
              reader.checkpoint()
              reader
            }
          }
          readerMap = readerMap + (name -> reader)
          reader
        }
      }
    }
  }

  // rare operation: destroy a reader.
  def dropReader(name: String) {
    withWriteLock {
      readerMap.get(name) foreach { reader =>
        readerMap -= name
        reader.file.delete()
        reader.close()
      }
    }
  }

  def journalSize: Long = {
    val (files, position) = withReadLock { (writerFiles(), _journalFile.position) }
    val fileSizes = files.foldLeft(0L) { (sum, file) => sum + file.length() }
    fileSizes - files.last.length() + position
  }

  def tail = withReadLock { _tailId }

  def close() {
    withWriteLock {
      readerMap.values.foreach { reader =>
        reader.checkpoint()
        reader.close()
      }
      readerMap = immutable.Map.empty[String, Reader]
      _journalFile.close()
    }
  }

  /**
   * Get rid of all journal files for this queue.
   */
  def erase() {
    withWriteLock {
      close()
      readerFiles().foreach { _.delete() }
      writerFiles().foreach { _.delete() }
      removeTemporaryFiles()
    }
  }

  def checkpoint() {
    val readers = withReadLock { readerMap.toList }
    readers.foreach { case (_, reader) =>
      reader.checkpoint()
    }
    withWriteLock {
      checkOldFiles()
    }
  }

  def put(
    data: ByteBuffer, addTime: Time, expireTime: Option[Time],
    errorCount: Int = 0): (QueueItem, Future[Unit]) = {
    val (item, future) = withWriteLock {
      val id = _tailId + 1
      val item = QueueItem(id, addTime, expireTime, data, errorCount)
      if (_journalFile.position + _journalFile.storageSizeOf(item) > maxFileSize.inBytes) rotate()

      _tailId = id
      val future = _journalFile.put(item)
      currentItems += 1
      currentBytes += data.remaining

      readerMap.values.foreach { reader =>
        reader.itemCount.getAndIncrement()
        reader.byteCount.getAndAdd(item.dataSize)
        reader.putCount.getAndIncrement()
        reader.putBytes.getAndAdd(item.dataSize)
      }

      (item, future)
    }

    (item, future)
  }

  /**
   * Track state for a queue reader. Every item prior to the "head" pointer (including the "head"
   * pointer itself) has been read by this reader. Separately, "doneSet" is a set of items that
   * have been read out of order, usually because they refer to transactional reads that were
   * confirmed out of order.
   */
  class Reader(@volatile var name: String, @volatile var file: File) {
    @volatile var haveReadState: Boolean = false
    @volatile private[this] var dirty = true

    // NOTE ON LOCKING: withReadLock takes the Journal's read lock. Use synchronization to
    // protect the Reader's data from concurrent modification by multiple callers on THIS reader
    // instance.

    private[this] var journalFile: JournalFileReader = _
    private[this] var _head = 0L
    private[this] var _next = 0L
    private[this] val _doneSet = new mutable.HashSet[Long]

    val itemCount = new AtomicInteger(0)
    val byteCount = new AtomicLong(0)

    val putCount = new AtomicInteger(0)
    val putBytes = new AtomicLong(0)

    private val rejectedQueue = Journal.newDeque[QueueItem]

    private[libkestrel] def readState() {
      val bookmarkFile = BookmarkFile.open(file)
      try {
        bookmarkFile.foreach { entry =>
          entry match {
            case Record.ReadHead(id) => _head = id
            case Record.ReadDone(ids) => _doneSet ++= ids
            case x => log.warning("Skipping unknown entry %s in read journal: %s", x, file)
          }
        }
      } finally {
        bookmarkFile.close()
      }
      _head = (_head min _tailId) max (earliestHead - 1)
      _doneSet.retain { id => id <= _tailId && id > _head }
      haveReadState = true
      log.debug("Read checkpoint %s+%s: head=%s done=(%s)", queueName, name, _head, _doneSet.toSeq.sorted.mkString(","))
    }

    private[this] def withReadLock[A](f: => A) = Journal.this.withReadLock(f)

    def open() {
      synchronized {
        withReadLock {
          close()
          readState()

          val fileInfo = fileInfoForId(_head).getOrElse { idMap(earliestHead) }
          log.info("Reader %s+%s starting on: %s", queueName, name, fileInfo.file)
          val jf = JournalFile.open(fileInfo.file)
          var initialItems = 0
          var initialBytes = 0L
          if (_head >= earliestHead) {
            var lastId = -1L
            var readItems = 0
            var readBytes = 0L
            while (lastId < _head) {
              jf.readNext() match {
                case None => lastId = _head + 1 // end of last file
                case Some(Record.Put(QueueItem(id, _, _, data, _))) =>
                  lastId = id
                  readItems += 1
                  readBytes += data.remaining
                case _ => ()
              }
            }
            initialItems = fileInfo.items - readItems
            initialBytes = fileInfo.bytes - readBytes
          }
          val afterFiles = fileInfosAfter(_head + 1)
          itemCount.set(afterFiles.foldLeft(initialItems) { case (items, fileInfo) =>
            items + fileInfo.items })
          byteCount.set(afterFiles.foldLeft(initialBytes) { case (bytes, fileInfo) =>
            bytes + fileInfo.bytes })
          _next = _head + 1
          journalFile = jf
        }
      }
    }

    private[this] def nextFile(): Boolean = {
      val fileInfoCandidate = withReadLock { fileInfoForId(_next) }
      fileInfoCandidate match {
        case Some(fileInfo) if (fileInfo.file != journalFile.file) =>
            journalFile.close()
            journalFile = null
            log.info("Reader %s+%s moving to: %s", queueName, name, fileInfo.file)
            journalFile = JournalFile.open(fileInfo.file)
            true
          case _ =>
            false
      }
    }

    @tailrec
    private[this] def next(peek: Boolean, cond: QueueItem => Boolean): Option[QueueItem] = {
      val retryItem = Option(rejectedQueue.poll()).flatMap { item =>
        if (peek) {
          val result = Some(item.copy(data = item.data.duplicate()))
          rejectedQueue.addFirst(item)
          result
        } else if (cond(item)) {
          Some(item)
        } else {
          rejectedQueue.addFirst(item)
          None
        }
      }
      if (retryItem.isDefined) return retryItem

      synchronized {
        if (withReadLock { _next > tail }) {
          return None
        }

        val mark = journalFile.position
        journalFile.readNext() match {
          case Some(Record.Put(item)) if peek =>
            val newItem = item.copy(data = item.data.duplicate())
            journalFile.rewind(mark)
            return Some(newItem)
          case Some(Record.Put(item)) =>
            if (cond(item)) {
              _next = item.id + 1
              return Some(item)
            } else {
              journalFile.rewind(mark)
              return None
            }
          case None =>
            if (!nextFile()) {
              return None
            }
          case _ => ()
        }
      }

      next(peek, cond)
    }

    def next(): Option[QueueItem] = {
      next(false, Journal.NoCondition)
    }

    def nextIf(f: QueueItem => Boolean): Option[QueueItem] = {
      next(false, f)
    }

    final def peekOldest(): Option[QueueItem] = {
      next(true, Journal.NoCondition)
    }

    /**
     * Test if this reader is caught up with the writer.
     */
    def isCaughtUp = synchronized { withReadLock { _next > tail } }

    /**
     * To avoid a race while setting up a new reader, call this after initialization to reset the
     * head of the queue.
     */
    def catchUp() {
      synchronized {
        withReadLock {
          if (!haveReadState) head = _tailId
          dirty = true
        }
      }
    }

    /**
     * Rewrite the reader file with the current head and out-of-order committed reads.
     */
    def checkpoint() {
      val checkpointData =
        synchronized {
          if (dirty) {
            dirty = false
            Some(_head, _doneSet.toSeq)
          } else {
            None
          }
        }

      checkpointData.foreach { case (head, doneSeq) =>
        val sortedDoneSeq = doneSeq.sorted
        log.debug("Checkpoint %s+%s: head=%s done=(%s)", queueName, name, head, sortedDoneSeq.mkString(","))
        val newFile = uniqueFile(new File(file.getParent, file.getName + "~~"))
        val newBookmarkFile = BookmarkFile.create(newFile)
        newBookmarkFile.readHead(head)
        newBookmarkFile.readDone(sortedDoneSeq)
        newBookmarkFile.close()
        newFile.renameTo(file)
      }
    }

    private[libkestrel] def forceCheckpoint() {
      synchronized { dirty = true }
      checkpoint()
    }

    def head: Long = this._head
    def doneSet: Set[Long] = _doneSet.toSeq.toSet
    def tail: Long = Journal.this._tailId

    def head_=(id: Long) {
      synchronized {
        _head = id
        _doneSet.retain { _ > head }
        dirty = true
      }
    }

    def commit(item: QueueItem) {
      val id = item.id
      synchronized {
        if (id == _head + 1) {
          _head += 1
          while (_doneSet contains _head + 1) {
            _head += 1
            _doneSet.remove(_head)
          }
        } else if (id > _head) {
          _doneSet.add(id)
        }
        dirty = true
        itemCount.getAndDecrement()
        byteCount.getAndAdd(-item.dataSize)
      }
    }

    def unget(item: QueueItem) {
      rejectedQueue.add(item)
    }

    /**
     * Discard all items and catch up with the main queue.
     */
    def flush() {
      synchronized {
        withReadLock {
          val currentTail = _tailId
          while(_head < currentTail) {
            next().foreach { commit(_) }
          }
          dirty = true
          checkpoint()
        }
      }
    }

    def close() {
      synchronized {
        if (journalFile ne null) {
          journalFile.close()
          journalFile = null
        }
      }
    }

    override def toString() = "Reader(%s, %s)".format(name, file)
  }
}
