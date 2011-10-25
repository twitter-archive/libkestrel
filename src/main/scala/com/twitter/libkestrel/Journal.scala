package com.twitter.libkestrel

import com.twitter.concurrent.Serialized
import com.twitter.conversions.storage._
import com.twitter.logging.Logger
import com.twitter.util._
import java.io.{File, FileOutputStream, IOException}
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable

/*
 *
 * journal TODO:
 *   - rotate to new file after X
 *   X checkpoint reader
 *   X read-behind pointer for reader
 *   - clean up old files if they're dead
 *   X fix readers with a too-far-future head
 */


object Journal {
  def getQueueNamesFromFolder(path: File): Set[String] = {
    path.list().filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }.toSet
  }

  def builder(queuePath: File, timer: Timer, syncJournal: Duration) = {
    (queueName: String, maxFileSize: StorageUnit) => {
      new Journal(queuePath, queueName, maxFileSize, timer, syncJournal)
    }
  }

  def builder(queuePath: File, maxFileSize: StorageUnit, timer: Timer, syncJournal: Duration) = {
    (queueName: String) => {
      new Journal(queuePath, queueName, maxFileSize, timer, syncJournal)
    }
  }
}

/**
 * Maintain a set of journal files with the same prefix (`queuePath`/`queueName`):
 *   - list of adds (<prefix>.<timestamp>)
 *   - one state file for each reader (<prefix>.read.<name>)
 * The files filled with adds will be chunked as they reach `maxFileSize` in length.
 */
class Journal(queuePath: File, queueName: String, maxFileSize: StorageUnit, timer: Timer, syncJournal: Duration)
  extends Serialized
{
  private[this] val log = Logger.get(getClass)

  val prefix = new File(queuePath, queueName)

  @volatile var idMap = immutable.TreeMap.empty[Long, File]
  @volatile var readerMap = immutable.Map.empty[String, Reader]

  @volatile private[this] var _journalFile: JournalFile = null
  @volatile private[this] var _tailId = 0L

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
    var newMap = immutable.TreeMap.empty[Long, File]
    writerFiles().foreach { file =>
      try {
        val j = JournalFile.openWriter(file, timer, Duration.MaxValue)
        j.readNext() match {
          case Some(JournalFile.Record.Put(item)) => {
            newMap = newMap + (item.id -> file)
          }
          case _ =>
        }
      } catch {
        case e: IOException => log.warning("Skipping corrupted file: %s", file)
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

  def fileForId(id: Long): Option[File] = {
    idMap.to(id).lastOption.map { case (k, v) => v }
  }

  private[this] def buildReaderMap() {
    var newMap = immutable.HashMap.empty[String, Reader]
    readerFiles().foreach { file =>
      val name = file.getName.split("\\.")(2)
      try {
        val reader = new Reader(file)
        reader.readState()
        newMap = newMap + (name -> reader)
      } catch {
        case e: IOException => log.warning("Skipping corrupted reader file: %s", file)
      }
    }
    readerMap = newMap
  }

  private[this] def openJournal() {
    if (idMap.size > 0) {
      val (id, file) = idMap.last
      try {
        val journalFile = JournalFile.openWriter(file, timer, syncJournal)
        try {
          log.info("Scanning journal '%s' file %s", queueName, file)
          var lastUpdate = 0L
          journalFile.foreach { entry =>
            val position = journalFile.position
            if (position >= lastUpdate + 10.megabytes.inBytes) {
              log.info("Continuing to read '%s' file %s; %s so far...", queueName, file,
                position.bytes.toHuman)
              lastUpdate += 10.megabytes.inBytes
            }
            entry match {
              case JournalFile.Record.Put(item) => _tailId = item.id
              case _ =>
            }
          }
        } catch {
          case e @ CorruptedJournalException(position, file, message) => {
            log.error("Corrupted journal %s at position %d; truncating. DATA MAY HAVE BEEN LOST!",
              file, position)
            val trancateWriter = new FileOutputStream(file, true).getChannel
            try {
              trancateWriter.truncate(position)
            } finally {
              trancateWriter.close()
            }
            throw e
          }
        } finally {
          journalFile.close()
        }
        _journalFile = JournalFile.append(file, timer, syncJournal)
      } catch {
        case e: CorruptedJournalException => {
          // we truncated it. try again.
          _tailId = 0
          openJournal()
        }
        case e: IOException => {
          log.error("Unable to open journal %s; aborting!", file)
          throw e
        }
      }
    } else {
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

  private[this] def rotate() {
    var newFile = uniqueFile(new File(queuePath, queueName + "."))
    _journalFile = JournalFile.createWriter(newFile, timer, syncJournal)
    idMap = idMap + (_tailId + 1 -> newFile)
  }

  // warning: set up your chaining before calling this. _tailId could increment during this method.
  def reader(name: String): Reader = {
    readerMap.get(name).getOrElse {
      // grab a lock so only one thread does this potentially slow thing at once
      synchronized {
        readerMap.get(name).getOrElse {
          val file = new File(queuePath, queueName + ".read." + name)
          val reader = readerMap.get("") match {
            case Some(r) => {
              // move the default reader over to our new one.
              val oldFile = r.file
              r.file = file
              r.checkpoint()
              oldFile.delete()
              readerMap = readerMap - ""
              r
            }
            case None => {
              val reader = new Reader(file)
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

  def journalSize: Long = {
    writerFiles().foldLeft(0L) { (sum, file) => sum + file.length() }
  }

  def tail = _tailId

  def close() {
    readerMap.values.foreach { reader =>
      reader.checkpoint()
      reader.close()
    }
    readerMap = immutable.Map.empty[String, Reader]
    _journalFile.close()
  }

  /**
   * Get rid of all journal files for this queue.
   */
  def erase() {
    close()
    readerFiles().foreach { _.delete() }
    writerFiles().foreach { _.delete() }
    removeTemporaryFiles()
  }

  def checkpoint() {
    readerMap.foreach { case (name, reader) =>
      reader.checkpoint()
    }
  }

  def put(data: Array[Byte], addTime: Time, expireTime: Option[Time]): Future[(Long, Future[Unit])] = {
    serialized {
      _tailId += 1
      val id = _tailId
      val future = _journalFile.put(QueueItem(_tailId, addTime, expireTime, data))
      if (_journalFile.position >= maxFileSize.inBytes) rotate()
      (id, future)
    }
  }

  /**
   * Track state for a queue reader. Every item prior to the "head" pointer (including the "head"
   * pointer itself) has been read by this reader. Separately, "doneSet" is a set of items that
   * have been read out of order, usually because they refer to transactional reads that were
   * confirmed out of order.
   */
  case class Reader(_file: File) extends Serialized {
    @volatile var file: File = _file

    private[this] var _head = 0L
    private[this] val _doneSet = new ItemIdList()
    private[this] var _readBehind: Option[JournalFile] = None
    private[this] var _readBehindId = 0L

    def readState() {
      val journalFile = JournalFile.openReader(file, timer, syncJournal)
      try {
        journalFile.foreach { entry =>
          entry match {
            case JournalFile.Record.ReadHead(id) => _head = id
            case JournalFile.Record.ReadDone(ids) => _doneSet.add(ids.filter { _ <= _tailId })
            case x => log.warning("Skipping unknown entry %s in read journal: %s", x, file)
          }
        }
      } finally {
        journalFile.close()
      }
      _head = _head min _tailId
    }

    /**
     * Rewrite the reader file with the current head and out-of-order committed reads.
     */
    def checkpoint() {
      val head = _head
      val doneSet = _doneSet.toSeq
      // FIXME really this should go in another thread. doesn't need to happen inline.
      serialized {
        val newFile = uniqueFile(new File(file.getParent, file.getName + "~~"))
        val newJournalFile = JournalFile.createReader(newFile, timer, syncJournal)
        newJournalFile.readHead(_head)
        newJournalFile.readDone(_doneSet.toSeq)
        newJournalFile.close()
        newFile.renameTo(file)
      }
    }

    def head: Long = this._head
    def doneSet: Set[Long] = _doneSet.toSeq.toSet

    def head_=(id: Long) {
      _head = id
      val toRemove = _doneSet.toSeq.filter { _ <= _head }
      _doneSet.remove(toRemove.toSet)
    }

    def commit(id: Long) {
      if (id == _head + 1) {
        _head += 1
        while (_doneSet contains _head + 1) {
          _head += 1
          _doneSet.remove(_head)
        }
      } else {
        _doneSet.add(id)
      }
    }

    /**
     * Discard all items and catch up with the main queue.
     */
    def flush() {
      _head = _tailId
      _doneSet.popAll()
    }

    /**
     * Open the journal file containing a given item, so we can read items directly out of the
     * file. This means the queue no longer wants to try keeping every item in memory.
     */
    def startReadBehind(readBehindId: Long) {
      val file = fileForId(readBehindId)
      if (!file.isDefined) throw new IOException("Unknown id")
      val jf = JournalFile.openWriter(file.get, timer, syncJournal)
      var lastId = -1L
      while (lastId != readBehindId) {
        jf.readNext() match {
          case None => throw new IOException("Can't find id " + head + " in " + file)
          case Some(JournalFile.Record.Put(QueueItem(id, _, _, _))) => lastId = id
          case _ =>
        }
      }
      _readBehind = Some(jf)
      _readBehindId = readBehindId
    }

    /**
     * Read & return the next item in the read-behind journals.
     */
    @tailrec
    final def nextReadBehind(): QueueItem = {
      _readBehind.get.readNext() match {
        case None => {
          _readBehind.foreach { _.close() }
          val file = fileForId(_readBehindId + 1)
          if (!file.isDefined) throw new IOException("Unknown id")
          _readBehind = Some(JournalFile.openWriter(file.get, timer, syncJournal))
          nextReadBehind()
        }
        case Some(JournalFile.Record.Put(item)) => {
          _readBehindId = item.id
          item
        }
        case _ => nextReadBehind()
      }
    }

    /**
     * End read-behind mode, and close any open journal file.
     */
    def endReadBehind() {
      _readBehind.foreach { _.close() }
      _readBehind = None
    }

    def close() {
      endReadBehind()
    }

    def inReadBehind = _readBehind.isDefined
  }
}


/*


  def rotate(reservedItems: Seq[QItem], setCheckpoint: Boolean): Option[Checkpoint] = {
    writer.close()
    val rotatedFile = uniqueFile(".")
    new File(queuePath, queueName).renameTo(rotatedFile)
    size = 0
    calculateArchiveSize()
    open()

    if (readerFilename == Some(queueName)) {
      readerFilename = Some(rotatedFile.getName)
    }

    if (setCheckpoint && !checkpoint.isDefined) {
      checkpoint = Some(Checkpoint(rotatedFile.getName, reservedItems))
    }
    checkpoint
  }



 */

