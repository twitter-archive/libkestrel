package com.twitter.libkestrel

import com.twitter.logging.Logger
import com.twitter.util._
import java.io.{File, IOException}
import scala.collection.immutable
import scala.collection.mutable

/*
 *
 * journal TODO:
 * - rotate to new file after X
 * - checkpoint reader
 * - read-behind pointer for reader
 * - clean up old files if they're dead
 */


object Journal {
  def getQueueNamesFromFolder(path: File): Set[String] = {
    path.list().filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }.toSet
  }
}

/**
 * Maintain a set of journal files with the same prefix (`queuePath`/`queueName`):
 * - list of adds (<prefix>.<timestamp>)
 * - one state file for each reader (<prefix>.read.<name>)
 */
class Journal(queuePath: File, queueName: String, timer: Timer, syncJournal: Duration) {
  private[this] val log = Logger.get(getClass)

  val prefix = new File(queuePath, queueName)

  var idMap = immutable.TreeMap.empty[Long, File]
  var readerMap = immutable.HashMap.empty[String, Reader]

  buildIdMap()
  buildReaderMap()

  /**
   * Scan timestamp files for this queue, and build a map of (item id -> file) for the first id
   * seen in each file. This lets us quickly find the right file when we look for an item id.
   */
  def buildIdMap() {
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
    synchronized {
      idMap = newMap
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

  def buildReaderMap() {
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
    synchronized {
      readerMap = newMap
    }
  }

  def calculateArchiveSize() = {
    writerFiles().foldLeft(0L) { (sum, file) => sum + file.length() }
  }

  def close() {
    // FIXME
  }

  /**
   * Track state for a queue reader. Every item prior to the "head" pointer (including the "head"
   * pointer itself) has been read by this reader. Separately, "doneSet" is a set of items that
   * have been read out of order, usually because they refer to transactional reads that were
   * confirmed out of order.
   */
  class Reader(file: File) {
    private[this] var _head = 0L
    // FIXME probably use ItemIdList:
    private[this] val _doneSet = new mutable.HashSet[Long]()

    def readState() {
      val journalFile = JournalFile.openReader(file, timer, syncJournal)
      try {
        journalFile.foreach { entry =>
          entry match {
            case JournalFile.Record.ReadHead(id) => _head = id
            case JournalFile.Record.ReadDone(ids) => _doneSet ++= ids
            case x => log.warning("Skipping unknown entry %s in read journal: %s", x, file)
          }
        }
      } finally {
        journalFile.close()
      }
    }

    def checkpoint() {
      val newFile = new File(file.getParent, file.getName + "~~")
      val newJournalFile = JournalFile.createReader(newFile, timer, syncJournal)
      newJournalFile.readHead(_head)
      newJournalFile.readDone(_doneSet.toSeq)
      newJournalFile.close()
      newFile.renameTo(file)
    }

    def head: Long = this._head
    def doneSet: Set[Long] = _doneSet.toSet

    def head_=(id: Long) {
      this._head = id
      val toRemove = _doneSet.filter { _ <= _head }
      _doneSet --= toRemove
    }

    def commit(id: Long) {
      if (id == _head + 1) {
        _head += 1
        while (_doneSet contains _head + 1) {
          _head += 1
          _doneSet -= _head
        }
      } else {
        _doneSet += id
      }
    }
  }
}


/*

  private def uniqueFile(infix: String, suffix: String = ""): File = {
    var file = new File(queuePath, queueName + infix + Time.now.inMilliseconds + suffix)
    while (!file.createNewFile()) {
      Thread.sleep(1)
      file = new File(queuePath, queueName + infix + Time.now.inMilliseconds + suffix)
    }
    file
  }

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

  def erase() {
    try {
      close()
      Journal.archivedFilesForQueue(queuePath, queueName).foreach { filename =>
        new File(queuePath, filename).delete()
      }
      queueFile.delete()
    } catch {
      case _ =>
    }
  }


 */

