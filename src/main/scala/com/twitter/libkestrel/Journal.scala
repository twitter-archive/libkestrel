package com.twitter.libkestrel

import com.twitter.logging.Logger
import com.twitter.util._
import java.io.{File, IOException}
import scala.collection.immutable

/*
 *
 * journal:
 * - rotate to new file after X
 * - checkpoint reader
 * - read-behind pointer for reader
 * - clean up old files if they're dead
 */

class Reader {

}

object Journal {
  def getQueueNamesFromFolder(path: File): Set[String] = {
    path.list().filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }.toSet
  }

/*  // list of
  def filesForQueue(path: File):
  /**
   * Find all the archived (non-current) journal files for a queue, sort them in replay order (by
   * timestamp), and erase the remains of any unfinished business that we find along the way.
   */
  @tailrec
  def archivedFilesForQueue(path: File, queueName: String): List[String] = {
    val totalFiles = path.list()
    if (totalFiles eq null) {
      // directory is gone.
      Nil
    } else {
      val timedFiles = totalFiles.filter {
        _.startsWith(queueName + ".")
      }.map { filename =>
        (filename, filename.split('.')(1).toLong)
      }.sortBy { case (filename, timestamp) =>
        timestamp
      }.toList

      if (cleanUpPackedFiles(path, timedFiles)) {
        // probably only recurses once ever.
        archivedFilesForQueue(path, queueName)
      } else {
        timedFiles.map { case (filename, timestamp) => filename }
      }
    }
  }

  def journalsForQueue(path: File, queueName: String): List[String] = {
    archivedFilesForQueue(path, queueName) ++ List(queueName)
  }

  def journalsBefore(path: File, queueName: String, filename: String): Seq[String] = {
    journalsForQueue(path, queueName).takeWhile { _ != filename }
  }

  def journalAfter(path: File, queueName: String, filename: String): Option[String] = {
    journalsForQueue(path, queueName).dropWhile { _ != filename }.drop(1).headOption
  }

  val packerQueue = new LinkedBlockingQueue[PackRequest]()
  val packer = BackgroundProcess.spawnDaemon("journal-packer") {
    while (true) {
      val request = packerQueue.take()
      try {
        request.journal.pack(request)
        request.journal.outstandingPackRequests.decrementAndGet()
      } catch {
        case e: Throwable =>
          Logger.get(getClass).error(e, "Uncaught exception in packer: %s", e)
      }
    }
  }
}

*/
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

  buildIdMap()

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
      name.startsWith(queueName + ".") && !name.split("\\.")(1).find { !_.isDigit }.isDefined
    }.map { name =>
      new File(queuePath, name)
    }
  }

  def readerFiles() = {
    queuePath.list().filter { name =>
      name.startsWith(queueName + ".read.")
    }.map { name =>
      new File(queuePath, name)
    }
  }
  
  def fileForId(id: Long): Option[File] = {
    idMap.to(id).lastOption.map { case (k, v) => v }
  }
}

/*
 *   def calculateArchiveSize() {
    val files = Journal.archivedFilesForQueue(queuePath, queueName)
    archivedSize = files.foldLeft(0L) { (sum, filename) =>
      sum + new File(queuePath, filename).length()
    }
  }

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


  def close() {
    writer.close()
    reader.foreach { _.close() }
    reader = None
    readerFilename = None
    closed = true
    waitForPacksToFinish()
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


/*def replayFile(name: String, filename: String)(f: JournalItem => Unit): Unit = {
    log.debug("Replaying '%s' file %s", name, filename)
    size = 0
    var lastUpdate = 0L
    try {
      val in = new FileInputStream(new File(queuePath, filename).getCanonicalPath).getChannel
      replayer = Some(in)
      replayerFilename = Some(filename)
      try {
        var done = false
        do {
          readJournalEntry(in) match {
            case (JournalItem.EndOfFile, _) =>
              done = true
            case (x, itemsize) =>
              size += itemsize
              f(x)
              if (size > lastUpdate + 10.megabytes.inBytes) {
                log.info("Continuing to read '%s' journal (%s); %s so far...", name, filename, size.bytes.toHuman())
                lastUpdate = size
              }
          }
        } while (!done)
      } catch {
        case e: BrokenItemException =>
          log.error(e, "Exception replaying journal for '%s': %s", name, filename)
          log.error("DATA MAY HAVE BEEN LOST! Truncated entry will be deleted.")
          truncateJournal(e.lastValidPosition)
      }
    } catch {
      case e: FileNotFoundException =>
        log.info("No transaction journal for '%s'; starting with empty queue.", name)
      case e: IOException =>
        log.error(e, "Exception replaying journal for '%s': %s", name, filename)
        log.error("DATA MAY HAVE BEEN LOST!")
        // this can happen if the server hardware died abruptly in the middle
        // of writing a journal. not awesome but we should recover.
    }
    replayer = None
    replayerFilename = None
  }
*/
