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
package tools

import java.io.{File, FileNotFoundException, IOException}
import scala.collection.mutable
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time}

class QueueDumper(filename: String, quiet: Boolean, dump: Boolean, dumpRaw: Boolean, reader: Boolean) {
  var operations = 0L
  var bytes = 0L
  var firstId = 0L
  var lastId = 0L

  def verbose(s: String, args: Any*) {
    if (!quiet) {
      print(s.format(args: _*))
    }
  }

  def apply() {
    try {
      val file: RecordReader = if (reader) {
        BookmarkFile.open(new File(filename))
      } else {
        JournalFile.open(new File(filename))
      }
      var lastDisplay = 0L

      var position = file.position
      file.foreach { record =>
        operations += 1
        dumpItem(position, record)
        if (quiet && !dumpRaw && file.position - lastDisplay > 1024 * 1024) {
          print("\rReading journal: %-6s".format(file.position.bytes.toHuman))
          Console.flush()
          lastDisplay = file.position
        }
        position = file.position
      }
      if (!dumpRaw) {
        print("\r" + (" " * 30) + "\r")
      }

      if (!dumpRaw) {
        println()
        println("Journal size: %d operations, %d bytes.".format(operations, file.position))
        if (firstId > 0) println("Ids %d - %d.".format(firstId, lastId))
      }
    } catch {
      case e: FileNotFoundException =>
        Console.err.println("Can't open journal file: " + filename)
      case e: IOException =>
        Console.err.println("Exception reading journal file: " + filename)
        e.printStackTrace(Console.err)
    }
  }

  def dumpItem(position: Long, record: Record) {
    val now = Time.now
    verbose("%08x  ", position & 0xffffffffL)
    record match {
      case Record.Put(item) =>
        if (firstId == 0) firstId = item.id
        lastId = item.id
        if (!quiet) {
          verbose("PUT %-6d id=%d", item.dataSize, item.id)
          if (item.expireTime.isDefined) {
            if (item.expireTime.get - now < 0.milliseconds) {
              verbose(" expired")
            } else {
              verbose(" exp=%s", item.expireTime.get - now)
            }
          }
          if (item.errorCount > 0) verbose(" errors=%d", item.errorCount)
          verbose("\n")
        }
        val data = new Array[Byte](item.dataSize)
        item.data.get(data)
        if (dump) {
          println("    " + new String(data, "ISO-8859-1"))
        } else if (dumpRaw) {
          print(new String(data, "ISO-8859-1"))
        }
      case Record.ReadHead(id) =>
        verbose("HEAD %d\n", id)
      case Record.ReadDone(ids) =>
        verbose("DONE %s\n", ids.sorted.mkString("(", ", ", ")"))
      case x =>
        verbose(x.toString)
    }
  }
}


object QueueDumper {
  val filenames = new mutable.ListBuffer[String]
  var quiet = false
  var dump = false
  var dumpRaw = false
  var reader = false

  def usage() {
    println()
    println("usage: queuedumper <journal-files...>")
    println("    describe the contents of a kestrel journal file")
    println()
    println("options:")
    println("    -q      quiet: don't describe every line, just the summary")
    println("    -d      dump contents of added items")
    println("    -A      dump only the raw contents of added items")
    println("    -R      file is a reader pointer")
    println()
  }

  def parseArgs(args: List[String]) {
    args match {
      case Nil =>
      case "--help" :: xs =>
        usage()
        System.exit(0)
      case "-q" :: xs =>
        quiet = true
        parseArgs(xs)
      case "-d" :: xs =>
        dump = true
        parseArgs(xs)
      case "-A" :: xs =>
        dumpRaw = true
        quiet = true
        parseArgs(xs)
      case "-R" :: xs =>
        reader = true
        parseArgs(xs)
      case x :: xs =>
        filenames += x
        parseArgs(xs)
    }
  }

  def main(args: Array[String]) {
    parseArgs(args.toList)
    if (filenames.size == 0) {
      usage()
      System.exit(0)
    }

    for (filename <- filenames) {
      if (!quiet) println("Queue: " + filename)
      new QueueDumper(filename, quiet, dump, dumpRaw, reader)()
    }
  }
}
