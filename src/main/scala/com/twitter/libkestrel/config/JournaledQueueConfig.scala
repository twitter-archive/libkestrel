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
package config

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.util._
import java.io.File

/**
 * Configuration for a queue reader. Each JournaledQueue has at least one reader. Fanout queues
 * have multiple readers. Readers hold an in-memory buffer of the enqueued items (up to a limit)
 * and enforce policy on maximum queue size and item expiration.
 *
 * @param maxItems Set a hard limit on the number of items this queue can hold. When the queue is
 *   full, `fullPolicy` dictates the behavior when a client attempts to add another item.
 * @param maxSize Set a hard limit on the number of bytes (of data in queued items) this queue can
 *   hold. When the queue is full, `fullPolicy` dictates the behavior when a client attempts
 *   to add another item.
 * @param maxAge Expiration time for items on this queue. Any item that has been sitting on the
 *   queue longer than this duration will be discarded. Clients may also attach an expiration time
 *   when adding items to a queue, in which case the item expires at the earlier of the two
 *   expiration times.
 * @param fullPolicy What to do if a client attempts to add items to a queue that's reached its
 *   maxItems or maxSize.
 * @param processExpiredItem What to do with items that are expired from this queue. This can be
 *   used to implement special processing for expired items, such as moving them to another queue
 *   or writing them into a logfile.
 * @param errorHandler Any special action to take when an item is given to a client and the client
 *   aborts it. The `errorCount` of the item will already be incremented. Return `false` if the
 *   normal action (give the item to another client) should happen. Return `true` if libkestrel
 *   should consider the matter taken care of, and not do anything more.
 * @param maxExpireSweep Maximum number of expired items to process at once.
 * @param maxQueueAge If the queue is empty and has existed at least this long, it will be deleted.
 * @param deliveryLatency Code to execute if you wish to track delivery latency (the time between
 *   an item being added and a client being ready to receive it). Normally you would hook this up
 *   to a stats collection library like ostrich.
 * @param timeoutLatency Code to execute if you wish to track timeout latency (the time a client
 *   actually spent waiting for an item to arrive before it timed out). Normally you would hook
 *   this up to a stats collection library like ostrich.
 */
case class JournaledQueueReaderConfig(
  maxItems: Int = Int.MaxValue,
  maxSize: StorageUnit = Long.MaxValue.bytes,
  maxAge: Option[Duration] = None,
  fullPolicy: ConcurrentBlockingQueue.FullPolicy = ConcurrentBlockingQueue.FullPolicy.RefusePuts,
  processExpiredItem: (QueueItem) => Unit = { _ => },
  errorHandler: (QueueItem) => Boolean = { _ => false },
  maxExpireSweep: Int = Int.MaxValue,
  maxQueueAge: Option[Duration] = None,
  deliveryLatency: (JournaledQueue#Reader, Duration) => Unit = { (_, _) => },
  timeoutLatency: (JournaledQueue#Reader, Duration) => Unit = { (_, _) => }
) {
  override def toString() = {
    ("maxItems=%d maxSize=%s maxAge=%s fullPolicy=%s maxExpireSweep=%d " +
     "maxQueueAge=%s").format(
      maxItems, maxSize, maxAge, fullPolicy, maxExpireSweep, maxQueueAge)
  }
}

/**
 * Configuration for a journaled queue. All of the parameters have reasonable defaults, but can be
 * overridden.
 *
 * @param name Name of the queue being configured.
 * @param maxItemSize Set a hard limit on the number of bytes a single queued item can contain. A
 *   put request for an item larger than this will be rejected.
 * @param journalSize Maximum size of an individual journal file before libkestrel moves to a new
 *   file. In the (normal) state where a queue is usually empty, this is the amount of disk space
 *   a queue should consume before moving to a new file and erasing the old one.
 * @param syncJournal How often to sync the journal file. To sync after every write, set this to
 *   `0.milliseconds`. To never sync, set it to `Duration.MaxValue`. Syncing the journal will
 *   reduce the maximum throughput of the server in exchange for a lower chance of losing data.
 * @param saveArchivedJournals Optionally move "retired" journal files to this folder. Normally,
 *   once a journal file only refers to items that have all been removed, it's erased.
 * @param checkpointTimer How often to checkpoint the journal and readers associated with this queue.
 *   Checkpointing stores each reader's position on disk and causes old journal files to be deleted
 *   (provided that all extant readers are finished with them).
 * @param readerConfigs Configuration to use for readers of this queue.
 * @param defaultReaderConfig Configuration to use for readers of this queue when the reader name
 *   isn't in readerConfigs.
 */
case class JournaledQueueConfig(
  name: String,
  maxItemSize: StorageUnit = Long.MaxValue.bytes,
  journalSize: StorageUnit = 16.megabytes,
  syncJournal: Duration = Duration.MaxValue,
  saveArchivedJournals: Option[File] = None,
  checkpointTimer: Duration = 1.second,

  readerConfigs: Map[String, JournaledQueueReaderConfig] = Map.empty,
  defaultReaderConfig: JournaledQueueReaderConfig = new JournaledQueueReaderConfig()
) {
  override def toString() = {
    ("name=%s maxItemSize=%s journalSize=%s syncJournal=%s " +
     "saveArchivedJournals=%s checkpointTimer=%s").format(
      name, maxItemSize, journalSize, syncJournal, saveArchivedJournals,
      checkpointTimer)
  }

  def readersToStrings(): Seq[String] = {
    List("<default>: " + defaultReaderConfig.toString) ++ readerConfigs.map { case (k, v) =>
      "%s: %s".format(k, v)
    }
  }
}

