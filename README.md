
# libkestrel

Libkestrel is a library for scala/java containing:

- ConcurrentBlockingQueue - a lock-free queue that allows readers to block
  (wait) for items

- JournaledQueue - a queue with potentially many read-pointers, where state
  of the queue and its readers is saved to disk

These are variants and improvements of the building blocks of the kestrel
distributed queue server project. The intent (as of November 2011) is to make
kestrel to use this library in its next major release.


## Build

    $ sbt clean update package-dist


## ConcurrentBlockingQueue

ConcurrentBlockingQueue extends the idea of java's `ConcurrentLinkedQueue` to
allow consumers to block, indefinitely or with a timeout, until items arrive.

It works by having one `ConcurrentLinkedQueue` for the queue itself, and
another one to track waiting consumers. Each time an item is put into the
queue, it's handed off to the next waiting consumer, like an airport taxi
line.

The handoff occurs in a serialized block (like the `Serialized` trait in
util-core), so when there's no contention, a new item is handed directly from
the producer to the consumer. When there is contention, producers increment a
counter representing how many pent-up handoffs there are, and the producer
that got into the serialized block first will do each handoff in order until
the count is zero again. This way, no producer is ever blocked.

Consumers receive a future that will eventually be fulfilled either with
`Some(item)` if an item arrived before the requested timeout, or `None` if the
request timed out. If an item was available immediately, the future will be
fulfilled before the consumer receives it. This way, no consumer is ever
blocked.


## JournaledQueue

A JournaledQueue is an optionally-journaled queue that may have multiple
"readers", each of which may have multiple consumers. Various policies
determine how much of each queue is kept in memory, how large the queue can
get, and when items expire.

### Puts

When an item is added to a queue, it's journaled and passed on to any readers.
There is always at least one reader, and the reader contains the actual
in-memory queue. If there are multiple readers, they behave as multiple
independent queues, each receiving a copy of each item added to the
`JournaledQueue`, but sharing a single journal. They may have different
policies on memory use and expiration.

### Gets

Items are read only from readers. When an item is fetched, it's set aside as
an "open read", but not committed to the journal. A separate call is made to
either commit the item or abort it. Aborting an item returns it to the head of
the queue to be given to the next consumer.

Periodically each reader records its state in a separate checkpoint journal.
On startup, if a journal already exists for a queue and its readers, each
reader restores itself from this saved state. If the queues were not shutdown
cleanly, the state files may be out of date and items may be replayed. Care is
taken never to let any of the journal files be corrupted or in a
non-recoverable state. In case of error, the choice is always made to possibly
replay items instead of losing them.


## Tests

Some load tests are included. You can run them with

    $ ./dist/libkestrel/scripts/qtest

which will list the available tests. Each test responds to "`--help`".


## File by file overview

- BlockingQueue - interface for any blocking queue

- SimpleBlockingQueue - a simple queue using "synchronized", based on the one
  in kestrel 2.1

- ConcurrentBlockingQueue - a lock-free BlockingQueue (see above)

- PeriodicSyncFile - a writable file that `fsync`s on a schedule

- ItemIdList - a simple Set[Long] implementation optimized for small sets

- QueueItem - an item plus associated metadata (item ID, expiration time)

- JournalFile - a single on-disk journal file, encapsulating reading & writing
  journal records

- Journal - representation of a collection of files (the writer files and a
  file for each reader)

- JournaledQueue - a `Journal` and its in-memory components (see above)


## Improvement targets

- Trustin suggested that the read-pointer files could be, instead of an id, a
  filename and position. That would save us from reading the first half of a
  journal file on startup (to find the id).

- Nick suggested that writing all of the readers into the same file could
  reduce disk I/O by writing fewer blocks during reader checkpointing.

- The periodic fsync is directly copied from kestrel, so it has the same
  scaling problems Stephan is poking at.
