
# Journal file format

There are two types of journal file: writer and reader. A writer file is a
sequence of "put" operations -- an infinite scroll of all items ever added to
the queue. A reader file represents the state of a single reader: its current
head position, and any items that have been confirmed out-of-order. Each type
of file uses the same low-level format, differentiated only by their header
bytes.

## Filenames

All filenames for a queue begin with that queue name followed by a dot. For
example, the journal files for the queue "jobs" will all begin with "`jobs.`".

Reader files are followed by "`read.`" and the name of the reader, which might
be an empty string if there is only one reader. The default (empty-string)
reader for "jobs" would be "`jobs.read.`". For a reader named "indexer", it
would be "`jobs.read.indexer`".

Writer files are followed by an always-incrementing number, usually the
current time in milliseconds. An example journal file for the "jobs" queue is
"`jobs.1321401903634`". These journal files are always sorted numerically,
with smaller numbers being older segments of the journal.

## Low-level format

Each file contains a 4-byte identifying header followed by a sequence of records.

Each record is:

- command (1 byte)
- header bytes (optional)
- data bytes (optional)

The command byte is made up of 8 bits:

    . 7   6   5   4   3   2   1   0
    +---+---+---+---+---+---+---+---+
    | command       | header size   |
    +---+---+---+---+---+---+---+---+

The header size is the number of 32-bit header words following the command
byte. In other words, it's the number of header bytes divided by 4, so there
may be from 0 to 64 bytes of header.

Commands with the high bit set (8 - 15) are followed by a block of data. In
this case, there is always at least 4 bytes of header, and the first 32 bits
of header is a count of the number of data bytes to follow.

For example, command byte 0x73 represents command 7, which has no data block
and 12 bytes of header.

All header data is in little-endian format.

All enqueued items have an ID, which is a non-zero 64-bit number.

## Write journal

header: 27 64 26 03

### PUT (8)

An item was added to the queue. This is the only entry in the writer files.
The header size is either 6 or 8.

Header:

  - i32 data size
  - i32 error_count
  - i64 xid
  - i64 add_time (msec)
  - i64 expire_time (msec) [optional]

Data:

  - (bytes)

## Read checkpoint

header: 26 3C 26 03

### READ_HEAD (0)

The id of the current queue head for this reader. The header size is always
2.

Header:

  - i64 id (0 for empty queue)

### READ_DONE (9)

A set of item ids for items that have been confirmed as removed from the queue
out-of-order. These ids will always be greater than the current head id. The
header size is always 1, and the data block is a sequence of 64-bit ids.

Header:

  - i32 count

Data:

  - i64* xid

