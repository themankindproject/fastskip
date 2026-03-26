# fastskip — Lock-Free Arena-Backed Skip List

> Lock-free arena-backed skip list for LSM-tree memtables. Designed for high-throughput write workloads in storage engines like RocksDB, LevelDB, and CockroachDB.

[![Rust](https://github.com/anomalyco/fastskip/actions/workflows/ci.yml/badge.svg)](https://github.com/anomalyco/fastskip/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Rust 1.66+](https://img.shields.io/badge/Rust-1.66+-blue.svg)](https://blog.rust-lang.org/2022/12/15/Rust-1.66.0.html)

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Usage Guide](#usage-guide)
  - [Basic Operations](#basic-operations)
  - [Delete / Tombstones](#delete--tombstones)
  - [Iteration](#iteration)
  - [LSM Memtable Lifecycle](#lsm-memtable-lifecycle)
  - [Concurrent Reads](#concurrent-reads)
  - [Range Scans](#range-scans)
  - [Thread Safety](#thread-safety)
- [API Reference](#api-reference)
  - [ConcurrentSkipList](#concurrentskiplist)
  - [FrozenMemtable](#frozenmemtable)
  - [Entry](#entry)
  - [Cursor](#cursor)
  - [Snapshot](#snapshot)
  - [Error Types](#error-types)
- [Design Notes](#design-notes)
- [Changelog](#changelog)
- [License](#license)

---

## Features

| Feature | fastskip | dashmap | std::collections::BTreeMap |
|---------|----------|---------|----------------------------|
| Lock-free writes | Yes | Yes | No (global lock) |
| Lock-free reads | Yes | No (read lock) | No |
| Snapshot isolation | Yes | No | No |
| Range cursors | Yes | No | Yes |
| Seal/freeze lifecycle | Yes | No | No |
| Arena memory | Yes (per-thread shards) | No (heap) | No |
| Bulk deallocation | Yes | No | No |

### Key Capabilities

- **O(1) insert** — Bump allocation from per-thread arena shard
- **O(log n) get** — Skip list traversal with small constant
- **Lock-free reads** — No locks required for point lookups
- **Snapshot isolation** — Point-in-time iteration under concurrent writes
- **Memory efficiency** — Per-thread arenas avoid allocation contention
- **Bulk deallocation** — Dropping memtable reclaims all memory at once

---

## Quick Start

```toml
[dependencies]
fastskip = "0.1"
```

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();

sl.insert(b"user:1001", b"alice");
sl.insert(b"user:1002", b"bob");

let (val, tombstone) = sl.get(b"user:1001").unwrap();
assert_eq!(val, b"alice");
assert!(!tombstone);
```

---

## Architecture

### Memory Model

The skip list uses **per-thread arena shards** for allocation:

```
Thread 0 → Arena 0 (bump pointer, no sync)
Thread 1 → Arena 1 (bump pointer, no sync)
Thread 2 → Arena 2 (bump pointer, no sync)
          ↓
    SkipList (atomic tower pointers)
```

Each writer thread has its own arena shard. Writes don't contend on allocation — they're pure bump pointer operations. The skip list itself uses atomic operations for the lock-free CAS at level 0.

### Node Layout

Nodes are allocated as a single contiguous block:

```
[ header (32 bytes) ]
[ tower[0..height-1] ]  (8 bytes each)
[ key bytes ]
[ value bytes ]
```

Key and value are stored inline after the tower, enabling zero-copy reads.

---

## Usage Guide

### Basic Operations

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();

// Insert key-value pairs
sl.insert(b"user:1001", b"alice");
sl.insert(b"user:1002", b"bob");

// Point lookup
let (val, is_tombstone) = sl.get(b"user:1001").unwrap();
assert_eq!(val, b"alice");
assert!(!is_tombstone);

// Check if key exists (non-deleted)
assert!(sl.contains_key(b"user:1001"));

// Get or insert atomically
let (val, is_new) = sl.get_or_insert(b"user:1003", b"charlie");
assert!(is_new);
```

### Delete / Tombstones

Delete marks a key with a tombstone. Deleted keys remain in the skip list until flushed to SSTable.

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"key", b"value");

// Delete returns true if key was found
assert!(sl.delete(b"key"));

// get still returns the key, but with tombstone = true
let (_, tombstone) = sl.get(b"key").unwrap();
assert!(tombstone);

// get_live filters out tombstones
assert_eq!(sl.get_live(b"key"), None);
```

### Iteration

Iterate in sorted key order:

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"apple", b"1");
sl.insert(b"banana", b"2");
sl.insert(b"cherry", b"3");

for entry in sl.iter() {
    println!("{:?} -> {:?}", entry.key, entry.value);
}
```

### LSM Memtable Lifecycle

The intended lifecycle for an LSM-tree storage engine:

```rust
use fastskip::ConcurrentSkipList;

// 1. Create active memtable
let memtable = ConcurrentSkipList::with_shards(4);

// 2. Concurrent writers insert/delete
memtable.insert(b"key1", b"val1");
memtable.delete(b"key2");

// 3. When full, seal it — returns frozen (for flushing) + fresh (for writes)
let (frozen, fresh) = memtable.seal().unwrap();

// 4. Flush frozen to SSTable
for entry in frozen.iter() {
    if entry.is_tombstone {
        // write tombstone marker
    } else {
        // write key-value
    }
}

// 5. Drop frozen (reclaims arena memory)
std::mem::drop(frozen);

// 6. Fresh memtable is ready for new writes
fresh.insert(b"key3", b"val3");
```

### Concurrent Reads

Readers are fully lock-free. For consistent point-in-time reads, use a snapshot:

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"a", b"1");
sl.insert(b"b", b"2");

// Snapshot captures sequence number — skips post-snapshot inserts
let snap = sl.snapshot();

// Insert more after snapshot
sl.insert(b"c", b"3");

// Snapshot sees only "a" and "b"
assert_eq!(snap.iter().count(), 2);
// Live iterator sees all three
assert_eq!(sl.iter().count(), 3);
```

### Range Scans

Use `cursor_at` to seek to a key range:

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"apple", b"1");
sl.insert(b"banana", b"2");
sl.insert(b"cherry", b"3");
sl.insert(b"date", b"4");

// Seek to first key >= "banana"
let mut cursor = sl.cursor_at(b"banana").unwrap();
while let Some(entry) = cursor.next_entry() {
    println!("{:?}", entry.key);
}
// Output: banana, cherry, date
```

### Thread Safety

`ConcurrentSkipList` is `Send + Sync` — can be shared across threads:

```rust
use fastskip::ConcurrentSkipList;
use std::sync::Arc;
use std::thread;

let sl = Arc::new(ConcurrentSkipList::new());

let sl1 = sl.clone();
let t1 = thread::spawn(move || {
    for i in 0..1000 {
        sl1.insert(format!("key:{}", i).as_bytes(), b"value");
    }
});

let sl2 = sl.clone();
let t2 = thread::spawn(move || {
    for i in 1000..2000 {
        sl2.insert(format!("key:{}", i).as_bytes(), b"value");
    }
});

t1.join().unwrap();
t2.join().unwrap();

assert_eq!(sl.len(), 2000);
```

### Memory Stats

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"key", b"value");

println!("allocated: {} bytes", sl.memory_usage());
println!("reserved: {} bytes", sl.memory_reserved());
println!("utilization: {:.1}%", sl.memory_utilization() * 100.0);
println!("idle: {} bytes", sl.memory_idle());
```

---

## API Reference

### ConcurrentSkipList

The main lock-free skip list. `Send + Sync`.

#### Constructors

| Method | Description |
|--------|-------------|
| `new()` | Create with default shard count (CPU count) |
| `with_shards(n)` | Create with `n` arena shards |
| `with_capacity(bytes)` | Create with custom arena capacity |
| `with_capacity_and_shards(bytes, n)` | Custom capacity and shards |

#### Write Operations

| Method | Description |
|--------|-------------|
| `insert(key, value)` | Insert, returns false on OOM/duplicate |
| `try_insert(key, value)` | Returns `Result<(), InsertError>` |
| `delete(key)` | Delete (tombstone), returns false if not found |
| `get_or_insert(key, value)` | Get or insert atomically |

#### Read Operations

| Method | Description |
|--------|-------------|
| `get(key)` | Returns `Option<(value, is_tombstone)>` |
| `get_live(key)` | Returns `None` if deleted or missing |
| `contains_key(key)` | Returns true if key exists and not deleted |

#### Iteration

| Method | Description |
|--------|-------------|
| `iter()` | Live iterator |
| `snapshot()` | Point-in-time snapshot |
| `cursor()` | Cursor at first entry |
| `cursor_at(target)` | Cursor at first key >= target |

#### Stats

| Method | Description |
|--------|-------------|
| `len()` | Number of live entries |
| `is_empty()` | Returns true if no live entries |
| `is_sealed()` | Returns true if sealed |
| `memory_usage()` | Arena bytes allocated |
| `memory_reserved()` | Arena bytes reserved |
| `memory_utilization()` | Utilization (0.0 to 1.0) |
| `memory_idle()` | Reserved but unused bytes |

#### Lifecycle

| Method | Description |
|--------|-------------|
| `seal()` | Seal and create fresh memtable |
| `reset()` (unsafe) | Reset for reuse |

---

### FrozenMemtable

A sealed (read-only) memtable created by `seal()`.

| Method | Description |
|--------|-------------|
| `iter()` | Iterate all entries |
| `snapshot()` | Point-in-time snapshot |
| `cursor()` / `cursor_at()` | Range scan |
| `len()` / `is_empty()` | Entry count |
| `memory_*()` | Memory stats |
| `get()` / `get_live()` | Point lookup |

---

### Entry

```rust
pub struct Entry<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub is_tombstone: bool,
}
```

---

### Cursor

A positioned iterator for range scans.

| Method | Description |
|--------|-------------|
| `next_entry()` | Advance, returns false at end |
| `entry()` | Current entry |
| `seek(target)` | Seek to first key >= target |

Implements `Iterator<Entry>`.

---

### Snapshot

A point-in-time view for consistent iteration.

```rust
let snap = sl.snapshot();
for entry in snap.iter() {
    // Sees only entries at snapshot time
}
```

---

### Error Types

```rust
pub enum InsertError {
    DuplicateKey,  // Key already exists
    OutOfMemory,   // Arena shard out of memory
}

pub enum SealError {
    AlreadySealed, // Memtable was already sealed
}
```

---

## Design Notes

### Insert After Delete

Inserting a key that was tombstoned in the same memtable returns `false` (duplicate). Seal the memtable and write to a fresh one to re-insert.

### Empty Keys

User-inserted empty keys (`b""`) work correctly. The sentinel head node uses an empty key identified by position.

### Memory Management

Arena memory is bulk-allocated in blocks and bulk-reclaimed when the memtable is dropped. No per-node allocation/deallocation overhead.

---

## Changelog

### v0.1.0

- Initial release
- Lock-free skip list with per-thread arena shards
- Point lookups, iteration, snapshots
- Tombstone delete support
- Seal/freeze lifecycle for LSM memtables
- Range cursors for prefix scans

### v0.1.1

- Added memory utilization metrics:
  - `memory_reserved()` — total arena capacity
  - `memory_utilization()` — fraction used (0.0-1.0)
  - `memory_idle()` — unused bytes
- Updated documentation

---

## License

MIT License — see [LICENSE](LICENSE) for details.
