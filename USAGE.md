# fastskip — Complete Usage Guide

> Lock-free arena-backed skip list for LSM-tree memtables. Designed for high-throughput write workloads in storage engines like RocksDB, LevelDB, and CockroachDB.

---

## Table of Contents

- [Why fastskip?](#why-fastskip)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Basic Operations](#basic-operations)
  - [Delete / Tombstones](#delete--tombstones)
  - [Iteration](#iteration)
- [LSM Memtable Lifecycle](#lsm-memtable-lifecycle)
- [Concurrent Reads](#concurrent-reads)
- [Range Scans with Cursor](#range-scans-with-cursor)
- [API Reference](#api-reference)
  - [ConcurrentSkipList](#concurrentskiplist)
  - [FrozenMemtable](#frozenmemtable)
  - [Entry](#entry)
  - [Cursor](#cursor)
  - [Snapshot](#snapshot)
  - [InsertError](#inserterror)
  - [SealError](#sealerror)
- [Thread Safety](#thread-safety)
- [Performance](#performance)
- [Design Notes](#design-notes)
- [MSRV](#minimum-supported-rust-version)

---

## Why fastskip?

| Feature | fastskip | dashmap | std::collections::BTreeMap |
|---------|----------|---------|----------------------------|
| Lock-free writes | Yes | Yes | No (global lock) |
| Lock-free reads | Yes | No (read lock) | No |
| Snapshot isolation | Yes | No | No |
| Range cursors | Yes | No | Yes |
| Seal/freeze lifecycle | Yes | No | No |
| Arena memory | Yes (per-thread shards) | No (heap) | No |
| Bulk deallocation | Yes | No | No |

---

## Installation

```toml
[dependencies]
fastskip = "0.1"
```

---

## Quick Start

### Basic Operations

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();

sl.insert(b"user:1001", b"alice");
sl.insert(b"user:1002", b"bob");

let (val, tombstone) = sl.get(b"user:1001").unwrap();
assert_eq!(val, b"alice");
assert!(!tombstone);
```

### Delete / Tombstones

Delete marks a key with a tombstone. Deleted keys remain in the skip list until the memtable is flushed to an SSTable.

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"key", b"value");

// Delete returns true if key was found and marked
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

---

## LSM Memtable Lifecycle

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

// 4. Flush frozen to SSTable (iterate snapshot)
for entry in frozen.iter() {
    if entry.is_tombstone {
        // write tombstone marker to SSTable
    } else {
        // write key-value to SSTable
    }
}

// 5. Drop frozen (reclaims arena memory)
std::mem::drop(frozen);

// 6. Fresh memtable is ready for new writes
fresh.insert(b"key3", b"val3");
```

---

## Concurrent Reads

Readers are fully lock-free. For consistent point-in-time reads under concurrent writes, use a snapshot:

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"a", b"1");
sl.insert(b"b", b"2");

// Snapshot captures a sequence number — iterators skip post-snapshot inserts
let snap = sl.snapshot();

// Insert more after snapshot (won't appear in snapshot iteration)
sl.insert(b"c", b"3");

// Snapshot sees only "a" and "b"
assert_eq!(snap.iter().count(), 2);
// Live iterator sees all three
assert_eq!(sl.iter().count(), 3);
```

---

## Range Scans with Cursor

Use `cursor_at` to seek to a key range:

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"apple", b"1");
sl.insert(b"banana", b"2");
sl.insert(b"cherry", b"3");
sl.insert(b"date", b"4");

// Seek to first key >= "banana"
if let Some(cursor) = sl.cursor_at(b"banana") {
    let keys: Vec<_> = cursor
        .filter(|e| !e.is_tombstone)
        .map(|e| e.key.to_vec())
        .collect();
    assert_eq!(keys, vec![b"banana".to_vec(), b"cherry".to_vec(), b"date".to_vec()]);
}
```

---

## API Reference

### ConcurrentSkipList

The main lock-free skip list. `Send + Sync` — can be shared across threads.

#### Constructors

| Method | Description |
|--------|-------------|
| `new()` | Create with default shard count (CPU count) |
| `with_shards(n)` | Create with `n` arena shards |
| `with_capacity(bytes)` | Create with custom arena capacity |
| `with_capacity_and_shards(bytes, n)` | Create with custom capacity and shards |

```rust
let sl = ConcurrentSkipList::new();
let sl = ConcurrentSkipList::with_shards(8);
let sl = ConcurrentSkipList::with_capacity(1024 * 1024); // 1 MiB
```

#### Write Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `insert` | `fn insert(&self, key: &[u8], value: &[u8]) -> bool` | Insert, returns false on OOM/duplicate |
| `try_insert` | `fn try_insert(&self, key: &[u8], value: &[u8]) -> Result<(), InsertError>` | Typed error on failure |
| `delete` | `fn delete(&self, key: &[u8]) -> bool` | Delete (tombstone), returns false if not found |
| `get_or_insert` | `fn get_or_insert(&self, key: &[u8], value: &[u8]) -> (&[u8], bool)` | Get or insert atomically |

```rust
let sl = ConcurrentSkipList::new();

// Simple insert
sl.insert(b"key", b"value");

// Typed error handling
match sl.try_insert(b"key", b"value") {
    Ok(()) => println!("inserted"),
    Err(InsertError::DuplicateKey) => println!("already exists"),
    Err(InsertError::OutOfMemory) => println!("out of memory"),
}

// Delete
if sl.delete(b"key") {
    println!("marked as deleted");
}

// Get or insert
let (value, is_new) = sl.get_or_insert(b"key", b"default");
```

#### Read Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `fn get(&self, key: &[u8]) -> Option<(&[u8], bool)>` | Returns `(value, is_tombstone)` |
| `get_live` | `fn get_live(&self, key: &[u8]) -> Option<&[u8]>` | Returns `None` if deleted or missing |
| `contains_key` | `fn contains_key(&self, key: &[u8]) -> bool` | Returns true if key exists and is not deleted |

#### Iteration

| Method | Signature | Description |
|--------|-----------|-------------|
| `iter` | `fn iter(&self) -> Iter<'_>` | Live iterator (may see concurrent inserts) |
| `snapshot` | `fn snapshot(&self) -> Snapshot<'_>` | Point-in-time snapshot |
| `cursor` | `fn cursor(&self) -> Cursor<'_>` | Cursor at first entry |
| `cursor_at` | `fn cursor_at(&self, target: &[u8]) -> Option<Cursor<'_>>` | Cursor at first key >= target |

#### Stats

| Method | Signature | Description |
|--------|-----------|-------------|
| `len` | `fn len(&self) -> usize` | Number of live (non-tombstone) entries |
| `is_empty` | `fn is_empty(&self) -> bool` | Returns true if no live entries |
| `is_sealed` | `fn is_sealed(&self) -> bool` | Returns true if sealed |
| `memory_usage` | `fn memory_usage(&self) -> usize` | Total arena bytes allocated |

#### Lifecycle

| Method | Signature | Description |
|--------|-----------|-------------|
| `seal` | `fn seal(self) -> Result<(FrozenMemtable, Self), SealError>` | Seal and create fresh memtable |
| `reset` | `unsafe fn reset(&mut self)` | Reset for reuse (must be quiescent) |

---

### FrozenMemtable

A sealed (read-only) memtable created by `seal()`. Used for flushing to SSTable.

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `iter` | `fn iter(&self) -> Iter<'_>` | Iterate all entries |
| `snapshot` | `fn snapshot(&self) -> Snapshot<'_>` | Point-in-time snapshot |
| `cursor` | `fn cursor(&self) -> Cursor<'_>` | Cursor at first entry |
| `cursor_at` | `fn cursor_at(&self, target: &[u8]) -> Option<Cursor<'_>>` | Cursor at target |
| `len` | `fn len(&self) -> usize` | Live entry count |
| `is_empty` | `fn is_empty(&self) -> bool` | Returns true if empty |
| `memory_usage` | `fn memory_usage(&self) -> usize` | Arena bytes |
| `get` | `fn get(&self, key: &[u8]) -> Option<(&[u8], bool)>` | Point lookup |
| `get_live` | `fn get_live(&self, key: &[u8]) -> Option<&[u8]>` | Point lookup (live only) |

---

### Entry

A key-value entry returned by iterators.

```rust
pub struct Entry {
    pub key: &[u8],
    pub value: &[u8],
    pub is_tombstone: bool,
}
```

---

### Cursor

A positioned iterator for range scans.

| Method | Signature | Description |
|--------|-----------|-------------|
| `next_entry` | `fn next_entry(&mut self) -> bool` | Advance, returns false at end |
| `entry` | `fn entry(&self) -> Option<Entry>` | Current entry |
| `seek` | `fn seek(&mut self, target: &[u8]) -> bool` | Seek to first key >= target |

Implements `Iterator<Entry>`.

```rust
let mut cursor = sl.cursor_at(b"banana").unwrap();
while let Some(entry) = cursor.next_entry() {
    println!("{:?}", entry.key);
}
```

---

### Snapshot

A point-in-time view for consistent iteration.

```rust
let snap = sl.snapshot();
for entry in snap.iter() {
    // Sees only entries that existed when snapshot was taken
}
```

---

### InsertError

```rust
pub enum InsertError {
    DuplicateKey,  // Key already exists
    OutOfMemory,   // Arena shard is out of memory
}
```

---

### SealError

```rust
pub enum SealError {
    AlreadySealed, // Memtable was already sealed
}
```

---

## Thread Safety

`ConcurrentSkipList` is `Send + Sync` and can be shared across threads:

- **Writers**: Multiple threads can call `insert`/`delete` concurrently. Each thread writes to its own arena shard.
- **Readers**: `get`, `iter`, and `snapshot` are lock-free. No locks or atomics required for reads.

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

---

## Performance

### Architecture

- **O(1) insert**: Bump allocation from per-thread arena shard
- **O(log n) get**: Skip list traversal (~log n with small constant)
- **O(log n + m) range scan**: Seek + m iterations
- **Zero allocation for reads**: Readers never allocate

### Memory

- Arena shards: One per thread, avoids allocation contention
- Bulk deallocation: Dropping the memtable reclaims all memory at once
- No per-node free overhead

---

## Design Notes

### Insert After Delete

Inserting a key that was previously tombstoned in the **same** memtable returns `false` (duplicate). This is by design — the skip list maintains one node per key. To re-insert a deleted key, seal the memtable and write to a fresh one.

### Empty Keys

The internal sentinel head node uses an empty key. User-inserted empty keys (`b""`) work correctly for `get`, `insert`, and `delete`, and are yielded by iterators. The sentinel is identified by its position, not key content.

### Memory Management

Arena memory is bulk-allocated in blocks and bulk-reclaimed when the `ConcurrentSkipList` is dropped. No per-node allocation or deallocation overhead.

---

## Minimum Supported Rust Version

**Rust 1.66.0**
