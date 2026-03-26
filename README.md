# fastskip

[![Crates.io](https://img.shields.io/crates/v/fastskip)](https://crates.io/crates/fastskip)
[![Documentation](https://docs.rs/fastskip/badge.svg)](https://docs.rs/fastskip)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/anomalyco/fastskip/ci.yml)](https://github.com/anomalyco/fastskip/actions)
![Rust Version](https://img.shields.io/badge/rust-1.66%2B-blue)

Lock-free arena-backed skip list for LSM-tree memtables. Designed for high-throughput write workloads in storage engines like RocksDB, LevelDB, and CockroachDB.

## Why fastskip?

| Feature | Description |
|---------|-------------|
| **Lock-free writes** | Multiple threads insert/delete concurrently via single CAS at level 0 |
| **Lock-free reads** | Point lookups walk the skip list without any locks |
| **O(1) allocation** | Per-thread arena shards — zero-contention bump allocation |
| **Snapshot isolation** | Point-in-time snapshots remain consistent under concurrent writes |
| **Range cursors** | Cursor with lower-bound seek for prefix/range iteration |
| **Seal/freeze lifecycle** | `seal()` freezes memtable for flushing, returns fresh one |
| **Bulk deallocation** | Dropping memtable reclaims all arena memory at once |

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

// Check if key exists (non-deleted)
assert!(sl.contains_key(b"user:1001"));

// Iterate in sorted order
for entry in sl.iter() {
    println!("{:?} -> {:?}", entry.key, entry.value);
}
```

### Delete / Tombstones

Delete marks a key with a tombstone. Deleted keys remain in the skip list until flushed to SSTable.

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"key", b"value");

// Delete returns true if key was found
assert!(sl.delete(b"key"));

// get still returns the key, but tombstone = true
let (_, tombstone) = sl.get(b"key").unwrap();
assert!(tombstone);

// get_live filters out tombstones
assert_eq!(sl.get_live(b"key"), None);
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

// 3. When full, seal — returns frozen (for flush) + fresh (for writes)
let (frozen, fresh) = memtable.seal().unwrap();

// 4. Flush frozen to SSTable
for entry in frozen.iter() {
    if entry.is_tombstone {
        // write tombstone marker to SSTable
    } else {
        // write key-value to SSTable
    }
}

// 5. Drop frozen (reclaims arena memory)
std::mem::drop(frozen);

// 6. Fresh memtable ready for new writes
fresh.insert(b"key3", b"val3");
```

### Snapshots — Point-in-Time Iteration

Readers are fully lock-free. For consistent reads under concurrent writes, use a snapshot:

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"a", b"1");
sl.insert(b"b", b"2");

// Snapshot captures sequence number — skips post-snapshot inserts
let snap = sl.snapshot();

// Insert more after snapshot (won't appear in snapshot)
sl.insert(b"c", b"3");

// Snapshot sees only "a" and "b"
assert_eq!(snap.iter().count(), 2);
// Live iterator sees all three
assert_eq!(sl.iter().count(), 3);
```

### Range Scans

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

### Memory Stats

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"key", b"value");

println!("allocated: {} bytes", sl.memory_usage());
println!("reserved: {} bytes", sl.memory_reserved());
println!("utilization: {:.1}%", sl.memory_utilization() * 100.0);
```

## Use Cases

- **LSM-tree memtables** — RocksDB, LevelDB, CockroachDB style storage engines
- **Write-heavy key-value stores** — high-throughput ingestion pipelines
- **Time-series append** — sequential inserts, periodic flush to disk
- **Request-scoped caches** — per-request memtable, bulk reclaim on completion

See [USAGE.md](USAGE.md) for complete API reference.

## Configuration

```rust
use fastskip::ConcurrentSkipList;

// Default: CPU count shards, 64KB initial arena per shard
let sl = ConcurrentSkipList::new();

// Custom shard count
let sl = ConcurrentSkipList::with_shards(8);

// Custom capacity and shards
let sl = ConcurrentSkipList::with_capacity_and_shards(1024 * 1024, 8);
```

## Thread Safety

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

## Design

| Property | Detail |
|----------|--------|
| Algorithm | Lock-free skip list (RocksDB InlineSkipList / CockroachDB arenaskl) |
| Concurrency | Single CAS at level 0, best-effort CAS at upper levels |
| Memory | Per-thread arena shards, no per-node free |
| Ordering | Lexicographic byte ordering |
| Duplicates | Rejected (first value wins) |
| Delete | Tombstone (entry stays, marked deleted) |
| Re-insert after delete | Seal memtable, write to fresh one |

## Testing

```bash
cargo test              # Unit, concurrent, stress, correctness tests
cargo clippy            # Zero warnings
cargo run --example basic
cargo run --example lsm_memtable
```

## Documentation

See [USAGE.md](USAGE.md) for complete API reference and examples.
