# fastskip

[![Crates.io](https://img.shields.io/crates/v/fastskip)](https://crates.io/crates/fastskip)
[![Documentation](https://docs.rs/fastskip/badge.svg)](https://docs.rs/fastskip)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/themankindproject/fastskip/ci.yml)](https://github.com/themankindproject/fastskip/actions)
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
| **Auto-seal thresholds** | Automatic sealing based on memory usage or entry count |
| **Backpressure detection** | Detect when nearing capacity limits |
| **Batch operations** | Efficient bulk insert and multi-get operations |

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

### With Auto-Seal Thresholds

```rust
use fastskip::ConcurrentSkipList;

// Create with auto-seal at 1MB memory or 100k entries
let sl = ConcurrentSkipList::with_capacity_and_shards(
    64 * 1024,      // 64KB initial arena per shard
    4,              // 4 shards (should match writer thread count)
    1 * 1024 * 1024, // Auto-seal at 1MB total memory
    100_000         // Auto-seal at 100k entries
);

// Check if nearing limits
if !sl.is_under_backpressure() {
    // Safe to insert batch
    let batch = vec![(b"key1", b"val1"), (b"key2", b"val2")];
    sl.insert_batch(&batch).unwrap();
}

// Get memory statistics
println!("Used: {} bytes", sl.memory_usage());
println!("Utilization: {:.1}%", sl.memory_utilization() * 100.0);
println!("Avg key size: {} bytes", sl.avg_key_size() as usize);
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

// 1. Create active memtable with auto-seal thresholds
let memtable = ConcurrentSkipList::with_capacity_and_shards(
    10 * 1024 * 1024, // 10 MB per shard
    4,                // 4 shards
    50 * 1024 * 1024, // Auto-seal at 50 MB total
    1_000_000         // Auto-seal at 1M entries
);

// 2. Concurrent writers insert/delete
memtable.insert(b"key1", b"val1");
memtable.delete(b"key2");

// 3. Check for backpressure before inserting batch
if !memtable.is_under_backpressure() {
    let batch = vec![(b"key3", b"val3"), (b"key4", b"val4")];
    memtable.insert_batch(&batch).unwrap();
}

// 4. When full (auto-sealed or manual), seal it — returns frozen (for flush) + fresh (for writes)
let (frozen, fresh) = memtable.seal().unwrap();

// 5. Flush frozen to SSTable (iterate snapshot)
for entry in frozen.iter() {
    if entry.is_tombstone {
        // write tombstone marker to SSTable
    } else {
        // write key-value to SSTable
    }
}

// 6. Drop frozen (reclaims arena memory)
std::mem::drop(frozen);

// 7. Fresh memtable ready for new writes
fresh.insert(b"key5", b"val5");

// Monitor memory usage
println!("Memory usage: {} bytes", fresh.memory_usage());
println!("Utilization: {:.1}%", fresh.memory_utilization() * 100.0);
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
println!("idle: {} bytes", sl.memory_idle());
println!("avg key size: {} bytes", sl.avg_key_size() as usize);
println!("avg value size: {} bytes", sl.avg_value_size() as usize);
println!("total insert attempts: {}", sl.total_inserts());
println!("under backpressure: {}", sl.is_under_backpressure());
```

## Use Cases

- **LSM-tree memtables** — RocksDB, LevelDB, CockroachDB style storage engines
- **Write-heavy key-value stores** — high-throughput ingestion pipelines
- **Time-series append** — sequential inserts, periodic flush to disk
- **Request-scoped caches** — per-request memtable, bulk reclaim on completion

## Performance

Benchmarks on AMD Ryzen 5 3600 (6-core, 3.6GHz). All numbers from `cargo bench`.

### Concurrent writes (10K entries/thread, 64-byte keys)

| Threads | Throughput | Latency/insert |
|---------|------------|----------------|
| 4 | **8.4M ops/s** | 119ns |
| 8 | **7.8M ops/s** | 128ns |

### Single-threaded latency

| Operation | fastskip | BTreeMap | HashMap |
|-----------|----------|----------|---------|
| insert (random) | 221ns | 90ns | 21ns |
| get hit | 54ns | 33ns | 17ns |
| get miss | 24ns | 78ns | 16ns |
| cursor seek | 51ns | 551ns | — |

fastskip trades single-threaded speed for lock-free concurrent writes — BTreeMap and HashMap require external locking for multi-threaded access, which destroys throughput.

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

// With auto-seal thresholds
let sl = ConcurrentSkipList::with_capacity_and_shards(
    1024 * 1024,    // 1MB initial arena per shard
    8,              // 8 shards
    100 * 1024 * 1024, // Auto-seal at 100MB total memory
    10_000_000      // Auto-seal at 10M entries
);
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
| Auto-seal | Size or count based thresholds |
| Backpressure | 90% threshold warning |
| Batch ops | insert_batch, get_many |

## Testing

```bash
cargo test              # Unit, concurrent, stress, correctness tests
cargo clippy            # Zero warnings
cargo run --example basic
cargo run --example lsm_memtable
```

## Documentation

See [USAGE.md](USAGE.md) for complete API reference and examples.
