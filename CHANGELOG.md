# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-03-31

### Performance

- Up to **27% faster** concurrent inserts (4 threads) and **20% faster** single-threaded sequential inserts
- O(1) `memory_usage()` via atomic running total (was O(N shards))
- Adaptive backoff (`core::hint::spin_loop()`) on CAS failure to reduce cache-line bouncing
- `insert_batch()` optimized: single sealed check, single arena lookup, no per-insert overhead
- `init_node()` uses bulk u64 header writes (3 stores) instead of 8 individual stores
- `compare_keys()` uses `unwrap_unchecked()` behind length guards to eliminate bounds checks
- Conditional `record_alloc` — atomic tracking only when memory limits are configured
- Removed unnecessary prefetch from `get`, `delete`, `seek` paths (extra tower_load hurt cache-resident workloads)
- Kept lookahead prefetch in `find_less` (beneficial for insert-heavy workloads with larger datasets)
- `#[inline(always)]` on all hot-path functions
- Release profile: `lto = "fat"`, `strip = true` for maximum optimization

### Changed

- `insert_inner()` returns `(InsertResult, usize)` — exact allocation size tracked per insert
- `ConcurrentArena`: shard cache `Vec` → `HashMap` → 8-entry inline array with `Drop` cleanup
- `TowerPtr::is_null`: skip pointer mask, check raw value directly
- `#[inline]` on all `Iterator::next` impls (`Iter`, `SnapshotIter`, `Cursor`)
- `#[cold]` on `should_seal` (cold path hint for branch prediction)
- `compare_keys`: use `from_be_bytes` instead of `from_ne_bytes` + `swap_bytes`

### Added

- `# Example` code blocks on every public method and type (50 doc tests)
- `CHANGELOG.md`

### Removed

- `pub use fastarena;` re-export (was leaking internal dependency into public API)

## [0.1.0] - 2026-03-26

### Added

- `ConcurrentSkipList` — lock-free, arena-backed skip list for LSM-tree memtables
- Lock-free `insert`, `delete`, `get` with single CAS at level 0, best-effort CAS at upper levels
- `try_insert` with typed `InsertError` (`DuplicateKey` | `OutOfMemory`)
- Tombstone-based deletion with atomic CAS on the flags byte
- `get_live` and `contains_key` convenience read methods
- `get_or_insert` — get-or-compute without external locking
- `insert_batch` and `get_many` for bulk operations
- `snapshot` — point-in-time snapshot isolation using monotonic sequence numbers
- `SnapshotIter` — iterator that skips post-snapshot inserts
- `Iter` — live iterator reflecting current state
- `Cursor` — seekable forward cursor with lower-bound `seek` for range scans
- `seal` — freeze memtable for flushing, returns `FrozenMemtable` + fresh `ConcurrentSkipList`
- `reset` (unsafe) — reuse memtable by clearing arena and skip list
- Per-thread arena shard pool (`ConcurrentArena`) with atomic round-robin assignment
- Auto-sealing on configurable memory (`max_memory_bytes`) and entry count (`max_entries`) limits
- `is_under_backpressure` — 90% threshold warning for capacity planning
- Memory stats: `memory_usage`, `memory_reserved`, `memory_utilization`, `memory_idle`
- `avg_key_size`, `avg_value_size`, `total_inserts` metrics
- Masstree-style lookahead prefetching for cache efficiency on `get`, `delete`, and `find_less`
- Optimized key comparison with 8-byte/4-byte prefix fast paths
- CPU cache prefetch hints (`PREFETCHT0` on x86, `PRFM PLDL1KEEP` on aarch64)
- Thread-local splitmix64 PRNG for random height generation
- Loom-based concurrency verification tests (`cfg(loom)`)
- Criterion benchmarks comparing against `BTreeMap` and `HashMap`
- Examples: `basic`, `lsm_memtable`, `snapshot`, `concurrent_writers`
- Comprehensive docstrings on all public and internal APIs
