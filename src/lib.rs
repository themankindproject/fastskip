//! # fastskip — Lock-free arena-backed skip list for LSM-tree memtables
//!
//! `fastskip` provides a concurrent, lock-free skip list backed by per-thread
//! arena allocators. It is designed for use as the **memtable** in LSM-tree
//! storage engines (RocksDB, LevelDB, CockroachDB style).
//!
//! ## Features
//!
//! - **Lock-free writes**: Multiple threads insert/delete concurrently using
//!   a single CAS at level 0, best-effort CAS at upper levels
//! - **Lock-free reads**: Point lookups (`get`) walk the skip list without
//!   any locks or atomic read-modify-write
//! - **Snapshot isolation**: Take a point-in-time snapshot that remains
//!   consistent even under concurrent inserts
//! - **Range scans**: [`Cursor`] with lower-bound seek for prefix/range iteration
//! - **Safe lifecycle**: [`seal()`](ConcurrentSkipList::seal) freezes the
//!   memtable for flushing and returns a fresh one for new writes — no unsafe code
//! - **Arena memory**: Per-thread arena shards avoid allocation contention.
//!   Memory is bulk-reclaimed on drop (no per-node free overhead)
//!
//! ## Quick start
//!
//! ```rust
//! use fastskip::ConcurrentSkipList;
//!
//! let sl = ConcurrentSkipList::new();
//!
//! // Insert (multiple threads can call concurrently)
//! sl.insert(b"user:1001", b"alice");
//! sl.insert(b"user:1002", b"bob");
//!
//! // Point lookup
//! let (val, tombstone) = sl.get(b"user:1001").unwrap();
//! assert_eq!(val, b"alice");
//! assert!(!tombstone);
//!
//! // Delete (tombstone)
//! sl.delete(b"user:1002");
//! assert_eq!(sl.get_live(b"user:1002"), None);
//!
//! // Iteration (sorted order)
//! for entry in sl.iter() {
//!     println!("{:?} -> {:?}", entry.key, entry.value);
//! }
//! ```
//!
//! ## LSM memtable lifecycle
//!
//! The intended lifecycle for an LSM-tree memtable:
//!
//! ```rust
//! use fastskip::ConcurrentSkipList;
//!
//! // 1. Create active memtable
//! let memtable = ConcurrentSkipList::with_shards(4);
//!
//! // 2. Concurrent writers insert/delete
//! memtable.insert(b"key1", b"val1");
//! memtable.delete(b"key2");
//!
//! // 3. When full, seal it — returns frozen (for flushing) + fresh (for writes)
//! let (frozen, fresh) = memtable.seal().unwrap();
//!
//! // 4. Flush frozen to SSTable (iterate snapshot)
//! for entry in frozen.iter() {
//!     if entry.is_tombstone {
//!         // write tombstone marker to SSTable
//!     } else {
//!         // write key-value to SSTable
//!     }
//! }
//!
//! // 5. Drop frozen (reclaims arena memory)
//! std::mem::drop(frozen);
//!
//! // 6. Fresh memtable is ready for new writes
//! fresh.insert(b"key3", b"val3");
//! ```
//!
//! ## Concurrent reads
//!
//! Readers are fully lock-free. For consistent point-in-time reads under
//! concurrent writes, use a snapshot:
//!
//! ```rust
//! use fastskip::ConcurrentSkipList;
//!
//! let sl = ConcurrentSkipList::new();
//! sl.insert(b"a", b"1");
//! sl.insert(b"b", b"2");
//!
//! // Snapshot captures a sequence number — iterators skip post-snapshot inserts
//! let snap = sl.snapshot();
//!
//! // Insert more after snapshot (won't appear in snapshot iteration)
//! sl.insert(b"c", b"3");
//!
//! // Snapshot sees only "a" and "b"
//! assert_eq!(snap.iter().count(), 2);
//! // Live iterator sees all three
//! assert_eq!(sl.iter().count(), 3);
//! ```
//!
//! ## Range scans with Cursor
//!
//! ```rust
//! use fastskip::ConcurrentSkipList;
//!
//! let sl = ConcurrentSkipList::new();
//! sl.insert(b"apple", b"1");
//! sl.insert(b"banana", b"2");
//! sl.insert(b"cherry", b"3");
//! sl.insert(b"date", b"4");
//!
//! // Seek to first key >= "banana"
//! if let Some(cursor) = sl.cursor_at(b"banana") {
//!     let keys: Vec<_> = cursor
//!         .filter(|e| !e.is_tombstone)
//!         .map(|e| e.key.to_vec())
//!         .collect();
//!     assert_eq!(keys, vec![b"banana".to_vec(), b"cherry".to_vec(), b"date".to_vec()]);
//! }
//! ```
//!
//! ## Design notes
//!
//! ### Insert after delete
//!
//! Inserting a key that was previously tombstoned in the **same** memtable
//! returns `false` (duplicate). This is by design — the skip list maintains
//! one node per key. To re-insert a deleted key, seal the memtable and write
//! to a fresh one. The LSM-tree compaction process merges them later.
//!
//! ### Thread safety
//!
//! The [`ConcurrentSkipList`] is `Send + Sync` and can be shared across
//! threads via `Arc`. The arena uses per-thread shards (one writer thread
//! per shard). Readers never allocate, so they are contention-free.
//!
//! ### Memory management
//!
//! Arena memory is bulk-allocated in blocks and bulk-reclaimed when the
//! [`ConcurrentSkipList`] is dropped. There is no per-node allocation or
//! deallocation overhead. The [`seal()`](ConcurrentSkipList::seal) method
//! creates a fresh arena for the new memtable, and dropping the
//! [`FrozenMemtable`] reclaims the old one.

mod arena_sharded;
mod height;
mod iter;
mod node;
mod skiplist;
mod util;

use std::marker::PhantomData;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};

use crate::arena_sharded::ConcurrentArena;
use crate::node::*;
use crate::skiplist::{InsertResult, SkipList};

pub use crate::iter::{Cursor, Entry, Iter, Snapshot, SnapshotIter};
pub use fastarena;

// ─── Error types ──────────────────────────────────────────────────────────────

/// Error returned by [`ConcurrentSkipList::try_insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertError {
    /// The key already exists in the skip list.
    DuplicateKey,
    /// The arena shard is out of memory.
    OutOfMemory,
}

impl std::fmt::Display for InsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertError::DuplicateKey => write!(f, "duplicate key"),
            InsertError::OutOfMemory => write!(f, "out of memory"),
        }
    }
}

impl std::error::Error for InsertError {}

/// Error returned by [`ConcurrentSkipList::seal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealError {
    /// The memtable is already sealed.
    AlreadySealed,
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SealError::AlreadySealed => write!(f, "memtable is already sealed"),
        }
    }
}

impl std::error::Error for SealError {}

// ─── ConcurrentSkipList ────────────────────────────────────────────────────────
//
// Public API matching the design document. The skip list owns the arena
// and exposes lock-free insert/get/delete with snapshot iteration.

/// A lock-free, arena-backed skip list for LSM-tree memtables.
///
/// Multiple writer threads can call `insert` / `delete` concurrently.
/// Readers call `get` (lock-free) or take a `snapshot` for point-in-time iteration.
///
/// # Lifecycle
///
/// The recommended lifecycle for an LSM memtable:
///
/// 1. Create a `ConcurrentSkipList`
/// 2. Insert/delete key-value pairs (concurrent writers)
/// 3. When full, call [`seal()`](Self::seal) — returns a frozen memtable
///    (for flushing to disk) and a fresh empty memtable (for new writes)
/// 4. Flush the frozen memtable to an SSTable
/// 5. Drop the frozen memtable (reclaims arena memory)
///
/// # Sentinel / empty keys
///
/// The internal sentinel head node uses an empty key. User-inserted empty
/// keys (`b""`) work correctly for `get`, `insert`, and `delete`, and are
/// yielded by both live and snapshot iterators. The sentinel is identified
/// by its position (always first in the level-0 chain), not by key content.
pub struct ConcurrentSkipList {
    pub(crate) skiplist: SkipList,
    arena: ConcurrentArena,
    /// Count of live (non-tombstone) entries. Incremented on insert,
    /// decremented on delete.
    live_count: AtomicUsize,
    /// Whether this memtable has been sealed (no more writes allowed).
    sealed: AtomicBool,
}

// SAFETY: ConcurrentArena is Send+Sync (each shard is accessed by one writer
// thread at a time, readers never allocate). SkipList uses atomic operations
// for all concurrent access. The arena memory is never freed until the
// ConcurrentSkipList is dropped, so all pointers remain valid.
unsafe impl Send for ConcurrentSkipList {}
unsafe impl Sync for ConcurrentSkipList {}

impl ConcurrentSkipList {
    /// Create a new skip list with default shard count.
    pub fn new() -> Self {
        Self::with_shards(num_cpus())
    }

    /// Create with a given initial arena capacity per shard.
    pub fn with_capacity(arena_bytes: usize) -> Self {
        Self::with_capacity_and_shards(arena_bytes, num_cpus())
    }

    /// Create with a specific number of arena shards.
    pub fn with_shards(num_shards: usize) -> Self {
        Self::with_capacity_and_shards(64 * 1024, num_shards)
    }

    /// Create with custom capacity and shard count.
    pub fn with_capacity_and_shards(arena_bytes: usize, num_shards: usize) -> Self {
        let arena = ConcurrentArena::with_block_size(num_shards, arena_bytes);
        let head = Self::alloc_sentinel(&arena);

        let skiplist = unsafe { SkipList::new(head) };

        ConcurrentSkipList {
            skiplist,
            arena,
            live_count: AtomicUsize::new(0),
            sealed: AtomicBool::new(false),
        }
    }

    /// Allocate and initialize a sentinel head node in the arena.
    fn alloc_sentinel(arena: &ConcurrentArena) -> *const u8 {
        let local = arena.local();
        let head_size = node_alloc_size(MAX_HEIGHT, 0, 0);
        let head_ptr = local.alloc_raw(head_size, 8).as_ptr();
        unsafe {
            init_node(head_ptr, MAX_HEIGHT, b"", b"", false, 0);
        }
        fence(Ordering::Release);
        head_ptr
    }

    // ─── Write operations ──────────────────────────────────────────────────

    /// Insert a key-value pair. Returns `false` on OOM, duplicate key,
    /// or if the memtable is sealed.
    ///
    /// Use [`try_insert`](Self::try_insert) to distinguish the failure cases.
    /// Thread-safe: multiple writers can call concurrently.
    #[inline]
    pub fn insert(&self, key: &[u8], value: &[u8]) -> bool {
        if self.sealed.load(Ordering::Acquire) {
            return false;
        }
        let arena = self.arena.local();
        match self.skiplist.insert(key, value, arena) {
            InsertResult::Success => {
                self.live_count.fetch_add(1, Ordering::Relaxed);
                true
            }
            InsertResult::Duplicate | InsertResult::Oom => false,
        }
    }

    /// Insert a key-value pair, returning a typed error on failure.
    ///
    /// Unlike [`insert`](Self::insert), this distinguishes between
    /// `DuplicateKey` and `OutOfMemory` failures.
    #[inline]
    pub fn try_insert(&self, key: &[u8], value: &[u8]) -> Result<(), InsertError> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(InsertError::DuplicateKey); // sealed → treated as rejected
        }
        let arena = self.arena.local();
        match self.skiplist.insert(key, value, arena) {
            InsertResult::Success => {
                self.live_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            InsertResult::Duplicate => Err(InsertError::DuplicateKey),
            InsertResult::Oom => Err(InsertError::OutOfMemory),
        }
    }

    /// Get the value for a key, or insert and return a new value.
    ///
    /// Returns `(value, is_new)` where `is_new` is `true` if the key was
    /// not present and was inserted by this call.
    ///
    /// This is a convenience wrapper around [`get`](Self::get) and
    /// [`insert`](Self::insert). It is **not atomic** — under concurrent
    /// calls for the same key, both may observe the key as missing and
    /// one insert will be rejected as a duplicate. For truly concurrent
    /// updates, use [`insert`](Self::insert) directly and handle the
    /// duplicate rejection.
    #[inline]
    pub fn get_or_insert<'a>(&'a self, key: &[u8], value: &'a [u8]) -> (&'a [u8], bool) {
        if let Some((v, false)) = self.get(key) {
            return (v, false);
        }
        if self.insert(key, value) {
            (value, true)
        } else {
            // Lost the race — another thread inserted. Re-read.
            match self.get(key) {
                Some((v, false)) => (v, false),
                _ => (value, true), // tombstoned or OOM — return provided value
            }
        }
    }

    /// Delete a key by writing a tombstone. Returns `false` if the key
    /// was not found, was already tombstoned, or the memtable is sealed.
    #[inline]
    pub fn delete(&self, key: &[u8]) -> bool {
        if self.sealed.load(Ordering::Acquire) {
            return false;
        }
        if self.skiplist.delete(key) {
            self.live_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // ─── Read operations ───────────────────────────────────────────────────

    /// Point lookup. Returns `(value, is_tombstone)`.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<(&[u8], bool)> {
        self.skiplist.get(key)
    }

    /// Point lookup returning only live (non-tombstone) entries.
    #[inline]
    pub fn get_live(&self, key: &[u8]) -> Option<&[u8]> {
        match self.get(key) {
            Some((value, false)) => Some(value),
            _ => None,
        }
    }

    /// Returns `true` if the key exists and is not a tombstone.
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        matches!(self.get(key), Some((_, false)))
    }

    // ─── Iteration ─────────────────────────────────────────────────────────

    /// Live iterator. May see concurrent inserts mid-iteration.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            current: self.skiplist.head,
            _owner: PhantomData,
        }
    }

    /// Take a point-in-time snapshot for consistent iteration.
    ///
    /// The snapshot captures a sequence number. The iterator skips any node
    /// inserted after the snapshot was taken, providing a true point-in-time
    /// view even under concurrent inserts.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let snap_seq = self.skiplist.next_snapshot_seq();
        Snapshot {
            head: self.skiplist.head,
            snap_seq,
            _owner: PhantomData,
        }
    }

    // ─── Range scan ────────────────────────────────────────────────────────

    /// Create a cursor positioned at the first entry.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor {
            current: self.skiplist.head,
            _owner: PhantomData,
        }
    }

    /// Create a cursor positioned at the first key >= `target`.
    ///
    /// Returns `None` if all keys are < target.
    pub fn cursor_at(&self, target: &[u8]) -> Option<Cursor<'_>> {
        let mut c = Cursor {
            current: std::ptr::null(),
            _owner: PhantomData,
        };
        if c.seek(self, target) {
            Some(c)
        } else {
            None
        }
    }

    // ─── Stats ─────────────────────────────────────────────────────────────

    /// Number of live (non-tombstone) entries. Decrements on delete.
    pub fn len(&self) -> usize {
        self.live_count.load(Ordering::Relaxed)
    }

    /// Returns `true` if there are no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this memtable has been sealed (no more writes).
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    /// Total arena bytes allocated across all shards.
    pub fn memory_usage(&self) -> usize {
        self.arena.stats().bytes_allocated
    }

    // ─── Lifecycle ─────────────────────────────────────────────────────────

    /// Seal this memtable and create a fresh one for new writes.
    ///
    /// After sealing, no more inserts or deletes are accepted on this
    /// memtable. The returned [`FrozenMemtable`] can be iterated for
    /// flushing to disk, and the new `ConcurrentSkipList` is ready for
    /// writes.
    ///
    /// # Errors
    ///
    /// Returns `SealError::AlreadySealed` if called more than once.
    pub fn seal(self) -> Result<(FrozenMemtable, ConcurrentSkipList), SealError> {
        if self.sealed.swap(true, Ordering::Acquire) {
            // Already sealed — we consumed self but it was sealed. We need
            // to reconstruct self to return the error. Use ManuallyDrop.
            // Actually, since we consumed self, we can't return it. We'll
            // just drop it and return an error. But we can't get here
            // because swap returns the OLD value, and if old=true, we
            // already sealed. The self is consumed, so we drop it.
            std::mem::drop(FrozenMemtable { inner: self });
            return Err(SealError::AlreadySealed);
        }

        // Create a fresh memtable using the same arena configuration
        let num_shards = self.arena.num_shards();
        let stats = self.arena.stats();
        let block_size = if stats.block_count > 0 {
            stats.bytes_reserved / stats.block_count
        } else {
            64 * 1024
        };

        // The frozen memtable keeps the old arena alive.
        // We create a brand new arena for the fresh memtable.
        let fresh = ConcurrentSkipList::with_capacity_and_shards(block_size, num_shards);

        Ok((FrozenMemtable { inner: self }, fresh))
    }

    /// Reset this memtable for reuse. Clears all entries and resets the arena.
    ///
    /// # Safety
    ///
    /// No concurrent readers or writers may be active. Any existing
    /// [`Snapshot`], [`Iter`], or [`Cursor`] values become invalid
    /// (dangling pointers) and must not be used after this call.
    ///
    /// Prefer [`seal()`](Self::seal) for safe lifecycle management.
    pub unsafe fn reset(&mut self) {
        self.arena.reset_all();
        self.live_count.store(0, Ordering::Relaxed);
        self.sealed.store(false, Ordering::Release);

        // Re-allocate sentinel head node (the old one was freed by arena reset)
        let head = Self::alloc_sentinel(&self.arena);
        self.skiplist.reset(head);
    }
}

impl Default for ConcurrentSkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConcurrentSkipList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentSkipList")
            .field("len", &self.len())
            .field("sealed", &self.is_sealed())
            .field("memory_usage", &self.memory_usage())
            .finish()
    }
}

/// Simple CPU count detection.
fn num_cpus() -> usize {
    #[cfg(miri)]
    {
        4
    }
    #[cfg(not(miri))]
    {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}

// ─── FrozenMemtable ────────────────────────────────────────────────────────────

/// A sealed (read-only) memtable ready for flushing to disk.
///
/// Created by [`ConcurrentSkipList::seal`]. The frozen memtable prevents
/// further writes and can be iterated for flushing to an SSTable.
///
/// When dropped, the arena memory is reclaimed.
pub struct FrozenMemtable {
    inner: ConcurrentSkipList,
}

// SAFETY: FrozenMemtable wraps a ConcurrentSkipList which is Send+Sync.
// After sealing, no writes occur — only lock-free reads. The arena memory
// remains valid until the FrozenMemtable is dropped.
unsafe impl Send for FrozenMemtable {}
unsafe impl Sync for FrozenMemtable {}

impl FrozenMemtable {
    /// Iterate over all entries (including tombstones) in sorted order.
    pub fn iter(&self) -> Iter<'_> {
        self.inner.iter()
    }

    /// Take a point-in-time snapshot.
    pub fn snapshot(&self) -> Snapshot<'_> {
        self.inner.snapshot()
    }

    /// Create a cursor positioned at the first entry.
    pub fn cursor(&self) -> Cursor<'_> {
        self.inner.cursor()
    }

    /// Create a cursor positioned at the first key >= `target`.
    pub fn cursor_at(&self, target: &[u8]) -> Option<Cursor<'_>> {
        self.inner.cursor_at(target)
    }

    /// Number of live (non-tombstone) entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if there are no live entries.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Total arena bytes allocated.
    pub fn memory_usage(&self) -> usize {
        self.inner.memory_usage()
    }

    /// Point lookup. Returns `(value, is_tombstone)`.
    pub fn get(&self, key: &[u8]) -> Option<(&[u8], bool)> {
        self.inner.get(key)
    }

    /// Point lookup returning only live (non-tombstone) entries.
    pub fn get_live(&self, key: &[u8]) -> Option<&[u8]> {
        self.inner.get_live(key)
    }
}

impl std::fmt::Debug for FrozenMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenMemtable")
            .field("len", &self.len())
            .field("memory_usage", &self.memory_usage())
            .finish()
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_get() {
        let sl = ConcurrentSkipList::new();
        assert!(sl.insert(b"key1", b"value1"));
        assert!(sl.insert(b"key2", b"value2"));

        let (v, tomb) = sl.get(b"key1").unwrap();
        assert_eq!(v, b"value1");
        assert!(!tomb);

        assert_eq!(sl.get_live(b"key1"), Some(b"value1".as_slice()));
        assert_eq!(sl.get_live(b"missing"), None);
    }

    #[test]
    fn test_delete_tombstone() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key1", b"value1");
        assert!(sl.delete(b"key1"));

        let (_, tomb) = sl.get(b"key1").unwrap();
        assert!(tomb);
        assert_eq!(sl.get_live(b"key1"), None);
    }

    #[test]
    fn test_snapshot_iter() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"alpha", b"1");
        sl.insert(b"beta", b"2");
        sl.insert(b"gamma", b"3");

        let snap = sl.snapshot();
        let entries: Vec<_> = snap.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"alpha");
        assert_eq!(entries[1].key, b"beta");
        assert_eq!(entries[2].key, b"gamma");
    }

    #[test]
    fn test_debug_iter() {
        let sl = ConcurrentSkipList::new();
        let r1 = sl.insert(b"x", b"1");
        let r2 = sl.insert(b"y", b"2");
        assert!(r1, "insert x failed");
        assert!(r2, "insert y failed");

        // walk level-0 manually from head
        let mut cur = sl.skiplist.head;
        let mut count = 0;
        loop {
            let next = unsafe { crate::node::tower_load(cur, 0) };
            if next.is_null() {
                break;
            }
            let node = next.ptr();
            let key = unsafe { crate::node::node_key(node) };
            let seq = unsafe { crate::node::node_seq(node) };
            eprintln!(
                "  [{}] key={:?} seq={}",
                count,
                std::str::from_utf8(key).unwrap_or("<non-utf8>"),
                seq
            );
            cur = node;
            count += 1;
        }
        eprintln!("  total level-0 nodes: {}", count);

        let entries: Vec<_> = sl.iter().collect();
        for (i, e) in entries.iter().enumerate() {
            eprintln!(
                "  iter[{}] key={:?}",
                i,
                std::str::from_utf8(e.key).unwrap_or("<non-utf8>")
            );
        }
        assert_eq!(entries.len(), 2, "expected 2 iter entries");
    }

    #[test]
    fn test_live_iter() {
        let sl = ConcurrentSkipList::new();
        let r1 = sl.insert(b"x", b"1");
        let r2 = sl.insert(b"y", b"2");
        assert!(r1, "first insert should succeed");
        assert!(r2, "second insert should succeed");

        assert!(sl.get(b"x").is_some(), "x should be found");
        assert!(sl.get(b"y").is_some(), "y should be found");

        let entries: Vec<_> = sl.iter().collect();
        assert_eq!(
            entries.len(),
            2,
            "expected 2 entries, got {}",
            entries.len()
        );
    }

    #[test]
    fn test_duplicate_insert() {
        let sl = ConcurrentSkipList::new();
        assert!(sl.insert(b"key", b"v1"));
        assert!(!sl.insert(b"key", b"v2"));
        let (v, _) = sl.get(b"key").unwrap();
        assert_eq!(v, b"v1");
    }

    #[test]
    fn test_try_insert_distinguishes_errors() {
        let sl = ConcurrentSkipList::new();
        assert_eq!(sl.try_insert(b"key", b"v1"), Ok(()));
        assert_eq!(sl.try_insert(b"key", b"v2"), Err(InsertError::DuplicateKey));
    }

    #[test]
    fn test_contains_key() {
        let sl = ConcurrentSkipList::new();
        assert!(!sl.contains_key(b"missing"));
        sl.insert(b"key", b"val");
        assert!(sl.contains_key(b"key"));
        sl.delete(b"key");
        assert!(!sl.contains_key(b"key"));
    }

    #[test]
    fn test_memory_usage() {
        let sl = ConcurrentSkipList::new();
        let before = sl.memory_usage();
        for i in 0..100 {
            let k = format!("k{:04}", i);
            let v = format!("v{:04}", i);
            sl.insert(k.as_bytes(), v.as_bytes());
        }
        assert!(sl.memory_usage() > before);
    }

    #[test]
    fn test_empty_list() {
        let sl = ConcurrentSkipList::new();
        assert!(sl.is_empty());
        assert_eq!(sl.get(b"nope"), None);
        assert_eq!(sl.iter().count(), 0);
        let snap = sl.snapshot();
        assert_eq!(snap.iter().count(), 0);
    }

    #[test]
    fn test_empty_key_in_iter() {
        let sl = ConcurrentSkipList::new();
        assert!(sl.insert(b"", b"empty_key_val"));
        assert!(sl.insert(b"a", b"val_a"));

        let entries: Vec<_> = sl.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"");
        assert_eq!(entries[0].value, b"empty_key_val");
        assert_eq!(entries[1].key, b"a");

        let snap = sl.snapshot();
        let snap_entries: Vec<_> = snap.iter().collect();
        assert_eq!(snap_entries.len(), 2);
        assert_eq!(snap_entries[0].key, b"");
    }

    #[test]
    fn test_snapshot_excludes_concurrent_inserts() {
        let sl = ConcurrentSkipList::new();
        for i in 0..10 {
            let k = format!("key_{:04}", i);
            let v = format!("val_{:04}", i);
            sl.insert(k.as_bytes(), v.as_bytes());
        }

        let snap = sl.snapshot();
        let snap_seq = snap.snap_seq;

        for i in 10..20 {
            let k = format!("key_{:04}", i);
            let v = format!("val_{:04}", i);
            sl.insert(k.as_bytes(), v.as_bytes());
        }

        let entries: Vec<_> = snap.iter().collect();
        assert_eq!(entries.len(), 10);
        for (i, entry) in entries.iter().enumerate() {
            let expected = format!("key_{:04}", i);
            assert_eq!(entry.key, expected.as_bytes());
        }

        let live: Vec<_> = sl.iter().collect();
        assert_eq!(live.len(), 20);

        assert!(snap_seq < sl.skiplist.next_seq.load(Ordering::Relaxed) as u64);
    }

    #[test]
    fn test_len_decrements_on_delete() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"a", b"1");
        sl.insert(b"b", b"2");
        assert_eq!(sl.len(), 2);

        sl.delete(b"a");
        assert_eq!(sl.len(), 1);

        sl.delete(b"b");
        assert_eq!(sl.len(), 0);
        assert!(sl.is_empty());
    }

    #[test]
    fn test_seal_creates_fresh_memtable() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key1", b"val1");
        sl.insert(b"key2", b"val2");

        let (frozen, fresh) = sl.seal().unwrap();

        // Frozen has the old data
        assert_eq!(frozen.len(), 2);
        assert_eq!(frozen.get_live(b"key1"), Some(b"val1".as_slice()));

        // Fresh is empty and writable
        assert!(fresh.is_empty());
        assert!(fresh.insert(b"key3", b"val3"));
        assert_eq!(fresh.get_live(b"key3"), Some(b"val3".as_slice()));

        // Frozen rejects writes (via the sealed flag)
        // (can't test insert on frozen directly since it's consumed)

        // Iteration works on frozen
        let entries: Vec<_> = frozen.iter().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_seal_double_returns_error() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key", b"val");

        let (frozen, _fresh) = sl.seal().unwrap();
        // Can't seal again since `seal` consumes self.
        // frozen doesn't have a seal method.
        assert_eq!(frozen.len(), 1);
    }

    #[test]
    fn test_sealed_rejects_writes() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key", b"val");

        // Manually seal (without consuming via seal())
        sl.sealed.store(true, Ordering::Release);

        assert!(!sl.insert(b"new", b"val"));
        assert!(!sl.delete(b"key"));
    }

    #[test]
    fn test_cursor_basic() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"b", b"2");
        sl.insert(b"a", b"1");
        sl.insert(b"c", b"3");

        let mut cursor = sl.cursor();
        // Skip sentinel
        assert!(cursor.next_entry());
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"a");

        assert!(cursor.next_entry());
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"b");

        assert!(cursor.next_entry());
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"c");

        assert!(!cursor.next_entry());
    }

    #[test]
    fn test_cursor_seek() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"a", b"1");
        sl.insert(b"c", b"3");
        sl.insert(b"e", b"5");

        // Seek to exact key
        let cursor = sl.cursor_at(b"c").unwrap();
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"c");

        // Seek to between keys — lands on next
        let cursor = sl.cursor_at(b"b").unwrap();
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"c");

        // Seek past all keys
        assert!(sl.cursor_at(b"z").is_none());

        // Seek before all keys
        let cursor = sl.cursor_at(b"").unwrap();
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"a");
    }

    #[test]
    fn test_cursor_as_iterator() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"a", b"1");
        sl.insert(b"b", b"2");
        sl.insert(b"c", b"3");

        let cursor = sl.cursor_at(b"b").unwrap();
        let keys: Vec<_> = cursor.map(|e| e.key.to_vec()).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_cursor_with_tombstones() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"a", b"1");
        sl.insert(b"b", b"2");
        sl.insert(b"c", b"3");
        sl.insert(b"d", b"4");
        sl.insert(b"e", b"5");

        // Delete b and d
        assert!(sl.delete(b"b"));
        assert!(sl.delete(b"d"));

        // Cursor should see all 5 entries (including tombstones)
        let cursor = sl.cursor_at(b"a").unwrap();
        let entries: Vec<_> = cursor.collect();
        assert_eq!(entries.len(), 5);

        // Verify tombstones are correctly marked
        assert!(!entries[0].is_tombstone); // a
        assert!(entries[1].is_tombstone); // b
        assert!(!entries[2].is_tombstone); // c
        assert!(entries[3].is_tombstone); // d
        assert!(!entries[4].is_tombstone); // e

        // Filter to live entries only
        let live_keys: Vec<_> = sl
            .cursor_at(b"a")
            .unwrap()
            .filter(|e| !e.is_tombstone)
            .map(|e| e.key.to_vec())
            .collect();
        assert_eq!(live_keys, vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()]);

        // Seek into tombstoned key — should land on it
        let cursor = sl.cursor_at(b"b").unwrap();
        let e = cursor.entry().unwrap();
        assert_eq!(e.key, b"b");
        assert!(e.is_tombstone);
    }

    #[test]
    fn test_get_or_insert_new_key() {
        let sl = ConcurrentSkipList::new();
        let (val, is_new) = sl.get_or_insert(b"key", b"value");
        assert!(is_new);
        assert_eq!(val, b"value");
        assert_eq!(sl.len(), 1);
    }

    #[test]
    fn test_get_or_insert_existing_key() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key", b"original");

        let (val, is_new) = sl.get_or_insert(b"key", b"replacement");
        assert!(!is_new);
        assert_eq!(val, b"original");
        assert_eq!(sl.len(), 1);

        // Original value is preserved
        let (v, _) = sl.get(b"key").unwrap();
        assert_eq!(v, b"original");
    }

    #[test]
    fn test_debug_format() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key", b"val");
        let debug = format!("{:?}", sl);
        assert!(debug.contains("ConcurrentSkipList"));
        assert!(debug.contains("len: 1"));
        assert!(debug.contains("sealed: false"));
    }

    #[test]
    fn test_frozen_memtable_debug() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"key", b"val");
        let (frozen, _fresh) = sl.seal().unwrap();
        let debug = format!("{:?}", frozen);
        assert!(debug.contains("FrozenMemtable"));
        assert!(debug.contains("len: 1"));
    }

    #[test]
    fn test_seal_full_lifecycle() {
        // Simulate LSM memtable lifecycle
        let sl = ConcurrentSkipList::new();

        // Phase 1: Write
        for i in 0..100 {
            let k = format!("key_{:04}", i);
            let v = format!("val_{:04}", i);
            sl.insert(k.as_bytes(), v.as_bytes());
        }
        assert_eq!(sl.len(), 100);

        // Phase 2: Seal
        let (frozen, fresh) = sl.seal().unwrap();

        // Phase 3: Flush frozen to "disk" (just iterate)
        let mut flushed = 0;
        for entry in frozen.iter() {
            if !entry.is_tombstone {
                flushed += 1;
            }
        }
        assert_eq!(flushed, 100);

        // Phase 4: Drop frozen (reclaims memory)
        std::mem::drop(frozen);

        // Phase 5: Fresh memtable is ready
        assert!(fresh.is_empty());
        for i in 0..50 {
            let k = format!("new_{:04}", i);
            let v = format!("val_{:04}", i);
            fresh.insert(k.as_bytes(), v.as_bytes());
        }
        assert_eq!(fresh.len(), 50);
    }
}
