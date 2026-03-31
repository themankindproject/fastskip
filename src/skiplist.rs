//! Core lock-free skip list implementation.
//!
//! This module implements the skip list data structure with lock-free
//! concurrent insertion, deletion, and point lookups. The skip list
//! uses a single CAS at level 0 to commit inserts atomically, with
//! best-effort CAS at upper levels for search optimization.
//!
//! The algorithm follows the design of RocksDB's `InlineSkipList`
//! and CockroachDB's `arenaskl`: nodes are allocated from an arena,
//! published via a Release CAS at level 0, and upper-level pointers
//! are repaired lazily.

use std::sync::atomic::fence;
use std::sync::atomic::{AtomicUsize, Ordering};

use fastarena::Arena;

use crate::height::random_height;
use crate::node::*;
use crate::util::{compare_keys, prefetch_read};

/// Lock-free skip list backed by an arena allocator.
///
/// All concurrent access goes through atomic operations on the node
/// tower pointers. The sentinel head node has key `b""` and height
/// [`MAX_HEIGHT`], ensuring all levels have a starting point for
/// traversal.
pub(crate) struct SkipList {
    /// Sentinel head node (key = empty, height = MAX_HEIGHT).
    /// Always the first node in the level-0 chain.
    pub(crate) head: *const u8,
    /// Current maximum tower height used by any node in the list.
    /// Updated via CAS when a node with higher height is inserted.
    pub(crate) height: AtomicUsize,
    /// Monotonic insertion sequence counter for snapshot visibility.
    /// The sentinel uses seq 0; the first real node gets seq 1, etc.
    /// Incremented atomically on every insert attempt.
    pub(crate) next_seq: AtomicUsize,
}

/// Result of an insert attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertResult {
    /// The key-value pair was successfully inserted.
    Success,
    /// A node with the same key already exists (duplicate rejection).
    Duplicate,
    /// The arena shard has insufficient memory for the new node.
    Oom,
}

impl SkipList {
    /// Create a new skip list with a sentinel head node.
    ///
    /// # Safety
    ///
    /// `head` must point to a valid sentinel node allocated from the arena
    /// with height [`MAX_HEIGHT`] and sequence number 0. The sentinel must
    /// remain valid for the lifetime of this `SkipList`.
    pub(crate) unsafe fn new(head: *const u8) -> Self {
        SkipList {
            head,
            height: AtomicUsize::new(1),
            next_seq: AtomicUsize::new(1), // sentinel uses seq 0
        }
    }

    // ─── Get (lock-free read) ──────────────────────────────────────────────

    /// Lock-free point lookup. Returns `(value, is_tombstone)` if the key exists.
    ///
    /// Walks the skip list from the highest level down. At each level, advances
    /// along the tower chain until the successor key is `>= target`, then drops
    /// to the next level. At level 0, if the successor key matches exactly,
    /// returns the node's value and tombstone flag.
    ///
    /// All tower loads use Acquire ordering to synchronize with the Release
    /// CAS that published each node.
    ///
    /// Uses Masstree-style lookahead prefetching: while comparing one node,
    /// the successor's successor is prefetched into L1 cache. Most beneficial
    /// when the memtable exceeds L3 cache (>1 MB); on cache-resident data the
    /// overhead is neutral.
    #[inline]
    pub(crate) fn get(&self, key: &[u8]) -> Option<(&[u8], bool)> {
        let mut x = self.head;
        let h = self.height.load(Ordering::Acquire);
        let mut level = if h > 0 { h - 1 } else { 0 };

        prefetch_read(x);

        loop {
            let next = unsafe { tower_load(x, level) };
            if next.is_null() {
                if level == 0 {
                    return None;
                }
                level -= 1;
                continue;
            }
            let next_node = next.ptr();
            let next_next = unsafe { tower_load(next_node, level) };
            if !next_next.is_null() {
                prefetch_read(next_next.ptr());
            }
            let next_key = unsafe { node_key(next_node) };
            match compare_keys(next_key, key) {
                std::cmp::Ordering::Less => {
                    x = next_node;
                }
                std::cmp::Ordering::Equal => {
                    if level == 0 {
                        let val = unsafe { node_value(next_node) };
                        let tomb = unsafe { is_tombstone(next_node) };
                        return Some((val, tomb));
                    }
                    level -= 1;
                }
                std::cmp::Ordering::Greater => {
                    if level == 0 {
                        return None;
                    }
                    level -= 1;
                }
            }
        }
    }

    // ─── Insert (lock-free write) ──────────────────────────────────────────

    /// Insert a key-value pair into the skip list.
    ///
    /// Returns [`InsertResult::Success`] on successful insertion,
    /// [`InsertResult::Duplicate`] if the key already exists, or
    /// [`InsertResult::Oom`] if the arena shard is out of memory.
    ///
    /// Delegates to [`insert_inner`](Self::insert_inner) with `is_tombstone = false`.
    #[inline]
    pub(crate) fn insert(
        &self,
        key: &[u8],
        value: &[u8],
        arena: &mut Arena,
    ) -> (InsertResult, usize) {
        self.insert_inner(key, value, arena, false)
    }

    /// Internal insert with tombstone support.
    ///
    /// Algorithm (following RocksDB InlineSkipList / CockroachDB arenaskl):
    /// 1. find_less → preds/succs at all levels
    /// 2. Check duplicate at level 0
    /// 3. Allocate node, initialize key/value/tower
    /// 4. Release fence → CAS at level 0 (THE atomic commit)
    /// 5. On success: best-effort splice at upper levels (simplified for speed)
    /// 6. On failure: retry from step 1 (arena allocation wasted — OK, it's an arena)
    ///
    /// Returns `(InsertResult, allocated_size)` — the size is 0 on failure.
    pub(crate) fn insert_inner(
        &self,
        key: &[u8],
        value: &[u8],
        arena: &mut Arena,
        is_tombstone: bool,
    ) -> (InsertResult, usize) {
        let h = random_height();

        loop {
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed) as u64;
            let (preds, succs) = self.find_less(key);

            // Check for duplicate at level 0
            let succ0 = succs[0];
            if !succ0.is_null() {
                let succ_key = unsafe { node_key(succ0.ptr()) };
                if compare_keys(succ_key, key) == std::cmp::Ordering::Equal {
                    return (InsertResult::Duplicate, 0);
                }
            }

            // Allocate node
            let size = node_alloc_size(h, key.len(), value.len());
            let node_ptr = match arena.try_alloc_raw(size, 8) {
                Some(ptr) => ptr.as_ptr(),
                None => return (InsertResult::Oom, 0),
            };

            // Initialize node (not yet visible — plain stores for header/kv,
            // relaxed atomic stores for tower)
            unsafe {
                init_node(node_ptr, h, key, value, is_tombstone, seq);
                for (i, succ) in succs.iter().enumerate().take(h) {
                    tower_store(node_ptr, i, *succ);
                }
            }

            // Release fence: all node contents visible before CAS publishes
            fence(Ordering::Release);

            // CAS at level 0 — THE atomic commit
            let pred0 = preds[0];
            let new_ptr = TowerPtr::new(node_ptr);

            let cas_result = unsafe { tower_cas(pred0.ptr(), 0, succ0, new_ptr) };

            match cas_result {
                Ok(_) => {
                    for (i, pred) in preds.iter().enumerate().take(h).skip(1) {
                        if pred.is_null() {
                            continue;
                        }
                        let mut retries = 0u8;
                        loop {
                            let curr_succ = unsafe { tower_load(pred.ptr(), i) };
                            unsafe { tower_store(node_ptr, i, curr_succ) };
                            let cas = unsafe { tower_cas(pred.ptr(), i, curr_succ, new_ptr) };
                            if cas.is_ok() {
                                break;
                            }
                            retries += 1;
                            if retries >= 2 {
                                break;
                            }
                        }
                    }

                    let mut old_h = self.height.load(Ordering::Relaxed);
                    while h > old_h {
                        match self.height.compare_exchange_weak(
                            old_h,
                            h,
                            Ordering::Release,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(current) => old_h = current,
                        }
                    }

                    return (InsertResult::Success, size);
                }
                Err(_) => {
                    // Adaptive backoff: spin a few times, then yield.
                    // Reduces cache-line bouncing under extreme contention.
                    core::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    // ─── Delete (tombstone) ────────────────────────────────────────────────

    /// Delete a key by atomically setting its tombstone flag.
    ///
    /// Returns `true` if the key was found and tombstoned by this call.
    /// Returns `false` if the key was not found or was already tombstoned.
    ///
    /// The tombstone is set via a CAS on the node's flags byte, making
    /// the operation idempotent — calling delete twice on the same key
    /// returns `false` the second time.
    ///
    /// Under concurrent insert/delete, a delete may return `false` while
    /// a concurrent insert of the same key succeeds. This is linearizable:
    /// the operations can be ordered as delete-before-insert.
    #[inline]
    pub(crate) fn delete(&self, key: &[u8]) -> bool {
        let mut x = self.head;
        let h = self.height.load(Ordering::Acquire);
        let mut level = if h > 0 { h - 1 } else { 0 };

        prefetch_read(x);

        loop {
            let next = unsafe { tower_load(x, level) };
            if next.is_null() {
                if level == 0 {
                    return false;
                }
                level -= 1;
                continue;
            }
            let next_node = next.ptr();
            let next_next = unsafe { tower_load(next_node, level) };
            if !next_next.is_null() {
                prefetch_read(next_next.ptr());
            }
            let next_key = unsafe { node_key(next_node) };
            match compare_keys(next_key, key) {
                std::cmp::Ordering::Less => {
                    x = next_node;
                }
                std::cmp::Ordering::Equal => {
                    if level == 0 {
                        return unsafe { set_tombstone(next_node) };
                    }
                    level -= 1;
                }
                std::cmp::Ordering::Greater => {
                    if level == 0 {
                        return false;
                    }
                    level -= 1;
                }
            }
        }
    }

    // ─── find_less ─────────────────────────────────────────────────────────

    /// Find the predecessor and successor for `key` at each level.
    ///
    /// Returns two arrays of length [`MAX_HEIGHT`]:
    /// - `preds[i]` — the last node at level `i` whose key is `< key`
    /// - `succs[i]` — the first node at level `i` whose key is `>= key`
    ///
    /// Used by [`insert_inner`](Self::insert_inner) to locate the splice
    /// points for the new node at all levels simultaneously. Uses
    /// Masstree-style lookahead prefetching for cache efficiency.
    #[inline]
    pub(crate) fn find_less(&self, key: &[u8]) -> ([TowerPtr; MAX_HEIGHT], [TowerPtr; MAX_HEIGHT]) {
        let mut preds = [TowerPtr::NULL; MAX_HEIGHT];
        let mut succs = [TowerPtr::NULL; MAX_HEIGHT];

        let mut x = self.head;
        let h = self.height.load(Ordering::Acquire);
        let mut level = if h > 0 { h - 1 } else { 0 };

        // Prefetch head node
        prefetch_read(x);

        loop {
            let next = unsafe { tower_load(x, level) };
            if next.is_null() {
                preds[level] = TowerPtr::new(x);
                succs[level] = TowerPtr::NULL;
                if level == 0 {
                    break;
                }
                level -= 1;
                continue;
            }
            let next_node = next.ptr();
            // Lookahead prefetch: start loading next's successor while we compare
            let next_next = unsafe { tower_load(next_node, level) };
            if !next_next.is_null() {
                prefetch_read(next_next.ptr());
            }
            let next_key = unsafe { node_key(next_node) };
            if compare_keys(next_key, key) == std::cmp::Ordering::Less {
                x = next_node;
            } else {
                preds[level] = TowerPtr::new(x);
                succs[level] = next;
                if level == 0 {
                    break;
                }
                level -= 1;
            }
        }

        (preds, succs)
    }

    /// Capture the current insertion sequence number for snapshot isolation.
    ///
    /// Any node with `seq <` the returned value was fully inserted before
    /// this call and is visible to the snapshot. Nodes inserted concurrently
    /// after this call get a `seq >=` the returned value and are hidden.
    pub(crate) fn next_snapshot_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed) as u64
    }

    /// Reset the skip list with a new sentinel head node.
    ///
    /// Clears all state and replaces the sentinel. Used by
    /// [`ConcurrentSkipList::reset`](crate::ConcurrentSkipList::reset)
    /// after the arena is reclaimed.
    ///
    /// # Safety
    ///
    /// `head` must point to a valid sentinel node with height [`MAX_HEIGHT`]
    /// and sequence number 0. No concurrent readers or writers may be active.
    /// Any existing [`Snapshot`](crate::Snapshot), [`Iter`](crate::Iter), or
    /// [`Cursor`](crate::Cursor) values become invalid and must not be used.
    pub(crate) unsafe fn reset(&mut self, head: *const u8) {
        self.head = head;
        self.height.store(1, Ordering::Relaxed);
        self.next_seq.store(1, Ordering::Relaxed);
    }
}
