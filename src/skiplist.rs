use std::sync::atomic::fence;
use std::sync::atomic::{AtomicUsize, Ordering};

use fastarena::Arena;

use crate::height::random_height;
use crate::node::*;
use crate::util::{compare_keys, prefetch_read};

/// Lock-free skip list backed by an arena allocator.
/// Single CAS at level 0, best-effort CAS at upper levels.
pub(crate) struct SkipList {
    /// Sentinel head node (key = empty, height = MAX_HEIGHT).
    pub(crate) head: *const u8,
    /// Current maximum height used by any node.
    pub(crate) height: AtomicUsize,
    /// Monotonic insertion sequence counter for snapshot visibility.
    /// Sentinel = seq 0, first real node = seq 1, etc.
    pub(crate) next_seq: AtomicUsize,
}

/// Result of an insert attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertResult {
    Success,
    Duplicate,
    Oom,
}

impl SkipList {
    /// Create a new skip list with a sentinel head node.
    ///
    /// # Safety
    /// `head` must point to a valid sentinel node with height MAX_HEIGHT
    /// and sequence number 0.
    pub(crate) unsafe fn new(head: *const u8) -> Self {
        SkipList {
            head,
            height: AtomicUsize::new(1),
            next_seq: AtomicUsize::new(1), // sentinel uses seq 0
        }
    }

    // ─── Get (lock-free read) ──────────────────────────────────────────────

    /// Point lookup. Returns `(value, is_tombstone)` if the key exists.
    ///
    /// Walks from the highest level down. At level 0, if the successor matches
    /// the key, returns its value and tombstone flag.
    /// All tower loads use Acquire ordering.
    ///
    /// Uses Masstree-style lookahead prefetching: we prefetch the successor's
    /// successor while comparing the current node. Most beneficial when the
    /// memtable exceeds L3 cache (>1MB); on cache-resident data the overhead
    /// is neutral.
    #[inline]
    pub(crate) fn get(&self, key: &[u8]) -> Option<(&[u8], bool)> {
        let mut x = self.head;
        let h = self.height.load(Ordering::Acquire);
        let mut level = if h > 0 { h - 1 } else { 0 };

        // Prefetch the head node's tower
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
            // Prefetch next's successor into L1 while we compare.
            // If we advance, the data is ready. If we descend, wasted but harmless.
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

    /// Insert a key-value pair. Returns the result of the insertion.
    #[inline]
    pub(crate) fn insert(&self, key: &[u8], value: &[u8], arena: &mut Arena) -> InsertResult {
        self.insert_inner(key, value, arena, false)
    }

    /// Internal insert with tombstone support.
    ///
    /// Algorithm (following RocksDB InlineSkipList / CockroachDB arenaskl):
    /// 1. find_less → preds/succs at all levels
    /// 2. Check duplicate at level 0
    /// 3. Allocate node, initialize key/value/tower
    /// 4. Release fence → CAS at level 0 (THE atomic commit)
    /// 5. On success: best-effort splice at upper levels (infinite retry per level)
    /// 6. On failure: retry from step 1 (arena allocation wasted — OK, it's an arena)
    pub(crate) fn insert_inner(
        &self,
        key: &[u8],
        value: &[u8],
        arena: &mut Arena,
        is_tombstone: bool,
    ) -> InsertResult {
        let h = random_height();

        loop {
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed) as u64;
            let (preds, succs) = self.find_less(key);

            // Check for duplicate at level 0
            let succ0 = succs[0];
            if !succ0.is_null() {
                let succ_key = unsafe { node_key(succ0.ptr()) };
                if compare_keys(succ_key, key) == std::cmp::Ordering::Equal {
                    return InsertResult::Duplicate;
                }
            }

            // Allocate node
            let size = node_alloc_size(h, key.len(), value.len());
            let node_ptr = match arena.try_alloc_raw(size, 8) {
                Some(ptr) => ptr.as_ptr(),
                None => return InsertResult::Oom,
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
                    // Committed at level 0. Splice into upper levels (best-effort).
                    // Each level retries until it succeeds — no artificial retry cap.
                    // Upper levels are optimization hints; correctness depends only on level 0.
                    for i in 1..h {
                        let mut pred = preds[i];
                        if pred.is_null() {
                            continue;
                        }
                        loop {
                            // Re-read the current successor at this level
                            let curr_succ = unsafe { tower_load(pred.ptr(), i) };
                            // Set our node's tower to point to the current succ
                            unsafe { tower_store(node_ptr, i, curr_succ) };
                            // Try to CAS pred's pointer from curr_succ to us
                            let cas = unsafe { tower_cas(pred.ptr(), i, curr_succ, new_ptr) };
                            if cas.is_ok() {
                                break;
                            }
                            // CAS failed: another node was inserted between pred and succ.
                            // Re-find the predecessor at this level.
                            let (new_preds, _) = self.find_less(key);
                            pred = new_preds[i];
                            if pred.is_null() {
                                break;
                            }
                        }
                    }

                    // Update list height (CAS loop — another thread may be updating too)
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

                    return InsertResult::Success;
                }
                Err(_) => {
                    // CAS failed — retry from scratch (arena allocation wasted)
                    continue;
                }
            }
        }
    }

    // ─── Delete (tombstone) ────────────────────────────────────────────────

    /// Delete a key by marking it with a tombstone.
    /// Returns true if the key was found and tombstoned by this call.
    /// Returns false if the key was not found or was already tombstoned.
    ///
    /// Note: under concurrent insert/delete, a delete may return false while
    /// a concurrent insert of the same key succeeds. This is linearizable —
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
                    return false; // end of list
                }
                level -= 1;
                continue;
            }
            let next_node = next.ptr();
            // Lookahead prefetch: load next's successor into L1
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
                        // Found at level 0 — try to set tombstone.
                        // set_tombstone returns false if already tombstoned.
                        return unsafe { set_tombstone(next_node) };
                    }
                    level -= 1;
                }
                std::cmp::Ordering::Greater => {
                    if level == 0 {
                        return false; // key not found
                    }
                    level -= 1;
                }
            }
        }
    }

    // ─── find_less ─────────────────────────────────────────────────────────

    /// Find predecessors and successors for the given key at each level.
    /// Returns (preds, succs) where each entry is a `TowerPtr`.
    ///
    /// Uses Masstree-style lookahead prefetching for cache efficiency.
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

    /// Capture the current insertion sequence number for snapshot visibility.
    /// Any node with seq <= the returned value was fully inserted before
    /// this call and is visible to the snapshot.
    pub(crate) fn next_snapshot_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed) as u64
    }

    /// Reset the skip list with a new sentinel head node.
    ///
    /// # Safety
    /// `head` must point to a valid sentinel node with height MAX_HEIGHT
    /// and sequence number 0. No concurrent readers or writers may be active.
    pub(crate) unsafe fn reset(&mut self, head: *const u8) {
        self.head = head;
        self.height.store(1, Ordering::Relaxed);
        self.next_seq.store(1, Ordering::Relaxed);
    }
}
