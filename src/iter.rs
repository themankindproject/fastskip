use std::marker::PhantomData;
use std::sync::atomic::Ordering;

use crate::node::{is_tombstone, node_key, node_seq, node_value, tower_load};
use crate::util::prefetch_read;
use crate::ConcurrentSkipList;

// ─── Entry ─────────────────────────────────────────────────────────────────────

/// A single entry yielded by the iterator.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub is_tombstone: bool,
}

// ─── Helpers ────────────────────────────────────────────────────────────────────

/// Advance `current` to the next node at level 0. Returns the new node
/// pointer, or null if at the end.
#[inline]
fn advance_node(current: *const u8) -> *const u8 {
    let next = unsafe { tower_load(current, 0) };
    if next.is_null() {
        std::ptr::null()
    } else {
        next.ptr()
    }
}

// ─── Snapshot (point-in-time) ──────────────────────────────────────────────────

/// A point-in-time snapshot of the skip list.
///
/// The snapshot records a sequence number at creation time.
/// The iterator skips any node inserted after this sequence number,
/// providing a consistent point-in-time view even under concurrent inserts.
///
/// The snapshot borrows the `ConcurrentSkipList`, so it cannot outlive it
/// and the list cannot be modified while snapshots exist (enforced by `&self`).
#[derive(Debug)]
pub struct Snapshot<'a> {
    pub(crate) head: *const u8,
    /// Sequence number ceiling: only nodes with seq < snap_seq are visible.
    pub(crate) snap_seq: u64,
    /// Borrows the skip list to enforce lifetime and Send/Sync.
    pub(crate) _owner: PhantomData<&'a ConcurrentSkipList>,
}

// SAFETY: The snapshot contains a raw pointer into arena memory owned by
// ConcurrentSkipList, which is Send+Sync. The lifetime parameter ensures
// the arena outlives the snapshot. The arena is thread-safe (per-thread
// shards with atomic assignment).
unsafe impl<'a> Send for Snapshot<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for Snapshot<'a> where ConcurrentSkipList: Sync {}

impl<'a> Snapshot<'a> {
    /// Create an iterator over this snapshot.
    pub fn iter(&self) -> SnapshotIter<'a> {
        SnapshotIter {
            current: self.head,
            snap_seq: self.snap_seq,
            _owner: PhantomData,
        }
    }
}

/// Iterator that walks level 0, skipping nodes inserted after the snapshot.
#[derive(Debug)]
pub struct SnapshotIter<'a> {
    current: *const u8,
    snap_seq: u64,
    _owner: PhantomData<&'a ConcurrentSkipList>,
}

unsafe impl<'a> Send for SnapshotIter<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for SnapshotIter<'a> where ConcurrentSkipList: Sync {}

impl<'a> Iterator for SnapshotIter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.current = advance_node(self.current);

            if self.current.is_null() {
                return None;
            }

            // Skip nodes inserted at or after this snapshot was taken
            if unsafe { node_seq(self.current) } >= self.snap_seq {
                continue;
            }

            return Some(Entry {
                key: unsafe { node_key(self.current) },
                value: unsafe { node_value(self.current) },
                is_tombstone: unsafe { is_tombstone(self.current) },
            });
        }
    }
}

// ─── Live iterator ─────────────────────────────────────────────────────────────

/// Live iterator over the current state.
/// May see concurrent inserts that happen mid-iteration.
#[derive(Debug)]
pub struct Iter<'a> {
    pub(crate) current: *const u8,
    pub(crate) _owner: PhantomData<&'a ConcurrentSkipList>,
}

unsafe impl<'a> Send for Iter<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for Iter<'a> where ConcurrentSkipList: Sync {}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.current = advance_node(self.current);

        if self.current.is_null() {
            return None;
        }

        Some(Entry {
            key: unsafe { node_key(self.current) },
            value: unsafe { node_value(self.current) },
            is_tombstone: unsafe { is_tombstone(self.current) },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // We can't know the exact count without traversing, but we can
        // report 0 if we haven't started and the list might be empty.
        (0, None)
    }
}

// ─── Cursor (range scan) ───────────────────────────────────────────────────────

/// A cursor for range scans over the skip list.
///
/// Positions at a node and can advance forward. Created via
/// [`ConcurrentSkipList::cursor`] or [`ConcurrentSkipList::cursor_at`].
pub struct Cursor<'a> {
    pub(crate) current: *const u8,
    pub(crate) _owner: PhantomData<&'a ConcurrentSkipList>,
}

unsafe impl<'a> Send for Cursor<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for Cursor<'a> where ConcurrentSkipList: Sync {}

impl<'a> Cursor<'a> {
    /// Position the cursor at the first key >= `target` (lower bound seek).
    /// Returns `true` if a key was found, `false` if all keys are < target.
    pub fn seek(&mut self, skiplist: &'a ConcurrentSkipList, target: &[u8]) -> bool {
        // Walk from head to find the predecessor of target
        let mut x = skiplist.skiplist.head;
        let h = skiplist.skiplist.height.load(Ordering::Acquire);
        let mut level = if h > 0 { h - 1 } else { 0 };

        prefetch_read(x);

        loop {
            let next = unsafe { crate::node::tower_load(x, level) };
            if next.is_null() {
                if level == 0 {
                    // Reached end, target is past all keys
                    self.current = std::ptr::null();
                    return false;
                }
                level -= 1;
                continue;
            }
            let next_node = next.ptr();
            // Lookahead prefetch
            let next_next = unsafe { crate::node::tower_load(next_node, level) };
            if !next_next.is_null() {
                prefetch_read(next_next.ptr());
            }
            let next_key = unsafe { node_key(next_node) };
            match crate::util::compare_keys(next_key, target) {
                std::cmp::Ordering::Less => {
                    x = next_node;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    if level == 0 {
                        // Found: next_node is the first key >= target
                        self.current = next_node;
                        return true;
                    }
                    level -= 1;
                }
            }
        }
    }

    /// Advance to the next entry. Returns `true` if a next entry exists.
    pub fn next_entry(&mut self) -> bool {
        if self.current.is_null() {
            return false;
        }
        self.current = advance_node(self.current);
        !self.current.is_null()
    }

    /// Get the current entry, if the cursor is valid.
    pub fn entry(&self) -> Option<Entry<'a>> {
        if self.current.is_null() {
            return None;
        }
        Some(Entry {
            key: unsafe { node_key(self.current) },
            value: unsafe { node_value(self.current) },
            is_tombstone: unsafe { is_tombstone(self.current) },
        })
    }

    /// Returns `true` if the cursor points to a valid entry.
    pub fn valid(&self) -> bool {
        !self.current.is_null()
    }
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }
        let entry = self.entry();
        self.next_entry();
        entry
    }
}
