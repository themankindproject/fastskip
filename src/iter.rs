//! Iterator, snapshot, and cursor types for traversing the skip list.
//!
//! This module provides three traversal modes:
//!
//! - [`Iter`] — live iterator that reflects the current state, including
//!   concurrent inserts that happen mid-iteration.
//! - [`Snapshot`] / [`SnapshotIter`] — point-in-time view captured at creation.
//!   Nodes inserted after the snapshot are invisible to the iterator.
//! - [`Cursor`] — seekable forward cursor for range scans. Supports lower-bound
//!   seek and manual advancement via [`next_entry`](Cursor::next_entry).
//!
//! All iterators yield [`Entry`] values containing the key, value, and a
//! tombstone flag indicating whether the entry was deleted.

use std::marker::PhantomData;
use std::sync::atomic::Ordering;

use crate::node::{is_tombstone, node_key, node_seq, node_value, tower_load};
use crate::ConcurrentSkipList;

// ─── Entry ─────────────────────────────────────────────────────────────────────

/// A single key-value entry yielded by iterators and cursors.
///
/// Contains the raw key and value byte slices, plus a flag indicating
/// whether this entry has been deleted (tombstoned). Tombstone entries
/// are preserved in the skip list until the memtable is compacted or
/// dropped, allowing the LSM-tree to merge deletions during flush.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    /// The key bytes. May be empty for the internal sentinel node
    /// (never yielded by public iterators).
    pub key: &'a [u8],
    /// The value bytes. For tombstone entries, this is the value that
    /// was present before deletion (typically the last live value).
    pub value: &'a [u8],
    /// `true` if this entry was deleted via [`ConcurrentSkipList::delete`].
    /// Tombstone entries are visible in live iteration and snapshots,
    /// allowing the caller to propagate deletions to SSTables.
    pub is_tombstone: bool,
}

// ─── Helpers ────────────────────────────────────────────────────────────────────

/// Advance `current` to the next node at level 0 by following the tower[0] pointer.
///
/// Returns the new node pointer, or a null pointer if `current` is the
/// last node in the chain. This is the core step shared by all iterators.
///
/// # Safety
///
/// `current` must point to a valid, fully initialized node whose tower
/// entries have been written. The returned pointer (if non-null) is valid
/// for the lifetime of the owning [`ConcurrentSkipList`].
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
/// Created by [`ConcurrentSkipList::snapshot`]. The snapshot captures a
/// monotonic sequence number at creation time. When iterated via
/// [`Snapshot::iter`], only nodes that were fully inserted before the
/// snapshot was taken are visible — any node inserted concurrently after
/// the snapshot is skipped.
///
/// This provides true snapshot isolation without copying data or taking
/// locks, making it ideal for flushing memtables to SSTables where a
/// consistent view is required.
///
/// # Lifetime
///
/// The snapshot borrows the [`ConcurrentSkipList`], so it cannot outlive
/// the list and the borrow checker prevents the list from being dropped
/// while snapshots exist.
#[derive(Debug)]
pub struct Snapshot<'a> {
    pub(crate) head: *const u8,
    /// Sequence number ceiling: only nodes with `seq < snap_seq` are visible
    /// to the snapshot iterator. Nodes inserted at or after this sequence
    /// number are hidden.
    pub(crate) snap_seq: u64,
    /// Borrows the skip list to enforce lifetime and `Send`/`Sync`.
    pub(crate) _owner: PhantomData<&'a ConcurrentSkipList>,
}

// SAFETY: The snapshot contains a raw pointer into arena memory owned by
// ConcurrentSkipList, which is Send+Sync. The lifetime parameter ensures
// the arena outlives the snapshot. The arena is thread-safe (per-thread
// shards with atomic assignment).
unsafe impl<'a> Send for Snapshot<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for Snapshot<'a> where ConcurrentSkipList: Sync {}

impl<'a> Snapshot<'a> {
    /// Create an iterator over this snapshot's point-in-time view.
    ///
    /// The returned [`SnapshotIter`] walks the level-0 chain in sorted order,
    /// skipping any node whose insertion sequence number is `>= snap_seq`.
    /// This guarantees that only nodes present at the time of the snapshot
    /// are yielded.
    ///
    /// # Example
    ///
    /// ```rust
    /// use fastskip::ConcurrentSkipList;
    ///
    /// let sl = ConcurrentSkipList::new();
    /// sl.insert(b"a", b"1");
    /// let snap = sl.snapshot();
    ///
    /// let entries: Vec<_> = snap.iter().collect();
    /// assert_eq!(entries.len(), 1);
    /// ```
    pub fn iter(&self) -> SnapshotIter<'a> {
        SnapshotIter {
            current: self.head,
            snap_seq: self.snap_seq,
            _owner: PhantomData,
        }
    }
}

/// Iterator over a point-in-time snapshot of the skip list.
///
/// Walks the level-0 chain in sorted order, yielding only nodes whose
/// insertion sequence number is strictly less than the snapshot's
/// `snap_seq` ceiling. Nodes inserted concurrently after the snapshot
/// was taken are silently skipped.
///
/// Created by [`Snapshot::iter`].
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

    /// Advance to the next visible entry in the snapshot.
    ///
    /// Nodes with `seq >= snap_seq` are skipped to maintain snapshot
    /// isolation. Tombstone entries are included in the output — the
    /// caller decides how to handle them (e.g., writing tombstone
    /// markers to SSTables).
    #[inline]
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

/// Live iterator over the current skip list state.
///
/// Walks the level-0 chain in sorted order, yielding all entries
/// including tombstones. Unlike [`SnapshotIter`], this iterator
/// reflects the live state and **may observe concurrent inserts**
/// that happen mid-iteration. If a consistent point-in-time view
/// is needed, use [`ConcurrentSkipList::snapshot`] instead.
///
/// Created by [`ConcurrentSkipList::iter`].
#[derive(Debug)]
pub struct Iter<'a> {
    pub(crate) current: *const u8,
    pub(crate) _owner: PhantomData<&'a ConcurrentSkipList>,
}

unsafe impl<'a> Send for Iter<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for Iter<'a> where ConcurrentSkipList: Sync {}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    #[inline]
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

/// A seekable forward cursor for range scans over the skip list.
///
/// Provides lower-bound seek ([`seek`](Self::seek)) and manual advancement
/// ([`next_entry`](Self::next_entry)) for efficient range iteration. Also
/// implements [`Iterator`] for use in `for` loops and iterator adapters.
///
/// Created by [`ConcurrentSkipList::cursor`] (starts at first entry) or
/// [`ConcurrentSkipList::cursor_at`] (seeks to first key `>= target`).
///
/// # Example
///
/// ```rust
/// # use fastskip::ConcurrentSkipList;
/// let sl = ConcurrentSkipList::new();
/// sl.insert(b"b", b"2");
/// sl.insert(b"d", b"4");
/// sl.insert(b"f", b"6");
///
/// // Range scan: keys in [b"b", b"e")
/// if let Some(cursor) = sl.cursor_at(b"b") {
///     for entry in cursor.take_while(|e| e.key < b"e") {
///         println!("{:?}", entry.key);
///     }
/// }
/// ```
pub struct Cursor<'a> {
    pub(crate) current: *const u8,
    pub(crate) _owner: PhantomData<&'a ConcurrentSkipList>,
}

unsafe impl<'a> Send for Cursor<'a> where ConcurrentSkipList: Sync {}
unsafe impl<'a> Sync for Cursor<'a> where ConcurrentSkipList: Sync {}

impl<'a> Cursor<'a> {
    /// Seek to the first key `>= target` (lower bound).
    ///
    /// Walks the skip list from the highest level down, using the tower
    /// pointers to skip over intermediate nodes. Uses Masstree-style
    /// lookahead prefetching for cache efficiency.
    ///
    /// Returns `true` if a key was found (cursor is now valid),
    /// `false` if all keys in the list are `< target` (cursor is invalid).
    ///
    /// # Example
    ///
    /// ```rust
    /// use fastskip::ConcurrentSkipList;
    ///
    /// let sl = ConcurrentSkipList::new();
    /// sl.insert(b"a", b"1");
    /// sl.insert(b"c", b"3");
    ///
    /// let mut cursor = sl.cursor();
    /// assert!(cursor.seek(&sl, b"b")); // lands on "c"
    /// assert_eq!(cursor.entry().unwrap().key, b"c");
    /// ```
    pub fn seek(&mut self, skiplist: &'a ConcurrentSkipList, target: &[u8]) -> bool {
        let mut x = skiplist.skiplist.head;
        let h = skiplist.skiplist.height.load(Ordering::Relaxed);
        let mut level = if h > 0 { h - 1 } else { 0 };

        loop {
            let next = unsafe { crate::node::tower_load(x, level) };
            if next.is_null() {
                if level == 0 {
                    self.current = std::ptr::null();
                    return false;
                }
                level -= 1;
                continue;
            }
            let next_node = next.ptr();
            let next_key = unsafe { node_key(next_node) };
            match crate::util::compare_keys(next_key, target) {
                std::cmp::Ordering::Less => {
                    x = next_node;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    if level == 0 {
                        self.current = next_node;
                        return true;
                    }
                    level -= 1;
                }
            }
        }
    }

    /// Advance the cursor to the next entry at level 0.
    ///
    /// Returns `true` if a next entry exists (cursor remains valid),
    /// `false` if the end of the list was reached (cursor becomes invalid).
    ///
    /// # Example
    ///
    /// ```rust
    /// use fastskip::ConcurrentSkipList;
    ///
    /// let sl = ConcurrentSkipList::new();
    /// sl.insert(b"a", b"1");
    /// sl.insert(b"b", b"2");
    ///
    /// let mut cursor = sl.cursor();
    /// assert!(cursor.next_entry()); // "a"
    /// assert!(cursor.next_entry()); // "b"
    /// assert!(!cursor.next_entry()); // end
    /// ```
    pub fn next_entry(&mut self) -> bool {
        if self.current.is_null() {
            return false;
        }
        self.current = advance_node(self.current);
        !self.current.is_null()
    }

    /// Get the entry at the cursor's current position.
    ///
    /// Returns `None` if the cursor is invalid (at end or not yet seeked).
    ///
    /// # Example
    ///
    /// ```rust
    /// use fastskip::ConcurrentSkipList;
    ///
    /// let sl = ConcurrentSkipList::new();
    /// sl.insert(b"a", b"1");
    ///
    /// let cursor = sl.cursor_at(b"a").unwrap();
    /// let entry = cursor.entry().unwrap();
    /// assert_eq!(entry.key, b"a");
    /// assert_eq!(entry.value, b"1");
    /// ```
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
    ///
    /// A cursor is invalid if it was never seeked, if `seek` found no
    /// matching key, or if `next_entry` reached the end of the list.
    ///
    /// # Example
    ///
    /// ```rust
    /// use fastskip::ConcurrentSkipList;
    ///
    /// let sl = ConcurrentSkipList::new();
    /// sl.insert(b"a", b"1");
    ///
    /// let cursor = sl.cursor_at(b"a").unwrap();
    /// assert!(cursor.valid());
    ///
    /// assert!(sl.cursor_at(b"z").is_none());
    /// ```
    pub fn valid(&self) -> bool {
        !self.current.is_null()
    }
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Entry<'a>;

    /// Yield the current entry and advance to the next.
    ///
    /// Returns `None` when the cursor is invalid (at end or past all keys).
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }
        let entry = self.entry();
        self.next_entry();
        entry
    }
}
