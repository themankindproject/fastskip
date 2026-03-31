//! Per-thread arena shard pool for lock-free memory allocation.
//!
//! This module provides [`ConcurrentArena`], which maintains a fixed number
//! of arena shards. Each writer thread is assigned a unique shard via atomic
//! round-robin, ensuring that no two threads ever hold `&mut Arena` to the
//! same shard (avoiding data races without locking).
//!
//! Readers never allocate, so they are contention-free. Thread-local caching
//! avoids repeated atomic lookups on the hot path.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use fastarena::{Arena, ArenaStats};

/// Default initial block size per arena shard (64 KB).
#[allow(dead_code)]
const DEFAULT_INITIAL_BLOCK: usize = 64 * 1024;

/// Pool of per-thread arena shards for concurrent allocation.
///
/// Each writer thread calls [`local()`](Self::local) to obtain a mutable
/// reference to its assigned arena shard. Shards are assigned via atomic
/// round-robin on first access, with the assignment cached in thread-local
/// storage for subsequent calls.
///
/// # Thread safety
///
/// The arena is `Send + Sync`. Each concurrent writer thread gets a distinct
/// shard, so no locking is needed. Readers never allocate and access nodes
/// through shared references.
pub(crate) struct ConcurrentArena {
    shards: Vec<UnsafeCell<Arena>>,
    /// Per-instance shard assignment counter. Each call to `local()` from a
    /// new thread atomically claims the next shard index. Monotonically
    /// increments up to `shards.len()`, then panics if exceeded.
    shard_assign: AtomicUsize,
    /// Running total of allocated bytes across all shards.
    /// Updated on each allocation for O(1) memory_usage().
    bytes_allocated: AtomicUsize,
}

// Thread-local storage for per-arena shard assignments.
// Small inline array avoids HashMap overhead on the hot path.
// Most threads interact with 1-2 arenas, so 4 slots is plenty.
const MAX_CACHED_ARENAS: usize = 8;
thread_local! {
    static SHARD_CACHE: std::cell::RefCell<[(usize, usize); MAX_CACHED_ARENAS]> =
        const { std::cell::RefCell::new([(0, usize::MAX); MAX_CACHED_ARENAS]) };
}

// SAFETY: Each shard is accessed by at most one writer thread at a time.
// The per-instance atomic counter ensures uniqueness for concurrent threads
// that call `local()` on the same ConcurrentArena.
unsafe impl Send for ConcurrentArena {}
unsafe impl Sync for ConcurrentArena {}

impl ConcurrentArena {
    /// Create a new `ConcurrentArena` with the given number of shards
    /// and the default initial block size (64 KB per shard).
    #[allow(dead_code)]
    pub fn new(num_shards: usize) -> Self {
        Self::with_block_size(num_shards, DEFAULT_INITIAL_BLOCK)
    }

    /// Create a new `ConcurrentArena` with a custom initial block size per shard.
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is 0.
    pub fn with_block_size(num_shards: usize, block_size: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(UnsafeCell::new(Arena::with_capacity(block_size)));
        }
        ConcurrentArena {
            shards,
            shard_assign: AtomicUsize::new(0),
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    /// Record an allocation of `size` bytes in the running total.
    /// Call this immediately after a successful arena allocation.
    #[inline(always)]
    pub fn record_alloc(&self, size: usize) {
        self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
    }

    /// O(1) bytes allocated across all shards (approximate under concurrent allocs).
    #[inline(always)]
    pub fn bytes_allocated_fast(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }

    /// Get this thread's arena shard. First call assigns a unique shard via
    /// atomic increment. Returns `&mut Arena` — safe because each concurrent
    /// thread gets a distinct shard index.
    ///
    /// # Panics
    /// Panics if more threads call `local()` than there are shards.
    /// This prevents two threads from obtaining `&mut Arena` to the same
    /// shard, which would violate Rust's aliasing rules (UB).
    #[allow(clippy::mut_from_ref)]
    pub fn local(&self) -> &mut Arena {
        let self_ptr = self as *const Self as usize;
        SHARD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            // Linear scan of 8 entries — faster than HashMap hash+probe for small N
            for i in 0..MAX_CACHED_ARENAS {
                if cache[i].0 == self_ptr {
                    return unsafe { &mut *self.shards[cache[i].1].get() };
                }
                if cache[i].1 == usize::MAX {
                    // Empty slot — claim a shard
                    let assigned = self.shard_assign.fetch_add(1, Ordering::Relaxed);
                    assert!(
                        assigned < self.shards.len(),
                        "more concurrent threads ({}) than arena shards ({}); \
                         increase shard count via with_shards() or with_capacity_and_shards()",
                        assigned + 1,
                        self.shards.len()
                    );
                    cache[i] = (self_ptr, assigned);
                    return unsafe { &mut *self.shards[assigned].get() };
                }
            }
            // Cache full — evict slot 0, claim new shard
            let assigned = self.shard_assign.fetch_add(1, Ordering::Relaxed);
            assert!(
                assigned < self.shards.len(),
                "more concurrent threads ({}) than arena shards ({}); \
                 increase shard count via with_shards() or with_capacity_and_shards()",
                assigned + 1,
                self.shards.len()
            );
            cache[0] = (self_ptr, assigned);
            unsafe { &mut *self.shards[assigned].get() }
        })
    }

    /// Aggregate memory statistics across all shards.
    ///
    /// Returns a combined [`ArenaStats`] with total `bytes_allocated`,
    /// `bytes_reserved`, and `block_count` summed across all shards.
    pub fn stats(&self) -> ArenaStats {
        let mut total = ArenaStats::default();
        for shard in &self.shards {
            let arena = unsafe { &*shard.get() };
            let s = arena.stats();
            total.bytes_allocated += s.bytes_allocated;
            total.bytes_reserved += s.bytes_reserved;
            total.block_count += s.block_count;
        }
        total
    }

    /// Reset all arena shards, reclaiming all allocated memory.
    ///
    /// Called during [`ConcurrentSkipList::reset`](crate::ConcurrentSkipList::reset)
    /// to reuse the memtable. Also clears this thread's cached shard index.
    ///
    /// # Safety
    ///
    /// No concurrent readers or writers may be active. All pointers into
    /// the arena become invalid after this call.
    pub unsafe fn reset_all(&mut self) {
        for shard in &mut self.shards {
            shard.get_mut().reset();
        }
        self.bytes_allocated.store(0, Ordering::Relaxed);
        // Clear this thread's cached shard index for this arena.
        let self_ptr = self as *const Self as usize;
        SHARD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            for i in 0..MAX_CACHED_ARENAS {
                if cache[i].0 == self_ptr {
                    cache[i] = (0, usize::MAX);
                    break;
                }
            }
        });
    }

    /// Clear the calling thread's cached shard index for all arenas.
    ///
    /// Call this from any thread after a full [`reset_all`](Self::reset_all)
    /// to ensure fresh shard assignment on the next [`local`](Self::local) call.
    /// Useful when a thread is repurposed after a memtable flush.
    #[allow(dead_code)]
    pub fn reset_local() {
        SHARD_CACHE.with(|cache| {
            *cache.borrow_mut() = [(0, usize::MAX); MAX_CACHED_ARENAS];
        });
    }

    /// Returns the number of arena shards.
    #[allow(dead_code)]
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }
}

impl Drop for ConcurrentArena {
    fn drop(&mut self) {
        // Clear this arena's shard cache entry to prevent accumulation
        // when many arenas are created on the same thread (e.g., in benchmarks).
        let self_ptr = self as *const Self as usize;
        SHARD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            for i in 0..MAX_CACHED_ARENAS {
                if cache[i].0 == self_ptr {
                    cache[i] = (0, usize::MAX);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_arena_basic() {
        let arena = ConcurrentArena::new(4);
        let local = arena.local();
        let val = local.alloc(42u64);
        assert_eq!(*val, 42);
    }

    #[test]
    fn test_stats_aggregation() {
        let arena = ConcurrentArena::new(2);
        {
            let local = arena.local();
            local.alloc(1u64);
        }
        let stats = arena.stats();
        assert!(stats.bytes_allocated > 0);
    }

    #[test]
    fn test_multiple_arenas_same_thread() {
        let arena_a = ConcurrentArena::new(4);
        let arena_b = ConcurrentArena::new(4);
        // Same thread can use both arenas without conflict
        let local_a = arena_a.local();
        local_a.alloc(1u64);
        let local_b = arena_b.local();
        local_b.alloc(2u64);
        assert_eq!(*arena_a.local().alloc(3u64), 3);
    }
}
