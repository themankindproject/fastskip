use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use fastarena::{Arena, ArenaStats};

#[allow(dead_code)]
const DEFAULT_INITIAL_BLOCK: usize = 64 * 1024;

/// Pool of per-thread arenas. Each writer thread gets a unique shard via
/// atomic round-robin assignment. Readers never allocate.
pub(crate) struct ConcurrentArena {
    shards: Vec<UnsafeCell<Arena>>,
    /// Per-instance shard assignment counter. Each call to `local()` from a
    /// new thread atomically claims the next shard index.
    shard_assign: AtomicUsize,
}

// Thread-local storage for per-arena shard assignments.
// We use a Vec of (arena_ptr, shard_index) to support multiple arenas
// on the same thread (e.g., sequential test execution).
thread_local! {
    static SHARD_CACHE: std::cell::RefCell<Vec<(usize, isize)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

// SAFETY: Each shard is accessed by at most one writer thread at a time.
// The per-instance atomic counter ensures uniqueness for concurrent threads
// that call `local()` on the same ConcurrentArena.
unsafe impl Send for ConcurrentArena {}
unsafe impl Sync for ConcurrentArena {}

impl ConcurrentArena {
    /// Create a new ConcurrentArena with the given number of shards.
    #[allow(dead_code)]
    pub fn new(num_shards: usize) -> Self {
        Self::with_block_size(num_shards, DEFAULT_INITIAL_BLOCK)
    }

    /// Create a new ConcurrentArena with a custom initial block size per shard.
    pub fn with_block_size(num_shards: usize, block_size: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(UnsafeCell::new(Arena::with_capacity(block_size)));
        }
        ConcurrentArena {
            shards,
            shard_assign: AtomicUsize::new(0),
        }
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
            // Look up cached shard for this arena on this thread
            if let Some(&(_, idx)) = cache.iter().find(|&&(ptr, _)| ptr == self_ptr) {
                return unsafe { &mut *self.shards[idx as usize].get() };
            }
            // First call on this thread for this arena — claim a unique shard
            let assigned = self.shard_assign.fetch_add(1, Ordering::Relaxed);
            assert!(
                assigned < self.shards.len(),
                "more concurrent threads ({}) than arena shards ({}); \
                 increase shard count via with_shards() or with_capacity_and_shards()",
                assigned + 1,
                self.shards.len()
            );
            cache.push((self_ptr, assigned as isize));
            unsafe { &mut *self.shards[assigned].get() }
        })
    }

    /// Aggregate stats across all shards.
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

    /// Reset all arenas (memtable flush).
    ///
    /// # Safety
    /// No concurrent readers or writers may be active.
    pub unsafe fn reset_all(&mut self) {
        for shard in &mut self.shards {
            shard.get_mut().reset();
        }
        // Clear this thread's cached shard index for this arena.
        let self_ptr = self as *const Self as usize;
        SHARD_CACHE.with(|cache| {
            cache.borrow_mut().retain(|&(ptr, _)| ptr != self_ptr);
        });
    }

    /// Clear the calling thread's cached shard index for all arenas.
    /// Call this from any thread after a full reset to ensure
    /// fresh shard assignment on next `local()` call.
    #[allow(dead_code)]
    pub fn reset_local() {
        SHARD_CACHE.with(|cache| cache.borrow_mut().clear());
    }

    /// Number of shards.
    #[allow(dead_code)]
    pub fn num_shards(&self) -> usize {
        self.shards.len()
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
