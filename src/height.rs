//! Random tower height generation for skip list nodes.
//!
//! Each new node is assigned a random tower height using a geometric
//! distribution (p=0.5). The height determines how many levels the node
//! participates in, which controls the skip list's search performance.
//!
//! Heights are generated using a thread-local splitmix64 PRNG seeded
//! from the system clock and a global counter, ensuring different
//! threads get different sequences.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::MAX_HEIGHT;

/// Global counter mixed into each thread's seed to ensure distinct
/// sequences across threads, even if spawned in rapid succession.
static SEED_COUNTER: AtomicU64 = AtomicU64::new(1);

// Thread-local PRNG state. Each thread gets its own splitmix64 generator
// seeded from the system clock and a global counter.
thread_local! {
    static RNG_STATE: Cell<u64> = Cell::new(seed_from_time());
}

/// Generate a unique seed from the system clock and a global counter.
///
/// Combines the current time (seconds XOR nanoseconds) with a monotonic
/// counter to ensure distinct seeds even for threads spawned within the
/// same nanosecond. Returns a non-zero value.
fn seed_from_time() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let time = now.as_secs() ^ now.subsec_nanos() as u64;
    // Mix with a global counter so each thread gets a distinct seed
    let counter = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = time ^ counter.wrapping_mul(0x517cc1b727220a95);
    if seed == 0 {
        0xDEAD_BEEF_CAFE_BABE
    } else {
        seed
    }
}

/// Generate a random tower height in `1..=MAX_HEIGHT`.
///
/// Uses the number of trailing zeros in a splitmix64 random `u64` plus 1,
/// which produces a geometric distribution with p=0.5:
///
/// - P(height=1) ≈ 0.5
/// - P(height=2) ≈ 0.25
/// - P(height=3) ≈ 0.125
/// - ...and so on, capped at [`MAX_HEIGHT`] (20).
///
/// This distribution ensures most nodes only participate in the bottom
/// few levels (fast insertion), while a few nodes span many levels
/// (fast lookup). The expected list height is O(log n).
///
/// Uses splitmix64 for better statistical quality than xorshift64.
pub(crate) fn random_height() -> usize {
    RNG_STATE.with(|state| {
        let mut x = state.get();
        // splitmix64 next
        x = x.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^= z >> 31;
        state.set(x);
        (z.trailing_zeros() as usize + 1).min(MAX_HEIGHT)
    })
}

/// Returns the maximum tower height ([`MAX_HEIGHT`] = 20).
///
/// Exposed as a `const fn` for use in const contexts and assertions.
#[allow(dead_code)]
pub(crate) const fn max_height() -> usize {
    MAX_HEIGHT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_height_bounds() {
        for _ in 0..10_000 {
            let h = random_height();
            assert!((1..=MAX_HEIGHT).contains(&h), "height out of range: {}", h);
        }
    }

    #[test]
    fn test_distribution() {
        let mut histogram = [0usize; MAX_HEIGHT + 1];
        let n = 100_000;
        for _ in 0..n {
            let h = random_height();
            histogram[h] += 1;
        }

        // Height 1 should be the most common (p=0.5)
        assert!(
            histogram[1] > histogram[2],
            "h1={} should be > h2={}",
            histogram[1],
            histogram[2]
        );
        // Height 2 should be more common than height 3
        assert!(
            histogram[2] > histogram[3],
            "h2={} should be > h3={}",
            histogram[2],
            histogram[3]
        );
    }
}
