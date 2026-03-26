use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::MAX_HEIGHT;

static SEED_COUNTER: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static RNG_STATE: Cell<u64> = Cell::new(seed_from_time());
}

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

/// O(1) height generation using trailing zeros of a random u64.
/// Geometric distribution with p=0.5.
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
