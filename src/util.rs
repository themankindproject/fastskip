//! Low-level utility functions for key comparison and cache prefetching.
//!
//! Provides performance-critical helpers used throughout the skip list:
//! - [`compare_keys`] — lexicographic byte comparison with fast-path prefixes
//! - [`prefetch_read`] — CPU cache prefetch hint for lookahead optimization

/// Compare two byte slices in lexicographic (dictionary) order.
///
/// Uses Masstree-inspired fast paths:
/// 1. If both keys are >= 8 bytes, compare the first 8 bytes as a
///    big-endian `u64`. If they differ, return immediately.
/// 2. If both keys are >= 4 bytes (but the 8-byte prefix matched),
///    compare the first 4 bytes as a big-endian `u32`.
/// 3. Fall back to standard byte-by-byte comparison via `a.cmp(b)`.
///
/// The fast paths avoid the `memcmp`-style loop when prefixes differ,
/// which is the common case for LSM-tree keys with sorted prefixes.
#[inline]
pub(crate) fn compare_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    if a.len() >= 8 && b.len() >= 8 {
        // Read the first 8 bytes of each key as a big-endian u64.
        // On x86_64 this compiles to a single `movbe` or `mov + bswap`.
        let prefix_a = u64::from_be_bytes(a[..8].try_into().unwrap());
        let prefix_b = u64::from_be_bytes(b[..8].try_into().unwrap());
        if prefix_a != prefix_b {
            return prefix_a.cmp(&prefix_b);
        }
        // Prefixes match; try a 4-byte fallback at offset 8.
        if a.len() >= 12 && b.len() >= 12 {
            let lo_a = u32::from_be_bytes(a[8..12].try_into().unwrap());
            let lo_b = u32::from_be_bytes(b[8..12].try_into().unwrap());
            if lo_a != lo_b {
                return lo_a.cmp(&lo_b);
            }
        }
        // Fall through to byte-by-byte.
    }
    a.cmp(b)
}

/// Prefetch data into L1 cache. Hints the CPU to start loading the cache
/// line containing `ptr` while we continue working on the current node.
///
/// Compiles to:
/// - x86: `PREFETCHT0`
/// - AArch64: `PRFM PLDL1KEEP`
///
/// This is a no-op on architectures without prefetch support.
#[inline(always)]
pub(crate) fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, nomem));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}
