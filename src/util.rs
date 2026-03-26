/// Key comparison: lexicographic byte ordering.
///
/// Uses prefix fast-paths (Masstree-inspired). Tries an 8-byte big-endian
/// u64 compare first, then a 4-byte fallback for short keys. Avoids the
/// byte-by-byte loop when prefixes differ.
#[inline]
pub(crate) fn compare_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    if a.len() >= 8 && b.len() >= 8 {
        let prefix_a = u64::from_ne_bytes(a[..8].try_into().unwrap());
        let prefix_b = u64::from_ne_bytes(b[..8].try_into().unwrap());
        if prefix_a != prefix_b {
            // Swap bytes in-place for big-endian comparison without
            // depending on target endanness.
            return prefix_a.swap_bytes().cmp(&prefix_b.swap_bytes());
        }
    } else if a.len() >= 4 && b.len() >= 4 {
        let pa = u32::from_ne_bytes(a[..4].try_into().unwrap());
        let pb = u32::from_ne_bytes(b[..4].try_into().unwrap());
        if pa != pb {
            return pa.swap_bytes().cmp(&pb.swap_bytes());
        }
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
