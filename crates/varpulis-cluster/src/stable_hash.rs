//! A hash whose output is part of the cluster's wire contract.
//!
//! Two decisions in this crate must produce the same number in every process
//! and every build: which replica a partition key belongs to, and what a
//! durable consumer is called. Both are agreements between machines, not
//! local lookups.
//!
//! `std::collections::hash_map::DefaultHasher` cannot carry that agreement.
//! Its documentation is explicit that "the internal algorithm is not
//! specified, and so it and its hashes should not be relied upon over
//! releases". Nothing warns when it changes: a coordinator rebuilt on a newer
//! toolchain simply starts routing `user-42` to a different replica than the
//! one holding `user-42`'s window state, and the pipeline goes on emitting —
//! now from two half-populated partitions. During a rolling upgrade the two
//! coordinator versions disagree with each other at the same time.
//!
//! So the algorithm is spelled out here instead. FNV-1a for the accumulation
//! and MurmurHash3's `fmix64` finalizer for the avalanche, both fully
//! specified by constants that live in this file and can never drift.

/// FNV-1a, 64-bit.
///
/// Chosen over pulling in a hash crate because the whole function is nine
/// lines and its constants are the contract: an external crate could change
/// its output in a patch release and reshuffle a live cluster's partitions.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// MurmurHash3's 64-bit finalizer.
///
/// FNV-1a's weakness is its low bits: short, similar keys differ there far
/// less than uniformly, and `% n` reads exactly those bits. `bucket` below
/// would inherit that bias directly. This mixes the whole word so every
/// output bit depends on every input bit.
fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Map a partition key onto one of `n` buckets, stably and uniformly.
///
/// Returns 0 when `n` is 0 so callers cannot divide by zero; a caller with no
/// buckets has nothing to select and should not be asking.
pub fn bucket(key: &str, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    (fmix64(fnv1a(key.as_bytes())) % n as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The golden vector. These numbers are a wire contract: they decide which
    /// machine holds which key's state, so changing them re-partitions every
    /// running cluster and splits per-key state across two replicas until the
    /// operator notices.
    ///
    /// If this test fails, the hash changed. That is a breaking change to the
    /// cluster protocol, not a test to update — it needs a migration story
    /// (drain, or dual-route through the switch), not a new expected value.
    #[test]
    fn hash_is_pinned_to_its_published_values() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x85944171f73967e8);
    }

    /// The four keys `varpulis-cluster`'s chaos suite partitions on. Under
    /// the `DefaultHasher` this module replaces, the JSON renderings of all
    /// four hashed to an even number, so a two-replica group put every one of
    /// them on replica 0 and the suite's distribution assertion failed. It was
    /// bad luck rather than a broken hash, but it is the exact bad luck the
    /// finalizer exists to stop mattering, so it is pinned here.
    #[test]
    fn the_chaos_suite_partition_keys_spread_across_two_replicas() {
        let rendered: Vec<String> = ["alpha", "beta", "gamma", "delta"]
            .iter()
            .map(|k| serde_json::Value::String((*k).into()).to_string())
            .collect();
        let seen: std::collections::HashSet<usize> =
            rendered.iter().map(|k| bucket(k, 2)).collect();
        assert_eq!(
            seen.len(),
            2,
            "these four keys must not all land on one replica: {:?}",
            rendered
                .iter()
                .map(|k| (k, bucket(k, 2)))
                .collect::<Vec<_>>()
        );
    }

    /// Uniformity, stated as a bound loose enough never to flake and tight
    /// enough to catch a hash that has stopped spreading.
    #[test]
    fn buckets_are_close_to_uniform() {
        for n in [2usize, 3, 4, 8, 16] {
            let mut counts = vec![0usize; n];
            for i in 0..10_000 {
                counts[bucket(&format!("key-{i}"), n)] += 1;
            }
            let expected = 10_000 / n;
            for (i, &c) in counts.iter().enumerate() {
                assert!(
                    c > expected * 3 / 4 && c < expected * 5 / 4,
                    "bucket {i} of {n} got {c}, expected near {expected}: {counts:?}"
                );
            }
        }
    }

    #[test]
    fn bucket_of_zero_does_not_divide_by_zero() {
        assert_eq!(bucket("anything", 0), 0);
    }
}
