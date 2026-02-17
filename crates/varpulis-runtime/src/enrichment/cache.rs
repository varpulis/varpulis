//! TTL-based enrichment cache using DashMap for concurrent access.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;
use varpulis_core::Value;

/// Maximum cache entries before eviction.
const MAX_ENTRIES: usize = 100_000;

/// A single cached enrichment result with expiry time.
struct CacheEntry {
    fields: HashMap<String, Value>,
    expires_at: Instant,
}

/// Thread-safe TTL cache for enrichment results.
///
/// Uses a `Mutex<FxHashMap>` for simplicity (enrichment calls are async I/O-bound,
/// so lock contention is negligible compared to network latency).
pub struct EnrichmentCache {
    entries: std::sync::Mutex<FxHashMap<String, CacheEntry>>,
    ttl: std::time::Duration,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl EnrichmentCache {
    /// Create a new cache with the given TTL.
    pub fn new(ttl: std::time::Duration) -> Self {
        Self {
            entries: std::sync::Mutex::new(FxHashMap::default()),
            ttl,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Look up a cached result. Returns None on miss or expiry.
    pub fn get(&self, key: &str) -> Option<HashMap<String, Value>> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.get(key) {
            if entry.expires_at > Instant::now() {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.fields.clone());
            }
            // Expired — remove it
            entries.remove(key);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert a result into the cache.
    pub fn insert(&self, key: String, fields: HashMap<String, Value>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        // Evict oldest entries if at capacity
        if entries.len() >= MAX_ENTRIES {
            // Simple eviction: remove ~10% of oldest entries
            let to_remove = MAX_ENTRIES / 10;
            let now = Instant::now();
            let mut keys_to_remove: Vec<String> = Vec::with_capacity(to_remove);
            for (k, v) in entries.iter() {
                if v.expires_at <= now || keys_to_remove.len() < to_remove {
                    keys_to_remove.push(k.clone());
                }
                if keys_to_remove.len() >= to_remove {
                    break;
                }
            }
            for k in keys_to_remove {
                entries.remove(&k);
            }
        }
        entries.insert(
            key,
            CacheEntry {
                fields,
                expires_at: Instant::now() + self.ttl,
            },
        );
    }

    /// Get cache hit/miss counters.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}
