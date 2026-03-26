//! TTL-based enrichment cache using `FxHashMap` for concurrent access.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;
use varpulis_core::Value;

/// Maximum cache entries before eviction.
const MAX_ENTRIES: usize = 100_000;

/// A single cached enrichment result with expiry time.
#[derive(Debug)]
struct CacheEntry {
    fields: HashMap<String, Value>,
    expires_at: Instant,
}

/// Thread-safe TTL cache for enrichment results.
///
/// Uses a `Mutex<FxHashMap>` for simplicity (enrichment calls are async I/O-bound,
/// so lock contention is negligible compared to network latency).
#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_cache_insert_and_get() {
        let cache = EnrichmentCache::new(Duration::from_secs(60));
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::Str("alice".into()));
        fields.insert("score".to_string(), Value::Int(42));

        cache.insert("user:1".to_string(), fields.clone());

        let result = cache.get("user:1");
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(got.get("name"), Some(&Value::Str("alice".into())));
        assert_eq!(got.get("score"), Some(&Value::Int(42)));
    }

    #[test]
    fn test_cache_miss() {
        let cache = EnrichmentCache::new(Duration::from_secs(60));

        let result = cache.get("nonexistent");
        assert!(result.is_none());

        let (hits, misses) = cache.stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_cache_hit_counter() {
        let cache = EnrichmentCache::new(Duration::from_secs(60));
        let mut fields = HashMap::new();
        fields.insert("k".to_string(), Value::Bool(true));
        cache.insert("key".to_string(), fields);

        // Perform multiple gets to verify hit counter
        for _ in 0..5 {
            let result = cache.get("key");
            assert!(result.is_some());
        }

        let (hits, misses) = cache.stats();
        assert_eq!(hits, 5);
        assert_eq!(misses, 0);
    }

    #[test]
    fn test_cache_ttl_expiry() {
        let cache = EnrichmentCache::new(Duration::from_millis(10));
        let mut fields = HashMap::new();
        fields.insert("temp".to_string(), Value::Float(2.78));
        cache.insert("ephemeral".to_string(), fields);

        // Should be present immediately
        assert!(cache.get("ephemeral").is_some());

        // Wait past TTL
        std::thread::sleep(Duration::from_millis(50));

        // Should be expired now
        let result = cache.get("ephemeral");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_stats() {
        let cache = EnrichmentCache::new(Duration::from_secs(60));
        let mut fields = HashMap::new();
        fields.insert("v".to_string(), Value::Null);
        cache.insert("a".to_string(), fields);

        // 2 hits
        cache.get("a");
        cache.get("a");
        // 3 misses
        cache.get("x");
        cache.get("y");
        cache.get("z");

        let (hits, misses) = cache.stats();
        assert_eq!(hits, 2);
        assert_eq!(misses, 3);
    }
}
