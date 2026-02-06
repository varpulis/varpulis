//! Event routing: maps event types to stream names
//!
//! This module provides the EventRouter which manages the mapping from
//! event types to the streams that should receive those events.

use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Event router that maps event types to stream names.
///
/// Uses `Arc<[String]>` internally for zero-cost sharing in the hot path,
/// avoiding Vec cloning when accessing routes during event processing.
pub(crate) struct EventRouter {
    /// event_type -> stream names (Arc<[String]> for O(1) clone in hot path)
    routes: FxHashMap<String, Arc<[String]>>,
}

impl EventRouter {
    /// Create a new empty router
    pub fn new() -> Self {
        Self {
            routes: FxHashMap::default(),
        }
    }

    /// Register a stream to receive events of a given type.
    /// Uses Arc internally to avoid Vec cloning in the hot path.
    pub fn add_route(&mut self, event_type: &str, stream_name: &str) {
        let existing = self.routes.remove(event_type);
        let mut streams: Vec<String> = existing
            .map(|arc| arc.iter().cloned().collect())
            .unwrap_or_default();
        if !streams.contains(&stream_name.to_string()) {
            streams.push(stream_name.to_string());
        }
        self.routes.insert(event_type.to_string(), streams.into());
    }

    /// Get stream names registered for an event type.
    /// Returns `Option<&Arc<[String]>>` for zero-cost access in the hot path.
    pub fn get_routes(&self, event_type: &str) -> Option<&Arc<[String]>> {
        self.routes.get(event_type)
    }

    /// Clone the route arc (O(1) atomic increment).
    /// Returns an empty Arc slice if no routes exist for the event type.
    #[allow(dead_code)]
    pub fn clone_routes(&self, event_type: &str) -> Arc<[String]> {
        self.routes
            .get(event_type)
            .cloned()
            .unwrap_or_else(|| Arc::from([]))
    }

    /// Clear all routes (used during hot reload)
    pub fn clear(&mut self) {
        self.routes.clear();
    }

    /// Get all registered event types (keys)
    #[allow(dead_code)]
    pub fn event_types(&self) -> impl Iterator<Item = &String> {
        self.routes.keys()
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_single_route() {
        let mut router = EventRouter::new();
        router.add_route("OrderCreated", "OrderStream");

        let routes = router.get_routes("OrderCreated").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0], "OrderStream");
    }

    #[test]
    fn test_add_multiple_streams_for_same_event() {
        let mut router = EventRouter::new();
        router.add_route("OrderCreated", "OrderStream");
        router.add_route("OrderCreated", "AuditStream");

        let routes = router.get_routes("OrderCreated").unwrap();
        assert_eq!(routes.len(), 2);
        assert!(routes.iter().any(|s| s == "OrderStream"));
        assert!(routes.iter().any(|s| s == "AuditStream"));
    }

    #[test]
    fn test_no_duplicate_routes() {
        let mut router = EventRouter::new();
        router.add_route("OrderCreated", "OrderStream");
        router.add_route("OrderCreated", "OrderStream"); // duplicate

        let routes = router.get_routes("OrderCreated").unwrap();
        assert_eq!(routes.len(), 1);
    }

    #[test]
    fn test_nonexistent_route() {
        let router = EventRouter::new();
        assert!(router.get_routes("Unknown").is_none());
    }

    #[test]
    fn test_clear() {
        let mut router = EventRouter::new();
        router.add_route("OrderCreated", "OrderStream");
        router.clear();
        assert!(router.get_routes("OrderCreated").is_none());
    }

    #[test]
    fn test_clone_routes_empty() {
        let router = EventRouter::new();
        let routes = router.clone_routes("Unknown");
        assert!(routes.is_empty());
    }
}
