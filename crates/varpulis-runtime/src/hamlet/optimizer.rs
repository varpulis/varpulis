//! # Hamlet Optimizer - Dynamic Sharing Decisions
//!
//! The optimizer determines at runtime whether sharing is beneficial
//! for each graphlet, based on:
//! - Number of queries sharing the Kleene sub-pattern (ks)
//! - Number of snapshots created (sp)
//! - Graphlet size (g)
//!
//! ## Benefit Model
//!
//! ```text
//! Benefit(G_E) = NonShared(G_E) - Shared(G_E)
//!              = ks × g² - (sp × g² + ks × sp)
//!              = g² × (ks - sp) - ks × sp
//! ```
//!
//! Sharing is beneficial when `ks > sp` and graphlets are sufficiently large.

use crate::greta::QueryId;

/// Decision on whether to share a graphlet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharingDecision {
    /// Process in shared mode (propagate snapshots once)
    Shared,
    /// Process independently per query (no snapshot overhead)
    NonShared,
    /// Split a previously shared graphlet into non-shared
    Split,
    /// Merge previously non-shared graphlets into shared
    Merge,
}

/// Statistics for a Kleene sub-pattern
#[derive(Debug, Clone, Default)]
pub struct KleeneStats {
    /// Event type index
    pub event_type: u16,
    /// Number of queries sharing this pattern
    pub num_queries: usize,
    /// Number of snapshots created in recent window
    pub num_snapshots: usize,
    /// Average graphlet size
    pub avg_graphlet_size: f64,
    /// Number of graphlets processed
    pub num_graphlets: usize,
    /// Total events processed
    pub total_events: u64,
    /// Current mode (shared or not)
    pub currently_shared: bool,
}

impl KleeneStats {
    /// Create new stats for a Kleene sub-pattern
    pub fn new(event_type: u16, num_queries: usize) -> Self {
        Self {
            event_type,
            num_queries,
            ..Default::default()
        }
    }

    /// Update stats when a graphlet is processed
    pub fn update(&mut self, graphlet_size: usize, num_snapshots: usize) {
        self.num_graphlets += 1;
        self.num_snapshots += num_snapshots;
        self.total_events += graphlet_size as u64;

        // Exponential moving average for graphlet size
        let alpha = 0.1;
        self.avg_graphlet_size =
            alpha * graphlet_size as f64 + (1.0 - alpha) * self.avg_graphlet_size;
    }

    /// Compute the benefit of sharing
    ///
    /// Returns positive if sharing is beneficial, negative if not
    pub fn sharing_benefit(&self) -> f64 {
        let ks = self.num_queries as f64;
        let sp = (self.num_snapshots as f64) / (self.num_graphlets.max(1) as f64);
        let g = self.avg_graphlet_size;

        // Benefit = g² × (ks - sp) - ks × sp
        g * g * (ks - sp) - ks * sp
    }

    /// Should we share based on current statistics?
    pub fn should_share(&self) -> bool {
        // Need at least 2 queries and positive benefit
        self.num_queries > 1 && self.sharing_benefit() > 0.0
    }
}

/// Configuration for the optimizer
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Minimum graphlet size to consider sharing (smaller graphlets have less benefit)
    pub min_graphlet_size: usize,
    /// Minimum queries to consider sharing
    pub min_queries: usize,
    /// Benefit threshold for switching modes
    pub switch_threshold: f64,
    /// Number of graphlets between reevaluation
    pub reevaluate_interval: usize,
    /// Enable adaptive mode switching
    pub adaptive: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            min_graphlet_size: 4,
            min_queries: 2,
            switch_threshold: 100.0,
            reevaluate_interval: 10,
            adaptive: true,
        }
    }
}

/// The Hamlet optimizer
#[derive(Debug)]
pub struct HamletOptimizer {
    /// Configuration
    config: OptimizerConfig,
    /// Stats per Kleene sub-pattern (by event type)
    kleene_stats: rustc_hash::FxHashMap<u16, KleeneStats>,
    /// Current sharing decisions per event type
    sharing_decisions: rustc_hash::FxHashMap<u16, SharingDecision>,
    /// Graphlets since last reevaluation
    graphlets_since_eval: usize,
}

impl HamletOptimizer {
    /// Create a new optimizer
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            config,
            kleene_stats: rustc_hash::FxHashMap::default(),
            sharing_decisions: rustc_hash::FxHashMap::default(),
            graphlets_since_eval: 0,
        }
    }

    /// Register a Kleene sub-pattern
    pub fn register_kleene(&mut self, event_type: u16, queries: &[QueryId]) {
        let stats = KleeneStats::new(event_type, queries.len());
        self.kleene_stats.insert(event_type, stats);

        // Initial decision based on query count
        let initial = if queries.len() >= self.config.min_queries {
            SharingDecision::Shared
        } else {
            SharingDecision::NonShared
        };
        self.sharing_decisions.insert(event_type, initial);
    }

    /// Get the sharing decision for a Kleene type
    pub fn decision(&self, event_type: u16) -> SharingDecision {
        self.sharing_decisions
            .get(&event_type)
            .copied()
            .unwrap_or(SharingDecision::NonShared)
    }

    /// Report graphlet processing for statistics
    pub fn report_graphlet(&mut self, event_type: u16, graphlet_size: usize, num_snapshots: usize) {
        if let Some(stats) = self.kleene_stats.get_mut(&event_type) {
            stats.update(graphlet_size, num_snapshots);
        }

        self.graphlets_since_eval += 1;

        // Reevaluate periodically
        if self.config.adaptive && self.graphlets_since_eval >= self.config.reevaluate_interval {
            self.reevaluate();
            self.graphlets_since_eval = 0;
        }
    }

    /// Reevaluate sharing decisions based on statistics
    fn reevaluate(&mut self) {
        for (&event_type, stats) in &self.kleene_stats {
            let current = self.sharing_decisions.get(&event_type).copied();
            let should_share = stats.should_share()
                && stats.avg_graphlet_size >= self.config.min_graphlet_size as f64;

            let new_decision = match current {
                Some(SharingDecision::Shared) | Some(SharingDecision::Merge) => {
                    if should_share {
                        SharingDecision::Shared
                    } else if stats.sharing_benefit() < -self.config.switch_threshold {
                        SharingDecision::Split
                    } else {
                        SharingDecision::Shared
                    }
                }
                Some(SharingDecision::NonShared) | Some(SharingDecision::Split) => {
                    if should_share && stats.sharing_benefit() > self.config.switch_threshold {
                        SharingDecision::Merge
                    } else {
                        SharingDecision::NonShared
                    }
                }
                None => {
                    if should_share {
                        SharingDecision::Shared
                    } else {
                        SharingDecision::NonShared
                    }
                }
            };

            self.sharing_decisions.insert(event_type, new_decision);
        }
    }

    /// Get statistics for a Kleene type
    pub fn stats(&self, event_type: u16) -> Option<&KleeneStats> {
        self.kleene_stats.get(&event_type)
    }

    /// Get all statistics
    pub fn all_stats(&self) -> impl Iterator<Item = (&u16, &KleeneStats)> {
        self.kleene_stats.iter()
    }

    /// Force a specific sharing decision (for testing/debugging)
    pub fn force_decision(&mut self, event_type: u16, decision: SharingDecision) {
        self.sharing_decisions.insert(event_type, decision);
    }

    /// Reset statistics (e.g., at window boundary)
    pub fn reset_stats(&mut self) {
        for stats in self.kleene_stats.values_mut() {
            stats.num_snapshots = 0;
            stats.num_graphlets = 0;
            stats.total_events = 0;
        }
        self.graphlets_since_eval = 0;
    }
}

impl Default for HamletOptimizer {
    fn default() -> Self {
        Self::new(OptimizerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kleene_stats_benefit() {
        let mut stats = KleeneStats::new(0, 5); // 5 queries

        // Simulate large graphlets with few snapshots
        for _ in 0..10 {
            stats.update(100, 2);
        }

        // With ks=5, g≈100, sp≈2 per graphlet:
        // Benefit = g² × (ks - sp) - ks × sp
        //         = 10000 × (5 - 2) - 5 × 2
        //         = 30000 - 10 = 29990
        assert!(stats.sharing_benefit() > 0.0);
        assert!(stats.should_share());
    }

    #[test]
    fn test_optimizer_decisions() {
        let mut optimizer = HamletOptimizer::new(OptimizerConfig {
            min_graphlet_size: 4,
            min_queries: 2,
            switch_threshold: 100.0,
            reevaluate_interval: 5,
            adaptive: true,
        });

        // Register a Kleene pattern shared by 3 queries
        optimizer.register_kleene(0, &[0, 1, 2]);

        // Initially should be shared
        assert_eq!(optimizer.decision(0), SharingDecision::Shared);

        // Report some graphlets
        for _ in 0..10 {
            optimizer.report_graphlet(0, 50, 1);
        }

        // Should still be shared (large graphlets, few snapshots)
        let decision = optimizer.decision(0);
        assert!(matches!(
            decision,
            SharingDecision::Shared | SharingDecision::Merge
        ));
    }

    #[test]
    fn test_single_query_not_shared() {
        let mut optimizer = HamletOptimizer::default();
        optimizer.register_kleene(0, &[0]); // Only 1 query

        assert_eq!(optimizer.decision(0), SharingDecision::NonShared);
    }
}
