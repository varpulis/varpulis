//! # Snapshot - Key Mechanism for Cross-Query Sharing
//!
//! Snapshots capture intermediate aggregation state at graphlet boundaries,
//! enabling shared propagation through graphlets while maintaining per-query
//! correctness.
//!
//! ## How Snapshots Work
//!
//! When a shared graphlet G_E starts:
//! 1. A snapshot variable `x` is created
//! 2. `x(q)` is set to the sum of counts from G_E's predecessor graphlet for query q
//! 3. Propagation through G_E uses `x` symbolically
//! 4. Final counts are resolved by substituting each query's specific `x(q)` value
//!
//! This allows O(sp × n² + k × sp) complexity instead of O(k × n²),
//! where sp << k when queries share Kleene sub-patterns.

use crate::greta::QueryId;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// Unique identifier for a snapshot
pub type SnapshotId = u32;

/// Snapshot value for a specific query
#[derive(Debug, Clone, Copy)]
pub struct SnapshotValue {
    /// Query this value belongs to
    pub query_id: QueryId,
    /// The actual value (sum of counts from predecessor graphlet)
    pub value: u64,
}

/// A snapshot capturing intermediate state for shared propagation
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Unique identifier
    pub id: SnapshotId,
    /// Values for each query sharing this snapshot
    pub values: SmallVec<[SnapshotValue; 8]>,
    /// Source graphlet ID (where this snapshot originates)
    pub source_graphlet: u32,
    /// Target graphlet ID (where this snapshot is consumed)
    pub target_graphlet: u32,
}

impl Snapshot {
    /// Create a new snapshot
    pub fn new(id: SnapshotId, source_graphlet: u32, target_graphlet: u32) -> Self {
        Self {
            id,
            values: SmallVec::new(),
            source_graphlet,
            target_graphlet,
        }
    }

    /// Set the value for a query
    pub fn set_value(&mut self, query_id: QueryId, value: u64) {
        // Check if query already exists
        for v in &mut self.values {
            if v.query_id == query_id {
                v.value = value;
                return;
            }
        }
        // Add new value
        self.values.push(SnapshotValue { query_id, value });
    }

    /// Get the value for a query
    #[inline]
    pub fn get_value(&self, query_id: QueryId) -> Option<u64> {
        self.values
            .iter()
            .find(|v| v.query_id == query_id)
            .map(|v| v.value)
    }

    /// Get the value for a query, defaulting to 0
    #[inline]
    pub fn value(&self, query_id: QueryId) -> u64 {
        self.get_value(query_id).unwrap_or(0)
    }

    /// Number of queries with values
    #[inline]
    pub fn num_queries(&self) -> usize {
        self.values.len()
    }
}

/// Manager for snapshots
#[derive(Debug, Default)]
pub struct SnapshotManager {
    /// All active snapshots
    snapshots: Vec<Snapshot>,
    /// Next snapshot ID
    next_id: SnapshotId,
    /// Snapshots indexed by target graphlet
    by_target: FxHashMap<u32, SmallVec<[SnapshotId; 4]>>,
    /// Snapshots indexed by source graphlet
    by_source: FxHashMap<u32, SmallVec<[SnapshotId; 4]>>,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new() -> Self {
        Self {
            snapshots: Vec::with_capacity(64),
            next_id: 0,
            by_target: FxHashMap::default(),
            by_source: FxHashMap::default(),
        }
    }

    /// Create a new snapshot
    pub fn create(&mut self, source_graphlet: u32, target_graphlet: u32) -> SnapshotId {
        let id = self.next_id;
        self.next_id += 1;

        let snapshot = Snapshot::new(id, source_graphlet, target_graphlet);
        self.snapshots.push(snapshot);

        self.by_target.entry(target_graphlet).or_default().push(id);

        self.by_source.entry(source_graphlet).or_default().push(id);

        id
    }

    /// Get a snapshot by ID
    #[inline]
    pub fn get(&self, id: SnapshotId) -> Option<&Snapshot> {
        self.snapshots.get(id as usize)
    }

    /// Get a mutable snapshot by ID
    #[inline]
    pub fn get_mut(&mut self, id: SnapshotId) -> Option<&mut Snapshot> {
        self.snapshots.get_mut(id as usize)
    }

    /// Get all snapshots targeting a graphlet
    pub fn snapshots_for_target(&self, target_graphlet: u32) -> impl Iterator<Item = &Snapshot> {
        self.by_target
            .get(&target_graphlet)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|&id| self.snapshots.get(id as usize))
    }

    /// Get all snapshots originating from a graphlet
    pub fn snapshots_from_source(&self, source_graphlet: u32) -> impl Iterator<Item = &Snapshot> {
        self.by_source
            .get(&source_graphlet)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|&id| self.snapshots.get(id as usize))
    }

    /// Clear all snapshots for a processed graphlet
    pub fn clear_for_graphlet(&mut self, graphlet_id: u32) {
        self.by_target.remove(&graphlet_id);
        self.by_source.remove(&graphlet_id);
    }

    /// Clear all snapshots
    pub fn clear(&mut self) {
        self.snapshots.clear();
        self.by_target.clear();
        self.by_source.clear();
        self.next_id = 0;
    }

    /// Number of active snapshots
    #[inline]
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}

/// Propagation coefficients for shared graphlet processing
///
/// In a shared graphlet, each event's count can be expressed as:
/// count(e, q) = coeff(e) * snapshot_value(q) + local_sum(e)
///
/// Where coeff and local_sum are independent of the query.
#[derive(Debug, Clone)]
pub struct PropagationCoefficients {
    /// Coefficient per event (index = local event index in graphlet)
    pub coeffs: Vec<u64>,
    /// Local sum per event
    pub local_sums: Vec<u64>,
}

impl PropagationCoefficients {
    /// Create new coefficients for a graphlet of given size
    pub fn new(size: usize) -> Self {
        Self {
            coeffs: vec![0; size],
            local_sums: vec![0; size],
        }
    }

    /// Compute coefficients for a Kleene graphlet
    ///
    /// For a Kleene pattern with self-loops:
    /// - First event: coeff = 1, local_sum = 0 (count = snapshot_value)
    /// - Event i: coeff = sum of coeffs of all predecessors
    ///   local_sum = sum of local_sums of all predecessors
    pub fn compute_kleene(&mut self) {
        if self.coeffs.is_empty() {
            return;
        }

        // First event receives the snapshot directly
        self.coeffs[0] = 1;
        self.local_sums[0] = 0;

        // Each subsequent event is preceded by all earlier events (Kleene)
        for i in 1..self.coeffs.len() {
            let mut total_coeff = 0u64;
            let mut total_local = 0u64;

            for j in 0..i {
                total_coeff = total_coeff.saturating_add(self.coeffs[j]);
                total_local = total_local.saturating_add(self.local_sums[j]);
            }

            self.coeffs[i] = total_coeff;
            self.local_sums[i] = total_local;
        }
    }

    /// Compute coefficients for a sequence graphlet (no self-loops)
    ///
    /// Each event is preceded only by the immediately previous event
    pub fn compute_sequence(&mut self) {
        if self.coeffs.is_empty() {
            return;
        }

        self.coeffs[0] = 1;
        self.local_sums[0] = 0;

        for i in 1..self.coeffs.len() {
            self.coeffs[i] = self.coeffs[i - 1];
            self.local_sums[i] = self.local_sums[i - 1];
        }
    }

    /// Resolve final count for a query given its snapshot value
    pub fn resolve_count(&self, snapshot_value: u64) -> u64 {
        self.coeffs
            .iter()
            .zip(self.local_sums.iter())
            .map(|(&c, &l)| c.saturating_mul(snapshot_value).saturating_add(l))
            .fold(0u64, |acc, c| acc.saturating_add(c))
    }

    /// Resolve count at a specific event position
    #[inline]
    pub fn resolve_count_at(&self, index: usize, snapshot_value: u64) -> u64 {
        let coeff = self.coeffs.get(index).copied().unwrap_or(0);
        let local_sum = self.local_sums.get(index).copied().unwrap_or(0);
        coeff
            .saturating_mul(snapshot_value)
            .saturating_add(local_sum)
    }

    /// Get sum of final counts (for end events) per query
    pub fn resolve_final_counts(
        &self,
        end_indices: &[usize],
        snapshot: &Snapshot,
    ) -> Vec<PropagationResult> {
        snapshot
            .values
            .iter()
            .map(|sv| {
                let count: u64 = end_indices
                    .iter()
                    .map(|&idx| self.resolve_count_at(idx, sv.value))
                    .sum();

                PropagationResult {
                    query_id: sv.query_id,
                    count,
                }
            })
            .collect()
    }
}

/// Snapshot propagation result for a query
#[derive(Debug, Clone)]
pub struct PropagationResult {
    /// Query ID
    pub query_id: QueryId,
    /// Final count for this query
    pub count: u64,
}

/// Compute final counts by resolving snapshots
///
/// Given a graphlet with computed coefficients and local sums,
/// and the snapshot values for each query, compute the final
/// counts for each query.
pub fn resolve_counts(
    snapshot: &Snapshot,
    coeffs: &[u64],
    local_sums: &[u64],
) -> Vec<PropagationResult> {
    snapshot
        .values
        .iter()
        .map(|sv| {
            let count: u64 = coeffs
                .iter()
                .zip(local_sums.iter())
                .map(|(&c, &l)| c.saturating_mul(sv.value).saturating_add(l))
                .fold(0u64, |acc, c| acc.saturating_add(c));

            PropagationResult {
                query_id: sv.query_id,
                count,
            }
        })
        .collect()
}

/// Merge multiple snapshots into a single snapshot
///
/// This is used when a graphlet has multiple predecessor graphlets,
/// each contributing a snapshot. The merged snapshot sums the values.
pub fn merge_snapshots(snapshots: &[&Snapshot], target_graphlet: u32) -> Snapshot {
    let mut merged = Snapshot::new(0, 0, target_graphlet);

    for snapshot in snapshots {
        for sv in &snapshot.values {
            let current = merged.value(sv.query_id);
            merged.set_value(sv.query_id, current.saturating_add(sv.value));
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_values() {
        let mut snapshot = Snapshot::new(0, 0, 1);

        snapshot.set_value(0, 100);
        snapshot.set_value(1, 200);

        assert_eq!(snapshot.value(0), 100);
        assert_eq!(snapshot.value(1), 200);
        assert_eq!(snapshot.value(2), 0); // Non-existent query
    }

    #[test]
    fn test_snapshot_manager() {
        let mut manager = SnapshotManager::new();

        let s1 = manager.create(0, 1);
        let s2 = manager.create(0, 1);
        let s3 = manager.create(1, 2);

        assert_eq!(s1, 0);
        assert_eq!(s2, 1);
        assert_eq!(s3, 2);

        // Check snapshots for target
        let targets: Vec<_> = manager.snapshots_for_target(1).collect();
        assert_eq!(targets.len(), 2);

        // Check snapshots from source
        let sources: Vec<_> = manager.snapshots_from_source(0).collect();
        assert_eq!(sources.len(), 2);
    }

    #[test]
    fn test_propagation_coefficients_kleene() {
        let mut coeffs = PropagationCoefficients::new(4);
        coeffs.compute_kleene();

        // For Kleene with 4 events:
        // e0: coeff=1, local=0
        // e1: coeff=1 (sum of e0), local=0
        // e2: coeff=2 (sum of e0+e1), local=0
        // e3: coeff=4 (sum of e0+e1+e2), local=0
        assert_eq!(coeffs.coeffs, vec![1, 1, 2, 4]);
        assert_eq!(coeffs.local_sums, vec![0, 0, 0, 0]);

        // With snapshot value 1:
        // Total count = 1*1 + 1*1 + 2*1 + 4*1 = 8
        assert_eq!(coeffs.resolve_count(1), 8);

        // With snapshot value 2:
        // Total count = 1*2 + 1*2 + 2*2 + 4*2 = 16
        assert_eq!(coeffs.resolve_count(2), 16);
    }

    #[test]
    fn test_propagation_coefficients_sequence() {
        let mut coeffs = PropagationCoefficients::new(3);
        coeffs.compute_sequence();

        // For sequence: coeff stays 1 throughout
        assert_eq!(coeffs.coeffs, vec![1, 1, 1]);

        // With snapshot value 5:
        // Total count = 1*5 + 1*5 + 1*5 = 15
        assert_eq!(coeffs.resolve_count(5), 15);
    }

    #[test]
    fn test_resolve_counts() {
        let mut snapshot = Snapshot::new(0, 0, 1);
        snapshot.set_value(0, 2); // Query 0 has snapshot value 2
        snapshot.set_value(1, 3); // Query 1 has snapshot value 3

        // coeffs and local_sums for 3 events
        let coeffs = vec![1, 1, 2]; // e0: 1, e1: 1, e2: 2
        let local_sums = vec![0, 0, 0];

        let results = resolve_counts(&snapshot, &coeffs, &local_sums);

        // Query 0: (1*2 + 0) + (1*2 + 0) + (2*2 + 0) = 2 + 2 + 4 = 8
        assert_eq!(results[0].count, 8);

        // Query 1: (1*3 + 0) + (1*3 + 0) + (2*3 + 0) = 3 + 3 + 6 = 12
        assert_eq!(results[1].count, 12);
    }

    #[test]
    fn test_merge_snapshots() {
        let mut s1 = Snapshot::new(0, 0, 2);
        s1.set_value(0, 10);
        s1.set_value(1, 20);

        let mut s2 = Snapshot::new(1, 1, 2);
        s2.set_value(0, 5);
        s2.set_value(1, 15);

        let merged = merge_snapshots(&[&s1, &s2], 2);

        assert_eq!(merged.value(0), 15); // 10 + 5
        assert_eq!(merged.value(1), 35); // 20 + 15
    }
}
