//! IDX-03: the runs of one partition under `partition by`.
//!
//! An event visits only the runs of its own partition. When many of them
//! wait there (a low-cardinality key, or many orders waiting under the same
//! key), visiting them all costs one predicate evaluation per run, as the
//! non-partitioned list did before the predicate index. Each partition can
//! therefore hold its own index, under the same thresholds as the
//! non-partitioned one, counted on the partition's runs: a partition of two
//! runs never builds one, a partition of a thousand does.

use crate::run::Run;
use crate::run_index::RunIndex;

/// The runs of one partition: those every event of the partition visits,
/// and those its predicate index holds.
#[derive(Debug, Default)]
pub struct Partition {
    /// The runs every event of this partition visits.
    pub runs: Vec<Run>,
    /// Runs filed by the values their next step compares against, while the
    /// partition holds enough runs for it (`SaseEngine::with_predicate_index`).
    /// Boxed: most partitions never have one, and a partition is moved each
    /// time the map of partitions takes it in or lets it go.
    pub(crate) index: Option<Box<RunIndex>>,
}

impl From<Vec<Run>> for Partition {
    fn from(runs: Vec<Run>) -> Self {
        Self { runs, index: None }
    }
}

impl Partition {
    /// Every run of the partition, visited or parked.
    pub fn len(&self) -> usize {
        self.runs.len() + self.parked_run_count()
    }

    /// No run left, visited or parked.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty() && self.parked_run_count() == 0
    }

    /// Number of runs held by the partition's index.
    pub fn parked_run_count(&self) -> usize {
        self.index.as_ref().map_or(0, |index| index.len())
    }

    /// The runs held by the partition's index, in no particular order. A
    /// checkpoint must include them: they are not in `runs`.
    pub fn parked_runs(&self) -> impl Iterator<Item = &Run> {
        self.index.iter().flat_map(|index| index.runs())
    }

    /// Every run of the partition, the visited ones first, in no particular
    /// order.
    pub fn all_runs(&self) -> impl Iterator<Item = &Run> {
        self.runs.iter().chain(self.parked_runs())
    }

    /// File `run` in the index if it can be, else with the visited runs.
    pub(crate) fn file_run(&mut self, run: Run) {
        if let Some(index) = self.index.as_mut() {
            if let Some(key) = index.key_for(&run) {
                index.insert(run, key);
                return;
            }
        }
        self.runs.push(run);
    }

    /// Put the index's runs back with the visited runs and drop the index.
    pub(crate) fn release_index(&mut self) {
        if let Some(mut index) = self.index.take() {
            self.runs.extend(index.drain());
        }
    }

    /// Take out the run with the least `key` among those `spared` does not
    /// protect, visited or parked. The parked runs are compared where they
    /// are, so evicting does not take the index apart.
    pub(crate) fn remove_min_by_key_except<K: Ord>(
        &mut self,
        key: impl Fn(&Run) -> K,
        spared: impl Fn(&Run) -> bool,
    ) -> Option<Run> {
        let listed = self
            .runs
            .iter()
            .enumerate()
            .filter(|(_, r)| !spared(r))
            .map(|(i, r)| (key(r), i))
            .min_by(|a, b| a.0.cmp(&b.0));
        // A parked run is never a member of an `or` family, which is all
        // `spared` protects; it is checked anyway.
        let parked = self.index.as_ref().and_then(|ix| {
            ix.min_by_key(|r| (spared(r), key(r)))
                .filter(|((spared, _), _)| !spared)
                .map(|((_, k), slot)| (k, slot))
        });
        match (listed, parked) {
            (Some((lk, _)), Some((pk, slot))) if pk < lk => Some(self.index.as_mut()?.remove(slot)),
            (Some((_, i)), _) => Some(self.runs.swap_remove(i)),
            (None, Some((_, slot))) => Some(self.index.as_mut()?.remove(slot)),
            (None, None) => None,
        }
    }
}
