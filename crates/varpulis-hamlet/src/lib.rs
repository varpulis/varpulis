//! # Hamlet - Shared Online Event Trend Aggregation
//!
//! Multi-query optimization for online trend aggregation over bursty event streams.
//!
//! ## References
//!
//! This implementation is based on:
//!
//! > **Olga Poppe, Allison Rozet, Chuan Lei, Elke A. Rundensteiner, and David Maier.**
//! > *To Share or Not to Share: Online Event Trend Aggregation Over Bursty Event Streams.*
//! > Proceedings of the 2021 International Conference on Management of Data (SIGMOD '21),
//! > pp. 1453-1465, 2021.
//! > DOI: [10.1145/3448016.3457310](https://doi.org/10.1145/3448016.3457310)
//!
//! Hamlet builds upon the GRETA framework:
//!
//! > **Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier.**
//! > *GRETA: Graph-based Real-time Event Trend Aggregation.*
//! > Proceedings of the VLDB Endowment, Vol. 11, No. 1, pp. 80-92, 2017.
//! > DOI: [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)

pub mod aggregator;
pub mod graph;
pub mod graphlet;
pub mod optimizer;
pub mod snapshot;
pub mod template;

/// GRETA framework types used by Hamlet.
///
/// These are lightweight type aliases and enums mirroring the core GRETA
/// types to avoid coupling Hamlet to the full runtime.
pub mod greta {
    /// Query identifier.
    pub type QueryId = u32;
    /// Node identifier in the event graph.
    pub type NodeId = u32;

    /// Aggregation function for GRETA queries.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum GretaAggregate {
        /// COUNT(*) - number of trends
        CountTrends,
        /// COUNT(E) - number of events of type E in all trends
        CountEvents(u16),
        /// SUM(E.attr)
        Sum {
            /// Event type index in the pattern.
            type_index: u16,
            /// Field index within the event.
            field_index: u16,
        },
        /// AVG(E.attr)
        Avg {
            /// Event type index in the pattern.
            type_index: u16,
            /// Field index within the event.
            field_index: u16,
        },
        /// MIN(E.attr)
        Min {
            /// Event type index in the pattern.
            type_index: u16,
            /// Field index within the event.
            field_index: u16,
        },
        /// MAX(E.attr)
        Max {
            /// Event type index in the pattern.
            type_index: u16,
            /// Field index within the event.
            field_index: u16,
        },
    }
}

pub use aggregator::{HamletAggregator, HamletConfig, QueryRegistration};
pub use graph::HamletGraph;
pub use graphlet::{Graphlet, GraphletId, GraphletStatus};
pub use optimizer::{HamletOptimizer, SharingDecision};
pub use snapshot::{Snapshot, SnapshotId, SnapshotValue};
pub use template::{MergedTemplate, TemplateState, TemplateTransition};
