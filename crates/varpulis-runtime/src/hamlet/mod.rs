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
//!
//! ## Overview
//!
//! Hamlet addresses the challenge of efficiently processing multiple trend aggregation
//! queries that share common sub-patterns. The key insight is that queries with
//! overlapping Kleene patterns can share intermediate computation through:
//!
//! 1. **Graphlets**: Sub-graphs containing events of the same type
//! 2. **Snapshots**: Intermediate state captured at graphlet boundaries
//! 3. **Propagation Coefficients**: Query-independent computation within graphlets
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    HAMLET FRAMEWORK                         │
//! │                                                             │
//! │  ┌──────────────────────┐    ┌──────────────────────────┐   │
//! │  │   HAMLET OPTIMIZER   │    │    HAMLET EXECUTOR       │   │
//! │  │                      │    │                          │   │
//! │  │  Compile-time:       │    │  1. Stream partitioning  │   │
//! │  │  - Workload analysis │───▶│     (groupby + panes)    │   │
//! │  │  - Merged query      │    │  2. Hamlet graph         │   │
//! │  │    template (FSA)    │    │     construction         │   │
//! │  │                      │    │  3. Snapshot propagation │   │
//! │  │  Runtime:            │    │  4. Shared/non-shared    │   │
//! │  │  - Sharing benefit   │◀──▶│     execution switching  │   │
//! │  │    estimation        │    │                          │   │
//! │  │  - Split/merge       │    │                          │   │
//! │  │    decisions         │    │                          │   │
//! │  └──────────────────────┘    └──────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Concepts
//!
//! - **Graphlet**: Sub-graph of events of the same type within a time interval
//! - **Snapshot**: Captures intermediate state at graphlet boundaries for sharing
//! - **Merged Query Template**: FSA combining multiple queries with labeled transitions
//! - **Dynamic Optimizer**: Runtime split/merge decisions based on benefit estimation
//! - **Propagation Coefficients**: `count(e) = coeff × snapshot + local_sum`
//!
//! ## Complexity
//!
//! For k queries sharing a Kleene pattern of length n:
//! - **Non-shared**: O(k × n²)
//! - **Shared (Hamlet)**: O(sp × n² + k × sp) where sp is the number of shared graphlets
//!
//! When sp << k, Hamlet achieves significant speedups.
//!
//! ## Performance
//!
//! Benchmarks show Hamlet outperforms the alternative ZDD-based approach by 3x-100x
//! depending on query count. See `benches/hamlet_zdd_benchmark.rs`.
//!
//! ## Modules
//!
//! - [`graph`]: Hamlet graph structure (graphlets, edges)
//! - [`graphlet`]: Graphlet management and sharing
//! - [`snapshot`]: Snapshot mechanism for cross-query sharing
//! - [`template`]: Merged query template (compiled FSA)
//! - [`optimizer`]: Dynamic sharing optimizer
//! - [`aggregator`]: Online trend aggregation with sharing

pub mod aggregator;
pub mod graph;
pub mod graphlet;
pub mod optimizer;
pub mod snapshot;
pub mod template;

pub use aggregator::{HamletAggregator, HamletConfig, QueryRegistration};
pub use graph::HamletGraph;
pub use graphlet::{Graphlet, GraphletId, GraphletStatus};
pub use optimizer::{HamletOptimizer, SharingDecision};
pub use snapshot::{Snapshot, SnapshotId, SnapshotValue};
pub use template::{MergedTemplate, TemplateState, TemplateTransition};
