//! # Hamlet - Shared Online Event Trend Aggregation
//!
//! Implementation of the Hamlet framework from SIGMOD 2021:
//! "To Share, or not to Share: Online Event Trend Aggregation Over Bursty Event Streams"
//!
//! This is **Approach 2b**: Hamlet for structural NFA sharing + ZDD for run set management.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    HAMLET FRAMEWORK                         │
//! │                                                             │
//! │  ┌──────────────────────┐    ┌──────────────────────────┐   │
//! │  │   HAMLET OPTIMIZER    │    │    HAMLET EXECUTOR        │   │
//! │  │                      │    │                          │   │
//! │  │  Compile-time:       │    │  1. Stream partitioning  │   │
//! │  │  - Workload analysis │───▶│     (groupby + panes)    │   │
//! │  │  - Merged query      │    │  2. Hamlet graph         │   │
//! │  │    template (FSA)    │    │     construction         │   │
//! │  │                      │    │  3. Snapshot propagation  │   │
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
