//! # ZDD Unified - ZDD as Unified Representation for Multi-Query Aggregation
//!
//! Experimental approach using Zero-suppressed Decision Diagrams (ZDDs) for
//! automatic sharing in multi-query event trend aggregation.
//!
//! ## References
//!
//! This implementation explores using ZDDs for NFA state representation, based on:
//!
//! > **Shin-ichi Minato.**
//! > *Zero-Suppressed BDDs for Set Manipulation in Combinatorial Problems.*
//! > Proceedings of the 30th ACM/IEEE Design Automation Conference, pp. 272-277, 1993.
//! > DOI: [10.1145/157485.164890](https://doi.org/10.1145/157485.164890)
//!
//! The aggregation concepts come from:
//!
//! > **Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier.**
//! > *GRETA: Graph-based Real-time Event Trend Aggregation.*
//! > Proceedings of the VLDB Endowment, Vol. 11, No. 1, pp. 80-92, 2017.
//! > DOI: [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)
//!
//! ## Overview
//!
//! This approach encodes NFA states as ZDD variables, with the hypothesis that
//! ZDD's canonical reduction would automatically share common substructures
//! between queries, similar to how Hamlet shares graphlets manually.
//!
//! ## Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │                   ZDD UNIFIED FRAMEWORK                        │
//! │                                                                │
//! │  ┌─────────────────────┐     ┌─────────────────────────────┐   │
//! │  │   NFA → ZDD ENCODER │     │      ZDD AGGREGATOR         │   │
//! │  │                     │     │                             │   │
//! │  │  - States as ZDD    │────▶│  1. Event → ZDD update      │   │
//! │  │    variables        │     │  2. Apply ops for           │   │
//! │  │  - Transitions as   │     │     transitions             │   │
//! │  │    ZDD families     │     │  3. Count via traversal     │   │
//! │  │  - Canonical        │     │  4. Automatic sharing       │   │
//! │  │    reduction        │     │     via reduction           │   │
//! │  └─────────────────────┘     └─────────────────────────────┘   │
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Concepts
//!
//! - **NFA State Encoding**: Each NFA state becomes a ZDD variable
//! - **Transition Sets**: Sets of (state, event_type) pairs as ZDD families
//! - **Canonical Sharing**: ZDD reduction automatically shares common sub-patterns
//! - **Count Propagation**: Traversal with accumulators
//!
//! ## Performance Analysis
//!
//! **This approach is NOT recommended for production use.**
//!
//! Benchmarks show that while ZDD provides elegant automatic sharing, the
//! per-operation cost O(ZDD size) is too high compared to Hamlet's O(1) transitions:
//!
//! | Queries | Hamlet | ZDD Unified | Hamlet Speedup |
//! |---------|--------|-------------|----------------|
//! | 1       | 6.9 M/s | 2.4 M/s    | 3x             |
//! | 10      | 2.1 M/s | 122 K/s    | 17x            |
//! | 50      | 0.95 M/s | 9 K/s     | 100x           |
//!
//! The ZDD approach is kept for research purposes and as a reference
//! implementation. For production, use the [`hamlet`](crate::hamlet) module.
//!
//! ## Trade-offs vs Hamlet
//!
//! | Aspect | ZDD Unified | Hamlet |
//! |--------|-------------|--------|
//! | Sharing | Automatic (canonical) | Manual (graphlets) |
//! | Transition cost | O(ZDD size) | O(1) |
//! | Implementation | Simpler | More complex |
//! | Performance | Poor at scale | Excellent at scale |
//!
//! ## Modules
//!
//! - [`nfa_zdd`]: NFA structure encoded in ZDD
//! - [`propagation`]: Count propagation via ZDD traversal
//! - [`aggregator`]: Main aggregator using ZDD operations

pub mod aggregator;
pub mod nfa_zdd;
pub mod propagation;

pub use aggregator::{ZddAggregator, ZddConfig, ZddQueryRegistration};
pub use nfa_zdd::{NfaZdd, ZddState, ZddTransition};
pub use propagation::{ZddPropagator, PropagationMode};
