//! # ZDD Unified - ZDD as Unified Representation for Multi-Query Aggregation
//!
//! Implementation of Approach 2a: encoding NFA structure and sharing directly in ZDD.
//!
//! ## Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │                   ZDD UNIFIED FRAMEWORK                        │
//! │                                                                │
//! │  ┌─────────────────────┐     ┌─────────────────────────────┐   │
//! │  │   NFA → ZDD ENCODER  │     │      ZDD AGGREGATOR         │   │
//! │  │                     │     │                             │   │
//! │  │  - States as ZDD    │────▶│  1. Event → ZDD update      │   │
//! │  │    variables        │     │  2. Apply ops for           │   │
//! │  │  - Transitions as   │     │     transitions             │   │
//! │  │    ZDD families     │     │  3. Count via traversal     │   │
//! │  │  - Canonical        │     │  4. Automatic sharing       │   │
//! │  │    reduction        │     │     via reduction           │   │
//! │  └─────────────────────┘     └─────────────────────────────┘   │
//! │                                                                │
//! │  Key Insight: ZDD canonical reduction provides Hamlet-style    │
//! │  sharing "for free" - common substructures automatically       │
//! │  merged during ZDD operations.                                 │
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Concepts
//!
//! - **NFA State Encoding**: Each NFA state becomes a ZDD variable
//! - **Transition Sets**: Sets of (state, event_type) pairs as ZDD families
//! - **Canonical Sharing**: ZDD reduction automatically shares common sub-patterns
//! - **Count Propagation**: Traversal with accumulators (more expensive than direct)
//!
//! ## Trade-offs vs Hamlet
//!
//! | Aspect | ZDD Unified | Hamlet + ZDD |
//! |--------|-------------|--------------|
//! | Sharing | Automatic (canonical) | Manual (graphlets) |
//! | Transition cost | O(ZDD size) | O(1) |
//! | Implementation | Simpler | More complex |
//! | Flexibility | Less | More |
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
