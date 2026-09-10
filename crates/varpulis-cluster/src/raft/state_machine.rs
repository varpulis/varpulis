//! Replicated coordinator state and command application logic.
//!
//! The state and the `apply` function are **backend-agnostic** and live in
//! [`crate::control_state`]; the `jetstream-control-plane` backend applies the
//! very same [`ClusterCommand`](crate::control_state::ClusterCommand) set to
//! the very same [`CoordinatorState`] over a JetStream KV bucket. This module
//! re-exports them under their historical `raft::state_machine::*` paths so
//! existing callers and tests are unaffected.

pub use crate::control_state::{
    apply_command, CoordinatorState, GroupCheckpointStatus, WorkerEntry,
};
