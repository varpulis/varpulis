//! Core actor traits defining lifecycle and message handling.

use async_trait::async_trait;
use serde::Serialize;
use std::fmt::Debug;

use crate::context::ActorContext;

/// Exit status returned when an actor's `run` loop completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorExitStatus {
    /// The actor completed successfully.
    Success,
    /// The actor was asked to shut down gracefully.
    Quit,
    /// The actor encountered a recoverable failure.
    Failure(String),
    /// The actor panicked (set by the runtime, not the actor itself).
    Panicked,
}

impl std::fmt::Display for ActorExitStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Quit => write!(f, "quit"),
            Self::Failure(msg) => write!(f, "failure: {msg}"),
            Self::Panicked => write!(f, "panicked"),
        }
    }
}

impl std::error::Error for ActorExitStatus {}

/// The core actor trait.
///
/// An actor is a concurrent unit of computation that processes messages
/// through a mailbox and exposes observable state for health monitoring.
///
/// # Example
///
/// ```rust,ignore
/// use varpulis_actors::{Actor, ActorContext, ActorExitStatus};
///
/// struct Counter { count: u64 }
///
/// #[async_trait::async_trait]
/// impl Actor for Counter {
///     type ObservableState = u64;
///
///     fn name(&self) -> &str { "counter" }
///
///     fn observable_state(&self) -> Self::ObservableState { self.count }
///
///     async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
///         loop {
///             tokio::select! {
///                 _ = ctx.shutdown_signal() => return Ok(()),
///                 msg = ctx.recv() => {
///                     match msg {
///                         Some(_) => self.count += 1,
///                         None => return Ok(()),
///                     }
///                 }
///             }
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Actor: Send + 'static {
    /// The type exposed for external observation (health checks, debugging).
    ///
    /// Must be cheaply cloneable — typically a small struct or primitive.
    type ObservableState: Debug + Serialize + Send + Sync + Clone;

    /// A human-readable name for this actor instance.
    fn name(&self) -> &str;

    /// Snapshot of the actor's current state for health monitoring.
    fn observable_state(&self) -> Self::ObservableState;

    /// The actor's main execution loop.
    ///
    /// The actor should select on `ctx.recv()` for incoming messages and
    /// `ctx.shutdown_signal()` for graceful shutdown.
    async fn run(self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus>
    where
        Self: Sized;
}

/// Trait for actors that handle a specific message type.
///
/// Unlike the `run`-based [`Actor`] trait, `Handler` provides a simpler
/// request/reply interface for message-driven actors.
#[async_trait]
pub trait Handler<M: crate::message::Message>: Actor {
    /// The reply type sent back to the caller.
    type Reply: Send + 'static;

    /// Handle a single message, optionally producing a reply.
    async fn handle(
        &mut self,
        message: M,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus>
    where
        Self: Sized;
}
