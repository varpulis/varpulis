#![warn(missing_docs)]
//! # varpulis-actors
//!
//! A lightweight actor framework for Varpulis providing:
//!
//! - **Actor lifecycle** — spawn, run, and shut down actors with typed mailboxes
//! - **Supervision** — automatic restart with configurable policies
//! - **Health observation** — query actor state without stopping it
//! - **Type-safe wiring** — connect actors via `MessageSink`/`MessageSource` traits
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌──────────┐     ┌─────────────┐
//! │  Producer    │────>│ Mailbox  │────>│   Actor     │
//! │  (Sender)   │     │ (mpsc)   │     │  (run loop) │
//! └─────────────┘     └──────────┘     └─────────────┘
//!                                             │
//!                                      ┌──────┴──────┐
//!                                      │  Observable  │
//!                                      │    State     │
//!                                      └─────────────┘
//!
//!  Supervisor wraps an actor with restart logic:
//!
//! ┌───────────────────────────────────────┐
//! │            Supervisor                 │
//! │  ┌───────┐  restart   ┌───────┐     │
//! │  │ Actor │ ────────> │ Actor │ ... │
//! │  │ (v1)  │  on fail   │ (v2)  │     │
//! │  └───────┘            └───────┘     │
//! └───────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use varpulis_actors::*;
//!
//! struct PingPong { received: u64 }
//!
//! #[async_trait::async_trait]
//! impl Actor for PingPong {
//!     type ObservableState = u64;
//!     fn name(&self) -> &str { "ping-pong" }
//!     fn observable_state(&self) -> u64 { self.received }
//!
//!     async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
//!         loop {
//!             tokio::select! {
//!                 _ = ctx.shutdown_signal() => return Ok(()),
//!                 msg = ctx.recv_mut() => match msg {
//!                     Some(_) => self.received += 1,
//!                     None => return Ok(()),
//!                 }
//!             }
//!         }
//!     }
//! }
//! ```

pub mod actor;
pub mod builders;
pub mod context;
pub mod handle;
pub mod mailbox;
pub mod message;
pub mod runtime;
pub mod supervisor;

// Re-exports for convenience
pub use actor::{Actor, ActorExitStatus, Handler};
pub use builders::{Builder, MessageSink, MessageSource};
pub use context::ActorContext;
pub use handle::{ActorHandle, Health};
pub use mailbox::{create_mailbox, Mailbox, MailboxError, MailboxSender};
pub use runtime::Runtime;
pub use supervisor::{RestartPolicy, Supervisor, SupervisorConfig, SupervisorMetrics};
