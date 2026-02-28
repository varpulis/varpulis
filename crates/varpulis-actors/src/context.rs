//! Actor execution context providing mailbox access and shutdown signaling.

use crate::actor::Actor;
use crate::mailbox::{Mailbox, MailboxSender};
use tokio_util::sync::CancellationToken;

/// Execution context provided to each actor during its `run` loop.
///
/// The context is split into two independent parts to avoid borrow conflicts
/// in `tokio::select!`:
///
/// - `mailbox` — mutable receiver for incoming envelopes
/// - `shutdown` — cloneable cancellation token (shared-ref `cancelled()`)
///
/// # Usage
///
/// ```rust,ignore
/// loop {
///     tokio::select! {
///         _ = ctx.shutdown.cancelled() => return Ok(()),
///         msg = ctx.mailbox.recv() => { /* handle */ }
///     }
/// }
/// ```
pub struct ActorContext<A: Actor> {
    /// The actor's mailbox receiver. Use `mailbox.recv()` to get the next envelope.
    pub mailbox: Mailbox<A>,
    /// Cancellation token for graceful shutdown.
    /// Use `shutdown.cancelled().await` in a `tokio::select!` branch.
    pub shutdown: CancellationToken,
    self_sender: MailboxSender<A>,
}

impl<A: Actor> ActorContext<A> {
    /// Create a new actor context.
    pub(crate) fn new(
        mailbox: Mailbox<A>,
        self_sender: MailboxSender<A>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            mailbox,
            shutdown,
            self_sender,
        }
    }

    /// Check if shutdown has been requested (non-blocking).
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Get a sender to this actor's own mailbox (for self-scheduling).
    pub fn self_sender(&self) -> &MailboxSender<A> {
        &self.self_sender
    }

    /// Get a child cancellation token (for passing to child tasks).
    pub fn child_token(&self) -> CancellationToken {
        self.shutdown.child_token()
    }
}
