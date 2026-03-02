//! Typed mailbox backed by a bounded `tokio::sync::mpsc` channel.

use std::fmt;

use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::actor::Actor;

/// A command sent through the mailbox, wrapping either a plain message
/// or a request-reply pair.
pub enum Envelope<A: Actor> {
    /// A one-way message with no reply expected.
    Message(Box<dyn std::any::Any + Send>),
    /// A request that expects a reply via the oneshot sender.
    Ask {
        /// The type-erased request message.
        message: Box<dyn std::any::Any + Send>,
        /// Channel to send the reply back to the caller.
        reply_tx: oneshot::Sender<Box<dyn std::any::Any + Send>>,
    },
    /// Internal: observe the actor's current state.
    Observe(oneshot::Sender<A::ObservableState>),
}

impl<A: Actor> fmt::Debug for Envelope<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(_) => write!(f, "Envelope::Message(..)"),
            Self::Ask { .. } => write!(f, "Envelope::Ask(..)"),
            Self::Observe(_) => write!(f, "Envelope::Observe(..)"),
        }
    }
}

/// The receiving half of a mailbox, held by the actor.
pub struct Mailbox<A: Actor> {
    rx: mpsc::Receiver<Envelope<A>>,
}

impl<A: Actor> std::fmt::Debug for Mailbox<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailbox").finish_non_exhaustive()
    }
}

impl<A: Actor> Mailbox<A> {
    /// Receive the next envelope, blocking until one is available.
    ///
    /// Returns `None` when all senders have been dropped.
    pub async fn recv(&mut self) -> Option<Envelope<A>> {
        self.rx.recv().await
    }

    /// Try to receive without blocking.
    pub fn try_recv(&mut self) -> Option<Envelope<A>> {
        self.rx.try_recv().ok()
    }
}

/// The sending half of a mailbox, cheaply cloneable.
pub struct MailboxSender<A: Actor> {
    tx: mpsc::Sender<Envelope<A>>,
}

impl<A: Actor> std::fmt::Debug for MailboxSender<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MailboxSender").finish_non_exhaustive()
    }
}

impl<A: Actor> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<A: Actor> MailboxSender<A> {
    /// Send a message to the actor's mailbox.
    ///
    /// Returns an error if the actor has stopped (channel closed).
    pub async fn send<M: Send + 'static>(&self, message: M) -> Result<(), MailboxError> {
        self.tx
            .send(Envelope::Message(Box::new(message)))
            .await
            .map_err(|_| MailboxError::ActorStopped)
    }

    /// Try to send a message without blocking.
    pub fn try_send<M: Send + 'static>(&self, message: M) -> Result<(), MailboxError> {
        self.tx
            .try_send(Envelope::Message(Box::new(message)))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => MailboxError::Full,
                mpsc::error::TrySendError::Closed(_) => MailboxError::ActorStopped,
            })
    }

    /// Send a message and wait for a reply (request/reply pattern).
    ///
    /// Returns `Err` if the actor has stopped or fails to reply.
    pub async fn ask<M: Send + 'static, R: Send + 'static>(
        &self,
        message: M,
    ) -> Result<R, MailboxError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Envelope::Ask {
                message: Box::new(message),
                reply_tx,
            })
            .await
            .map_err(|_| MailboxError::ActorStopped)?;

        let reply = reply_rx.await.map_err(|_| MailboxError::ReplyDropped)?;
        reply
            .downcast::<R>()
            .map(|r| *r)
            .map_err(|_| MailboxError::TypeMismatch)
    }

    /// Request the actor's observable state.
    pub async fn observe(&self) -> Result<A::ObservableState, MailboxError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Observe(tx))
            .await
            .map_err(|_| MailboxError::ActorStopped)?;
        rx.await.map_err(|_| MailboxError::ReplyDropped)
    }

    /// Check if the actor is still running (channel is open).
    pub fn is_connected(&self) -> bool {
        !self.tx.is_closed()
    }
}

/// Errors that can occur when interacting with a mailbox.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MailboxError {
    /// The target actor has stopped and its mailbox channel is closed.
    #[error("actor has stopped")]
    ActorStopped,
    /// The mailbox is full; the sender should apply backpressure.
    #[error("mailbox is full (backpressure)")]
    Full,
    /// The actor dropped the reply channel without sending a response.
    #[error("reply was dropped before being sent")]
    ReplyDropped,
    /// The reply could not be downcast to the expected type.
    #[error("reply type mismatch")]
    TypeMismatch,
}

/// Create a new mailbox pair with the given capacity.
///
/// The capacity controls backpressure: senders will block when the
/// mailbox is full.
pub fn create_mailbox<A: Actor>(capacity: usize) -> (MailboxSender<A>, Mailbox<A>) {
    let cap = if capacity == 0 {
        warn!("Mailbox capacity of 0 is not allowed, using 1");
        1
    } else {
        capacity
    };
    let (tx, rx) = mpsc::channel(cap);
    (MailboxSender { tx }, Mailbox { rx })
}
