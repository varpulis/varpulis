//! Actor handle for external observation and health checking.

use crate::actor::{Actor, ActorExitStatus};
use crate::mailbox::{MailboxError, MailboxSender};
use serde::Serialize;
use tokio::sync::watch;

/// Health status of an actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    /// The actor is running normally.
    Healthy,
    /// The actor is running but may be degraded.
    Degraded {
        /// Human-readable description of why the actor is degraded.
        reason: String,
    },
    /// The actor has stopped.
    Down {
        /// The exit status of the stopped actor as a human-readable string.
        exit_status: String,
    },
}

/// A handle to a running actor, used for observation and health checks.
///
/// Handles are cheap to clone and can be shared across tasks.
pub struct ActorHandle<A: Actor> {
    name: String,
    sender: MailboxSender<A>,
    exit_rx: watch::Receiver<Option<ActorExitStatus>>,
}

impl<A: Actor> Clone for ActorHandle<A> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            sender: self.sender.clone(),
            exit_rx: self.exit_rx.clone(),
        }
    }
}

impl<A: Actor> ActorHandle<A> {
    /// Create a new actor handle.
    pub(crate) fn new(
        name: String,
        sender: MailboxSender<A>,
        exit_rx: watch::Receiver<Option<ActorExitStatus>>,
    ) -> Self {
        Self {
            name,
            sender,
            exit_rx,
        }
    }

    /// The actor's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Request the actor's current observable state.
    ///
    /// Returns `Err` if the actor has stopped.
    pub async fn observe(&self) -> Result<A::ObservableState, MailboxError> {
        self.sender.observe().await
    }

    /// Check the actor's health.
    pub fn check_health(&self) -> Health {
        if let Some(exit_status) = self.exit_rx.borrow().as_ref() {
            return Health::Down {
                exit_status: exit_status.to_string(),
            };
        }
        if self.sender.is_connected() {
            Health::Healthy
        } else {
            Health::Down {
                exit_status: "channel closed".to_string(),
            }
        }
    }

    /// Send a message to the actor.
    pub async fn send<M: Send + 'static>(&self, message: M) -> Result<(), MailboxError> {
        self.sender.send(message).await
    }

    /// Send a message and await a reply.
    pub async fn ask<M: Send + 'static, R: Send + 'static>(
        &self,
        message: M,
    ) -> Result<R, MailboxError> {
        self.sender.ask::<M, R>(message).await
    }

    /// Get a reference to the underlying mailbox sender.
    pub fn sender(&self) -> &MailboxSender<A> {
        &self.sender
    }

    /// Wait for the actor to exit.
    pub async fn wait_for_exit(&mut self) -> ActorExitStatus {
        loop {
            self.exit_rx.changed().await.ok();
            if let Some(status) = self.exit_rx.borrow().as_ref() {
                return status.clone();
            }
        }
    }
}
