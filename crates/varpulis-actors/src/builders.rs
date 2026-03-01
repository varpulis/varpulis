//! Builder traits for type-safe actor wiring at construction time.

use crate::actor::Actor;
use crate::mailbox::MailboxSender;
use async_trait::async_trait;

/// A sink that accepts messages of type `M`.
///
/// Used for type-safe wiring: a source actor can hold a `Box<dyn MessageSink<M>>`
/// to forward processed messages to the next stage.
#[async_trait]
pub trait MessageSink<M: Send + 'static>: Send + Sync {
    /// Send a message to this sink.
    async fn send(&self, message: M) -> Result<(), crate::mailbox::MailboxError>;
}

/// Blanket implementation: any `MailboxSender<A>` is a `MessageSink` for
/// messages that `A` can receive (i.e., any `Send + 'static` type).
#[async_trait]
impl<A: Actor, M: Send + 'static> MessageSink<M> for MailboxSender<A> {
    async fn send(&self, message: M) -> Result<(), crate::mailbox::MailboxError> {
        Self::send(self, message).await
    }
}

/// A source that produces messages of type `M` given configuration `C`.
///
/// Typically implemented by connector actors that read from external systems.
#[async_trait]
pub trait MessageSource<M: Send + 'static, C = ()>: Send {
    /// Start producing messages, forwarding them to the given sink.
    async fn start(
        &mut self,
        config: C,
        sink: Box<dyn MessageSink<M>>,
    ) -> Result<(), crate::actor::ActorExitStatus>;
}

/// A builder that constructs a value of type `T`.
///
/// Used for configuring actors before spawning them.
pub trait Builder<T> {
    /// The error type returned if building fails.
    type Error: std::error::Error + Send + 'static;

    /// Build the value.
    fn build(self) -> Result<T, Self::Error>;
}
