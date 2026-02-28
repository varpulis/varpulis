//! Actor runtime managing spawned actors, graceful shutdown, and lifecycle.

use crate::actor::{Actor, ActorExitStatus};
use crate::context::ActorContext;
use crate::handle::ActorHandle;
use crate::mailbox::create_mailbox;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// The actor runtime manages spawned actors and coordinates shutdown.
pub struct Runtime {
    shutdown: CancellationToken,
    tasks: Vec<(String, JoinHandle<()>)>,
    grace_period: Duration,
}

impl Runtime {
    /// Create a new runtime with the default 10-second grace period.
    pub fn new() -> Self {
        Self {
            shutdown: CancellationToken::new(),
            tasks: Vec::new(),
            grace_period: Duration::from_secs(10),
        }
    }

    /// Create a new runtime with a custom grace period for shutdown.
    pub fn with_grace_period(grace_period: Duration) -> Self {
        Self {
            shutdown: CancellationToken::new(),
            tasks: Vec::new(),
            grace_period,
        }
    }

    /// Get the runtime's cancellation token (for sharing with other systems).
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Spawn an actor and return a handle for observation and messaging.
    ///
    /// The actor is spawned on the tokio runtime and will run until it
    /// exits or the runtime shuts down.
    pub fn spawn<A: Actor>(&mut self, actor: A, mailbox_capacity: usize) -> ActorHandle<A> {
        let name = actor.name().to_string();
        let (sender, mailbox) = create_mailbox::<A>(mailbox_capacity);
        let (exit_tx, exit_rx) = watch::channel(None);

        let mut ctx = ActorContext::new(mailbox, sender.clone(), self.shutdown.clone());
        let actor_name = name.clone();

        let handle = tokio::spawn(async move {
            let result = actor.run(&mut ctx).await;
            let status = match result {
                Ok(()) => ActorExitStatus::Success,
                Err(status) => status,
            };
            info!("Actor '{}' exited: {}", actor_name, status);
            let _ = exit_tx.send(Some(status));
        });

        self.tasks.push((name.clone(), handle));

        ActorHandle::new(name, sender, exit_rx)
    }

    /// Request graceful shutdown of all actors.
    pub fn shutdown(&self) {
        info!("Runtime: initiating graceful shutdown");
        self.shutdown.cancel();
    }

    /// Run until all actors have exited or the grace period expires.
    ///
    /// This is typically called after `shutdown()` or used as the main
    /// entry point for an actor-based application.
    pub async fn run_to_completion(self) {
        // Wait for shutdown signal or all tasks to complete
        let shutdown = self.shutdown.clone();
        let grace = self.grace_period;

        // Install signal handler for SIGTERM / Ctrl+C
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("failed to install SIGTERM handler");
                tokio::select! {
                    _ = ctrl_c => info!("Runtime: received SIGINT"),
                    _ = sigterm.recv() => info!("Runtime: received SIGTERM"),
                }
            }
            #[cfg(not(unix))]
            {
                ctrl_c.await.ok();
                info!("Runtime: received SIGINT");
            }
            shutdown_clone.cancel();
        });

        // Wait for all tasks with grace period
        shutdown.cancelled().await;
        info!(
            "Runtime: shutdown signal received, waiting up to {:?} for actors",
            grace
        );

        let deadline = tokio::time::sleep(grace);
        tokio::pin!(deadline);

        for (name, handle) in self.tasks {
            tokio::select! {
                result = handle => {
                    match result {
                        Ok(()) => info!("Runtime: actor '{}' stopped cleanly", name),
                        Err(e) if e.is_panic() => error!("Runtime: actor '{}' panicked during shutdown", name),
                        Err(e) => warn!("Runtime: actor '{}' join error: {}", name, e),
                    }
                }
                _ = &mut deadline => {
                    warn!("Runtime: grace period expired, actor '{}' still running", name);
                    break;
                }
            }
        }

        info!("Runtime: shutdown complete");
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
