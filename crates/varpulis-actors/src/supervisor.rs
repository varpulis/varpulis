//! Supervision tree for automatic actor restart and health monitoring.

use crate::actor::{Actor, ActorExitStatus};
use crate::context::ActorContext;
use crate::mailbox::create_mailbox;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Policy controlling when a supervised actor is restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    /// Always restart, regardless of exit status.
    Always,
    /// Restart only on failure or panic.
    OnFailure,
    /// Never restart — let the actor stay down.
    Never,
}

/// Metrics tracked by the supervisor.
#[derive(Debug, Clone, Serialize)]
pub struct SupervisorMetrics {
    pub restart_count: u64,
    pub panic_count: u64,
    pub uptime_secs: u64,
    pub last_restart_reason: Option<String>,
}

/// Configuration for a supervisor.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Restart policy.
    pub restart_policy: RestartPolicy,
    /// Maximum number of restarts before giving up.
    pub max_restarts: u32,
    /// Time window for counting restarts (resets after this duration).
    pub restart_window: Duration,
    /// Delay between restarts (with exponential backoff).
    pub base_restart_delay: Duration,
    /// Mailbox capacity for the supervised actor.
    pub mailbox_capacity: usize,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            restart_policy: RestartPolicy::OnFailure,
            max_restarts: 5,
            restart_window: Duration::from_secs(60),
            base_restart_delay: Duration::from_millis(100),
            mailbox_capacity: 100,
        }
    }
}

/// A supervisor that manages the lifecycle of a single actor.
///
/// The supervisor spawns the actor, monitors its health, and restarts
/// it according to the configured [`RestartPolicy`].
pub struct Supervisor<F> {
    name: String,
    factory: F,
    config: SupervisorConfig,
    restart_count: Arc<AtomicU64>,
    panic_count: Arc<AtomicU64>,
    started_at: Instant,
}

impl<A, F> Supervisor<F>
where
    A: Actor,
    F: Fn() -> A + Send + 'static,
{
    /// Create a new supervisor with the given actor factory and config.
    pub fn new(name: impl Into<String>, factory: F, config: SupervisorConfig) -> Self {
        Self {
            name: name.into(),
            factory,
            config,
            restart_count: Arc::new(AtomicU64::new(0)),
            panic_count: Arc::new(AtomicU64::new(0)),
            started_at: Instant::now(),
        }
    }

    /// Get current supervisor metrics.
    pub fn metrics(&self) -> SupervisorMetrics {
        SupervisorMetrics {
            restart_count: self.restart_count.load(Ordering::Relaxed),
            panic_count: self.panic_count.load(Ordering::Relaxed),
            uptime_secs: self.started_at.elapsed().as_secs(),
            last_restart_reason: None,
        }
    }

    /// Run the supervision loop.
    ///
    /// This spawns the actor and restarts it according to the policy.
    /// Returns when the actor exits cleanly or the restart limit is reached.
    pub async fn run(self, shutdown: CancellationToken) -> ActorExitStatus {
        let mut restarts_in_window = 0u32;
        let mut window_start = Instant::now();

        loop {
            // Reset window counter if enough time has passed
            if window_start.elapsed() > self.config.restart_window {
                restarts_in_window = 0;
                window_start = Instant::now();
            }

            let actor = (self.factory)();
            let actor_name = actor.name().to_string();

            info!(
                "Supervisor '{}': spawning actor '{}'",
                self.name, actor_name
            );

            let (sender, mailbox) = create_mailbox::<A>(self.config.mailbox_capacity);
            let mut ctx = ActorContext::new(mailbox, sender.clone(), shutdown.clone());

            // Run the actor in a spawned task to catch panics
            let exit_status = tokio::spawn(async move { actor.run(&mut ctx).await }).await;

            let status = match exit_status {
                Ok(Ok(())) => ActorExitStatus::Success,
                Ok(Err(status)) => status,
                Err(join_err) => {
                    self.panic_count.fetch_add(1, Ordering::Relaxed);
                    if join_err.is_panic() {
                        error!(
                            "Supervisor '{}': actor '{}' panicked",
                            self.name, actor_name
                        );
                        ActorExitStatus::Panicked
                    } else {
                        error!(
                            "Supervisor '{}': actor '{}' was cancelled",
                            self.name, actor_name
                        );
                        ActorExitStatus::Failure("task cancelled".to_string())
                    }
                }
            };

            info!(
                "Supervisor '{}': actor '{}' exited with: {}",
                self.name, actor_name, status
            );

            // Check if we should restart
            let should_restart = match self.config.restart_policy {
                RestartPolicy::Always => true,
                RestartPolicy::OnFailure => matches!(
                    status,
                    ActorExitStatus::Failure(_) | ActorExitStatus::Panicked
                ),
                RestartPolicy::Never => false,
            };

            if !should_restart {
                return status;
            }

            if shutdown.is_cancelled() {
                info!(
                    "Supervisor '{}': shutdown requested, not restarting",
                    self.name
                );
                return status;
            }

            restarts_in_window += 1;
            self.restart_count.fetch_add(1, Ordering::Relaxed);

            if restarts_in_window > self.config.max_restarts {
                error!(
                    "Supervisor '{}': max restarts ({}) exceeded in {:?} window, giving up",
                    self.name, self.config.max_restarts, self.config.restart_window
                );
                return ActorExitStatus::Failure(format!(
                    "max restarts ({}) exceeded",
                    self.config.max_restarts
                ));
            }

            // Exponential backoff
            let delay = self.config.base_restart_delay * 2u32.pow((restarts_in_window - 1).min(5));
            warn!(
                "Supervisor '{}': restarting actor '{}' in {:?} (restart {}/{})",
                self.name, actor_name, delay, restarts_in_window, self.config.max_restarts
            );

            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => {
                    info!("Supervisor '{}': shutdown during restart delay", self.name);
                    return ActorExitStatus::Quit;
                }
            }
        }
    }
}
