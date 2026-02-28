//! Unit tests for the actor framework.

use varpulis_actors::mailbox::Envelope;
use varpulis_actors::*;

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test actors
// ---------------------------------------------------------------------------

/// Simple counter actor for basic message passing tests.
struct PingPongActor {
    name: String,
    count: u64,
}

impl PingPongActor {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            count: 0,
        }
    }
}

#[derive(Debug)]
struct Ping;

#[async_trait]
impl Actor for PingPongActor {
    type ObservableState = u64;

    fn name(&self) -> &str {
        &self.name
    }

    fn observable_state(&self) -> u64 {
        self.count
    }

    async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => return Ok(()),
                msg = ctx.mailbox.recv() => {
                    match msg {
                        Some(Envelope::Message(_)) => {
                            self.count += 1;
                        }
                        Some(Envelope::Observe(tx)) => {
                            let _ = tx.send(self.observable_state());
                        }
                        Some(Envelope::Ask { reply_tx, .. }) => {
                            self.count += 1;
                            let _ = reply_tx.send(Box::new(self.count));
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Actor that fails after N messages (for supervisor tests).
struct FailingActor {
    fail_after: u64,
    count: u64,
}

impl FailingActor {
    fn new(fail_after: u64) -> Self {
        Self {
            fail_after,
            count: 0,
        }
    }
}

#[async_trait]
impl Actor for FailingActor {
    type ObservableState = u64;

    fn name(&self) -> &str {
        "failing"
    }

    fn observable_state(&self) -> u64 {
        self.count
    }

    async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
        // Fail immediately if fail_after is 0
        if self.fail_after == 0 {
            return Err(ActorExitStatus::Failure("immediate failure".to_string()));
        }
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => return Ok(()),
                msg = ctx.mailbox.recv() => {
                    match msg {
                        Some(_) => {
                            self.count += 1;
                            if self.count >= self.fail_after {
                                return Err(ActorExitStatus::Failure(
                                    "intentional failure".to_string(),
                                ));
                            }
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping_pong_basic_message_passing() {
    let mut runtime = Runtime::new();
    let handle = runtime.spawn(PingPongActor::new("test-ping"), 10);

    handle.send(Ping).await.unwrap();
    handle.send(Ping).await.unwrap();
    handle.send(Ping).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let state = handle.observe().await.unwrap();
    assert_eq!(state, 3);
    assert_eq!(handle.check_health(), Health::Healthy);

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_observable_state() {
    let mut runtime = Runtime::new();
    let handle = runtime.spawn(PingPongActor::new("observer"), 10);

    let state = handle.observe().await.unwrap();
    assert_eq!(state, 0);

    for _ in 0..5 {
        handle.send(Ping).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let state = handle.observe().await.unwrap();
    assert_eq!(state, 5);

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let mut runtime = Runtime::new();
    let handle = runtime.spawn(PingPongActor::new("shutdown-test"), 10);

    handle.send(Ping).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(handle.check_health(), Health::Healthy);

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let health = handle.check_health();
    assert!(matches!(health, Health::Down { .. }));
}

#[tokio::test]
async fn test_supervisor_restart_on_failure() {
    let restart_count = Arc::new(AtomicU64::new(0));
    let rc = restart_count.clone();

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let supervisor = Supervisor::new(
        "test-supervisor",
        move || {
            rc.fetch_add(1, Ordering::Relaxed);
            FailingActor::new(1)
        },
        SupervisorConfig {
            restart_policy: RestartPolicy::OnFailure,
            max_restarts: 3,
            restart_window: Duration::from_secs(10),
            base_restart_delay: Duration::from_millis(10),
            mailbox_capacity: 10,
        },
    );

    let handle = tokio::spawn(async move { supervisor.run(shutdown_clone).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();

    let _status = handle.await.unwrap();
    assert!(restart_count.load(Ordering::Relaxed) >= 1);
}

#[tokio::test]
async fn test_supervisor_never_restart() {
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let supervisor = Supervisor::new(
        "never-restart",
        || FailingActor::new(0),
        SupervisorConfig {
            restart_policy: RestartPolicy::Never,
            max_restarts: 10,
            restart_window: Duration::from_secs(10),
            base_restart_delay: Duration::from_millis(10),
            mailbox_capacity: 10,
        },
    );

    let status = supervisor.run(shutdown_clone).await;

    // With Never policy, actor fails immediately and is not restarted
    assert!(
        matches!(status, ActorExitStatus::Failure(_)),
        "expected Failure, got {:?}",
        status
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_mailbox_backpressure() {
    let (sender, mut mailbox) = create_mailbox::<PingPongActor>(2);

    sender.try_send(Ping).unwrap();
    sender.try_send(Ping).unwrap();

    let result = sender.try_send(Ping);
    assert!(result.is_err());

    let _ = mailbox.try_recv();
    sender.try_send(Ping).unwrap();
}

#[tokio::test]
async fn test_fan_in_message_type() {
    #[derive(Debug)]
    struct MsgA;
    #[derive(Debug)]
    struct MsgB;

    varpulis_actors::fan_in_message_type!(Combined, MsgA, MsgB);

    let _a: Combined = MsgA.into();
    let _b: Combined = MsgB.into();

    match Combined::MsgA(MsgA) {
        Combined::MsgA(_) => {}
        Combined::MsgB(_) => panic!("wrong variant"),
    }
}

#[tokio::test]
async fn test_ask_request_reply() {
    let mut runtime = Runtime::new();
    let handle = runtime.spawn(PingPongActor::new("ask-test"), 10);

    let reply: u64 = handle.ask(Ping).await.unwrap();
    assert_eq!(reply, 1);

    let reply: u64 = handle.ask(Ping).await.unwrap();
    assert_eq!(reply, 2);

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;
}
