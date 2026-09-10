//! JetStream substrate for cluster event transport.
//!
//! # Why this module exists
//!
//! Core NATS is fire-and-forget. A `client.publish` succeeds as soon as the
//! bytes are handed to the socket; if nobody holds a matching subscription at
//! that instant, the message is gone and nothing records that it happened.
//! That is acceptable for an RPC (the reply is the acknowledgement) and
//! unacceptable for an *event* — an event routed to a pipeline that is
//! restarting, migrating, or simply down is a missed detection.
//!
//! This module puts the two event paths — inter-pipeline routing and worker
//! heartbeats — on JetStream, where the server stores the message and a durable
//! consumer resumes from its last acknowledged position after a restart.
//!
//! # Substrate selection (backwards compatibility)
//!
//! JetStream requires a JetStream-enabled `nats-server` (`-js`). Existing
//! deployments may not have one, so the substrate is selectable and defaults to
//! [`Substrate::Core`] — byte-for-byte the previous behaviour, same subjects,
//! same payloads. Choose with `VARPULIS_NATS_SUBSTRATE=core|jetstream` or
//! explicitly via `ClusterTransport::connect`.
//!
//! When JetStream is *requested* and the server does not have it,
//! `ClusterTransport::connect` **fails loudly**. It never falls back to core:
//! silently downgrading an operator who asked for durability to fire-and-forget
//! would reproduce the exact silent loss this module exists to remove.
//!
//! # Stream layout
//!
//! | Stream                | Subjects                          | Holds |
//! |-----------------------|-----------------------------------|-------|
//! | `VARPULIS_ROUTING`    | `varpulis.cluster.pipeline.>`     | inter-pipeline routed events |
//! | `VARPULIS_HEARTBEAT`  | `varpulis.cluster.heartbeat.>`    | worker liveness |
//! | `VARPULIS_DLQ`        | `varpulis.dlq.>`                  | poison events, parked |
//!
//! One stream per *concern*, each bound to a wildcard, rather than a stream per
//! route edge: the routing subject already carries `{group}.{from}.{to}`, so a
//! durable consumer's `filter_subject` isolates delivery per edge while the
//! whole topology shares one set of limits an operator can actually size. A
//! stream per edge would multiply admin objects by the number of edges and give
//! each its own quota that nobody would tune.
//!
//! The DLQ deliberately sits on a **sibling root** (`varpulis.dlq.>`, not under
//! `varpulis.cluster.>`). JetStream forbids overlapping stream subjects, and
//! more importantly the evidence of a poison event must not be evictable by the
//! hot path's `max_age`, nor consumable by a routing consumer's wildcard.
//!
//! Subjects deliberately left on core NATS are listed on
//! `ClusterTransport`.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Substrate
// ---------------------------------------------------------------------------

/// Which NATS delivery substrate the cluster event paths use.
///
/// Defaults to [`Substrate::Core`] so an existing deployment on a plain
/// `nats-server` is unaffected by upgrading Varpulis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Substrate {
    /// Core NATS pub/sub: fire-and-forget, no persistence, no redelivery.
    /// The historical behaviour, and the default.
    #[default]
    Core,
    /// JetStream: the server stores the message, durable consumers resume after
    /// a restart, and unacked messages are redelivered.
    JetStream,
}

/// Environment variable that selects the substrate.
pub const SUBSTRATE_ENV: &str = "VARPULIS_NATS_SUBSTRATE";

impl Substrate {
    /// Parse a substrate name. Accepts `core` / `jetstream` (aliases: `js`,
    /// `pubsub`), case-insensitive.
    pub fn parse(s: &str) -> Result<Self, InvalidSubstrate> {
        match s.trim().to_ascii_lowercase().as_str() {
            "core" | "pubsub" | "pub_sub" => Ok(Substrate::Core),
            "jetstream" | "js" => Ok(Substrate::JetStream),
            other => Err(InvalidSubstrate(other.to_string())),
        }
    }

    /// Read the substrate from [`SUBSTRATE_ENV`], defaulting to
    /// [`Substrate::Core`] when unset.
    ///
    /// An unparseable value is an error rather than a silent default: an
    /// operator who typed `VARPULIS_NATS_SUBSTRATE=jetstrem` asked for
    /// durability and must not get fire-and-forget.
    pub fn from_env() -> Result<Self, InvalidSubstrate> {
        match std::env::var(SUBSTRATE_ENV) {
            Ok(v) if !v.trim().is_empty() => Self::parse(&v),
            _ => Ok(Substrate::Core),
        }
    }

    /// `true` when messages are stored and redelivered.
    pub fn is_durable(self) -> bool {
        matches!(self, Substrate::JetStream)
    }

    /// Name as accepted by [`Substrate::parse`].
    pub fn as_str(self) -> &'static str {
        match self {
            Substrate::Core => "core",
            Substrate::JetStream => "jetstream",
        }
    }
}

/// An unrecognised [`Substrate`] name.
#[derive(Debug, thiserror::Error)]
#[error("unknown NATS substrate '{0}' (expected 'core' or 'jetstream')")]
pub struct InvalidSubstrate(pub String);

// ---------------------------------------------------------------------------
// Stream names, subjects and limits
// ---------------------------------------------------------------------------

/// Stream holding inter-pipeline routed events.
pub const ROUTING_STREAM: &str = "VARPULIS_ROUTING";
/// Wildcard the routing stream is bound to.
pub const ROUTING_SUBJECTS: &str = "varpulis.cluster.pipeline.>";

/// Stream holding worker heartbeats.
pub const HEARTBEAT_STREAM: &str = "VARPULIS_HEARTBEAT";
/// Wildcard the heartbeat stream is bound to.
pub const HEARTBEAT_SUBJECTS: &str = "varpulis.cluster.heartbeat.>";

/// Stream holding parked poison events.
pub const DLQ_STREAM: &str = "VARPULIS_DLQ";
/// Root of the dead-letter subject space — a sibling of `varpulis.cluster.>`,
/// never a child, so hot-path retention cannot evict the evidence.
pub const DLQ_ROOT: &str = "varpulis.dlq";
/// Wildcard the DLQ stream is bound to.
pub const DLQ_SUBJECTS: &str = "varpulis.dlq.>";

/// How long a routed event is retained when nobody consumes it.
///
/// One hour: the "a detection pipeline was down for an hour" case the operator
/// actually has. A pipeline down longer than that has an operator problem, not
/// a buffering problem — and replaying a multi-day backlog into an engine with
/// time windows manufactures nonsense alerts rather than recovering real ones.
pub const ROUTING_MAX_AGE: Duration = Duration::from_hours(1);

/// Byte ceiling for the routing stream (1 GiB), so a wedged consumer cannot
/// fill the operator's disk.
pub const ROUTING_MAX_BYTES: i64 = 1024 * 1024 * 1024;

/// How long a heartbeat is retained.
///
/// Five minutes: a heartbeat is only interesting until the next one arrives.
/// This is comfortably above any sane `heartbeat_timeout`, so a coordinator
/// restart cannot outlive the evidence it needs to make a liveness decision.
pub const HEARTBEAT_MAX_AGE: Duration = Duration::from_mins(5);

/// Heartbeats retained per worker subject. A restarting coordinator replays at
/// most this many per worker, and one chatty worker cannot crowd out another.
pub const HEARTBEAT_MAX_PER_SUBJECT: i64 = 16;

/// Ceiling on parked poison events. A full DLQ is itself an operator signal,
/// never silent unbounded growth. No `max_age`: a poison event from last week
/// is exactly the thing you still want to find.
pub const DLQ_MAX_MSGS: i64 = 100_000;

/// Deliveries before an event is treated as poison.
///
/// At this count it is parked in the DLQ and acked. Matches the house reference
/// (Vejas `MAX_DELIVERIES`): enough for a transient downstream blip, few enough
/// that a genuinely poisonous event does not loop forever.
pub const MAX_DELIVER: i64 = 5;

/// How long the server waits for an ack before redelivering.
pub const ACK_WAIT: Duration = Duration::from_secs(30);

/// Redelivery backoff after an explicit negative acknowledgement.
pub const NAK_BACKOFF: Duration = Duration::from_secs(2);

/// Unacked messages a single consumer may hold at once.
pub const MAX_ACK_PENDING: i64 = 1024;

/// How long a durable consumer survives with nobody bound to it.
///
/// Seven days, not the JetStream default of "forever": a route edge that was
/// deleted from a pipeline group should not leave a consumer accumulating
/// unacked messages against the stream's limits indefinitely. Seven days is far
/// longer than [`ROUTING_MAX_AGE`], so a consumer is never reaped while it
/// still has retrievable work.
pub const CONSUMER_INACTIVE_THRESHOLD: Duration = Duration::from_hours(24 * 7);

// ---------------------------------------------------------------------------
// Durable-name derivation (pure, so it is testable without a broker)
// ---------------------------------------------------------------------------

/// Maximum length of a generated durable consumer name before it is hashed.
const DURABLE_NAME_MAX: usize = 96;

/// FNV-1a, spelled out rather than using `DefaultHasher`, because a durable
/// consumer name must be stable across Rust versions — `DefaultHasher`'s output
/// explicitly is not. A name that changes under a toolchain upgrade would
/// silently orphan the old consumer and its unacked backlog.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Map arbitrary text to the characters a NATS consumer name allows.
fn sanitize_token(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Durable consumer name for a pipeline consuming one routing subject.
///
/// Stable across restarts (that is the entire point — a restarting consumer
/// must find its own position, not create a fresh one) and unique per
/// `(pipeline, subject)` pair, so two pipelines subscribed to the same custom
/// subject fan out rather than silently load-balancing against each other.
///
/// Long inputs are truncated with a stable FNV-1a suffix so the name stays
/// within the server's limit without two different routes colliding.
pub fn route_durable_name(pipeline: &str, subject: &str) -> String {
    let raw = format!("route_{pipeline}__{subject}");
    let clean = sanitize_token(&raw);
    if clean.len() <= DURABLE_NAME_MAX {
        return clean;
    }
    let digest = fnv1a(raw.as_bytes());
    let keep = DURABLE_NAME_MAX - 17; // 16 hex digits + '_'
    format!("{}_{digest:016x}", &clean[..keep])
}

/// Durable consumer name for the coordinator's heartbeat consumer.
///
/// Keyed by coordinator identity so two coordinators each see *every*
/// heartbeat. A shared durable would load-balance them, and half of each
/// worker's liveness signal would be invisible to each coordinator.
pub fn heartbeat_durable_name(coordinator_id: &str) -> String {
    let clean = sanitize_token(coordinator_id);
    let name = format!("hb_{clean}");
    if name.len() <= DURABLE_NAME_MAX {
        return name;
    }
    let digest = fnv1a(coordinator_id.as_bytes());
    format!("hb_{digest:016x}")
}

/// DLQ subject for a named unit (a pipeline, a consumer, a connector).
pub fn dlq_subject(unit: &str) -> String {
    format!("{DLQ_ROOT}.{}", sanitize_token(unit))
}

// ---------------------------------------------------------------------------
// Transport (requires the `nats-transport` feature for async-nats)
// ---------------------------------------------------------------------------

/// Errors from the JetStream substrate.
#[cfg(feature = "nats-transport")]
#[derive(Debug, thiserror::Error)]
pub enum JetStreamError {
    #[error("failed to connect to NATS at {url}: {source}")]
    Connect {
        url: String,
        #[source]
        source: async_nats::ConnectError,
    },
    #[error(
        "JetStream was requested (VARPULIS_NATS_SUBSTRATE=jetstream) but the NATS server at \
         {url} does not have it enabled — start nats-server with `-js`, or set \
         VARPULIS_NATS_SUBSTRATE=core to keep the previous fire-and-forget behaviour (which \
         loses events across a consumer restart). Server said: {reason}"
    )]
    NotEnabled { url: String, reason: String },
    #[error("failed to provision stream {stream}: {reason}")]
    Stream { stream: String, reason: String },
    #[error("failed to provision consumer {consumer} on {stream}: {reason}")]
    Consumer {
        consumer: String,
        stream: String,
        reason: String,
    },
    #[error("serialization failed: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("publish to {subject} failed: {reason}")]
    Publish { subject: String, reason: String },
    #[error("consuming {subject} failed: {reason}")]
    Consume { subject: String, reason: String },
    #[error("ack failed: {0}")]
    Ack(String),
}

/// The cluster's event transport: publishes and consumes on either substrate
/// behind one API, so an ingress loop is written once and the durability
/// decision lives in configuration.
///
/// # What is deliberately *not* carried here
///
/// These subjects stay on core NATS, on purpose:
///
/// - `varpulis.cluster.register` — request/reply. The reply *is* the
///   acknowledgement, and the worker already retries with backoff
///   (`worker_nats_registration_loop`). Persisting an RPC only means replaying
///   a stale registration at an arbitrary later time.
/// - `varpulis.cluster.cmd.{worker}.>` — deploy / inject / drain. Same
///   reply-is-the-ack argument, and replaying a `deploy` or a `drain` an hour
///   late against a worker that has since been rebalanced is actively harmful.
///   These are *commands*; commands are not events.
/// - `varpulis.cluster.raft.>` — Raft has its own log, terms and retries.
///   Putting a second replicated log underneath a consensus log is a
///   correctness hazard: a redelivered `AppendEntries` from a past term.
/// - `varpulis.cluster.checkpoint.>` — a two-phase barrier handshake with its
///   own timeout and abort path. A redelivered `checkpoint_complete` for a
///   checkpoint that was already aborted would corrupt the protocol state
///   machine.
#[cfg(feature = "nats-transport")]
#[derive(Clone)]
pub struct ClusterTransport {
    client: async_nats::Client,
    context: Option<async_nats::jetstream::Context>,
    url: String,
}

#[cfg(feature = "nats-transport")]
impl std::fmt::Debug for ClusterTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTransport")
            .field("url", &self.url)
            .field("substrate", &self.substrate())
            .finish()
    }
}

#[cfg(feature = "nats-transport")]
impl ClusterTransport {
    /// Connect and provision whatever the chosen substrate needs.
    ///
    /// On [`Substrate::JetStream`] this verifies JetStream is actually enabled
    /// and creates the routing, heartbeat and DLQ streams if absent. It does
    /// **not** fall back to core on failure — see the module docs.
    pub async fn connect(url: &str, substrate: Substrate) -> Result<Self, JetStreamError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|source| JetStreamError::Connect {
                url: url.to_string(),
                source,
            })?;
        Self::from_client(client, url, substrate).await
    }

    /// Wrap an already-connected client. `url` is used for diagnostics only.
    pub async fn from_client(
        client: async_nats::Client,
        url: &str,
        substrate: Substrate,
    ) -> Result<Self, JetStreamError> {
        let context = match substrate {
            Substrate::Core => None,
            Substrate::JetStream => {
                let ctx = async_nats::jetstream::new(client.clone());
                // Probe before provisioning so "no JetStream on this server"
                // reports as itself rather than as an opaque stream error.
                ctx.query_account()
                    .await
                    .map_err(|e| JetStreamError::NotEnabled {
                        url: url.to_string(),
                        reason: e.to_string(),
                    })?;
                ensure_streams(&ctx).await?;
                Some(ctx)
            }
        };
        Ok(Self {
            client,
            context,
            url: url.to_string(),
        })
    }

    /// The substrate in force.
    pub fn substrate(&self) -> Substrate {
        if self.context.is_some() {
            Substrate::JetStream
        } else {
            Substrate::Core
        }
    }

    /// The underlying core client, for subjects that stay on core NATS.
    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    /// The JetStream context, when the substrate is JetStream.
    pub fn context(&self) -> Option<&async_nats::jetstream::Context> {
        self.context.as_ref()
    }

    /// Publish a cluster event.
    ///
    /// On JetStream this awaits the server's `PubAck`, so a publish the stream
    /// did not store is an *error* the caller sees, not a silent drop. `msg_id`
    /// feeds the stream's duplicate window, so a retried publish of the same
    /// logical event is deduplicated server-side rather than double-counted.
    ///
    /// On core it is the historical fire-and-forget publish.
    pub async fn publish_event<T: serde::Serialize>(
        &self,
        subject: &str,
        payload: &T,
        msg_id: Option<&str>,
    ) -> Result<(), JetStreamError> {
        let bytes = serde_json::to_vec(payload).map_err(JetStreamError::Serialize)?;
        self.publish_bytes(subject, bytes, msg_id).await
    }

    /// [`Self::publish_event`] for an already-encoded payload.
    pub async fn publish_bytes(
        &self,
        subject: &str,
        bytes: Vec<u8>,
        msg_id: Option<&str>,
    ) -> Result<(), JetStreamError> {
        match &self.context {
            None => self
                .client
                .publish(subject.to_string(), bytes.into())
                .await
                .map_err(|e| JetStreamError::Publish {
                    subject: subject.to_string(),
                    reason: e.to_string(),
                }),
            Some(ctx) => {
                let ack_future = match msg_id {
                    Some(id) => {
                        let mut headers = async_nats::HeaderMap::new();
                        headers.insert("Nats-Msg-Id", id);
                        ctx.publish_with_headers(subject.to_string(), headers, bytes.into())
                            .await
                    }
                    None => ctx.publish(subject.to_string(), bytes.into()).await,
                }
                .map_err(|e| JetStreamError::Publish {
                    subject: subject.to_string(),
                    reason: e.to_string(),
                })?;
                // Await the PubAck: without this, `publish` only means "handed
                // to the socket" and a stream that rejected the message (limits,
                // no stream bound to the subject) would look like success.
                ack_future.await.map_err(|e| JetStreamError::Publish {
                    subject: subject.to_string(),
                    reason: e.to_string(),
                })?;
                Ok(())
            }
        }
    }

    /// Bind a consumer to one routing subject on behalf of `pipeline`.
    ///
    /// On JetStream this is a **durable** pull consumer: it resumes from its
    /// last acknowledged position, so events published while the pipeline was
    /// down are delivered when it comes back. On core it is a plain
    /// subscription with the historical semantics — anything published while
    /// this process was not subscribed is gone.
    pub async fn consume_route(
        &self,
        pipeline: &str,
        subject: &str,
    ) -> Result<EventConsumer, JetStreamError> {
        self.consume(
            subject,
            route_durable_name(pipeline, subject),
            ROUTING_STREAM,
            pipeline.to_string(),
        )
        .await
    }

    /// Bind the coordinator's heartbeat consumer to `varpulis.cluster.heartbeat.>`.
    pub async fn consume_heartbeats(
        &self,
        coordinator_id: &str,
    ) -> Result<EventConsumer, JetStreamError> {
        self.consume(
            HEARTBEAT_SUBJECTS,
            heartbeat_durable_name(coordinator_id),
            HEARTBEAT_STREAM,
            format!("coordinator:{coordinator_id}"),
        )
        .await
    }

    async fn consume(
        &self,
        subject: &str,
        durable: String,
        stream_name: &str,
        unit: String,
    ) -> Result<EventConsumer, JetStreamError> {
        match &self.context {
            None => {
                let sub = self
                    .client
                    .subscribe(subject.to_string())
                    .await
                    .map_err(|e| JetStreamError::Consume {
                        subject: subject.to_string(),
                        reason: e.to_string(),
                    })?;
                Ok(EventConsumer {
                    inner: ConsumerInner::Core(sub),
                    subject: subject.to_string(),
                    unit,
                    context: None,
                })
            }
            Some(ctx) => {
                let stream =
                    ctx.get_stream(stream_name)
                        .await
                        .map_err(|e| JetStreamError::Stream {
                            stream: stream_name.to_string(),
                            reason: e.to_string(),
                        })?;
                let consumer = stream
                    .get_or_create_consumer::<async_nats::jetstream::consumer::pull::Config>(
                        &durable,
                        async_nats::jetstream::consumer::pull::Config {
                            durable_name: Some(durable.clone()),
                            // Ack after apply, one message at a time. `All`
                            // would ack every lower sequence too, silently
                            // discarding anything still in flight behind a
                            // slow apply; `None` would discard everything.
                            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                            ack_wait: ACK_WAIT,
                            max_deliver: MAX_DELIVER,
                            max_ack_pending: MAX_ACK_PENDING,
                            filter_subject: subject.to_string(),
                            inactive_threshold: CONSUMER_INACTIVE_THRESHOLD,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| JetStreamError::Consumer {
                        consumer: durable.clone(),
                        stream: stream_name.to_string(),
                        reason: e.to_string(),
                    })?;
                let messages = consumer
                    .messages()
                    .await
                    .map_err(|e| JetStreamError::Consume {
                        subject: subject.to_string(),
                        reason: e.to_string(),
                    })?;
                Ok(EventConsumer {
                    inner: ConsumerInner::JetStream(Box::new(messages)),
                    subject: subject.to_string(),
                    unit,
                    context: Some(ctx.clone()),
                })
            }
        }
    }
}

/// Create the routing, heartbeat and DLQ streams if they do not exist.
///
/// Idempotent, and deliberately *not* an update: an operator who tuned
/// `max_bytes` upward for their own volume keeps their tuning across a Varpulis
/// upgrade rather than having it silently reset.
#[cfg(feature = "nats-transport")]
pub async fn ensure_streams(ctx: &async_nats::jetstream::Context) -> Result<(), JetStreamError> {
    use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy, StorageType};

    // Routing: Limits retention, not WorkQueue and not Interest.
    //
    // WorkQueue deletes on first ack and forbids two consumers on the same
    // subject — that kills both replay and fan-out, which are the point.
    // Interest deletes once every *currently known* consumer has acked, so an
    // event published before a brand-new route edge created its consumer is
    // dropped: precisely the silent loss being fixed. Limits keeps the message
    // for the window regardless of who is or is not listening.
    ctx.get_or_create_stream(Config {
        name: ROUTING_STREAM.to_string(),
        subjects: vec![ROUTING_SUBJECTS.to_string()],
        retention: RetentionPolicy::Limits,
        max_age: ROUTING_MAX_AGE,
        max_bytes: ROUTING_MAX_BYTES,
        // At the limit, drop the OLDEST and keep accepting. `DiscardPolicy::New`
        // would fail the publish instead, turning a stalled downstream pipeline
        // into an outage of the upstream one that detects. For a detection
        // engine the freshest events matter most, and an evicting stream is
        // visible in `nats stream info` rather than silent.
        discard: DiscardPolicy::Old,
        // Surviving a broker restart is the whole reason this stream exists.
        storage: StorageType::File,
        // Server-side dedup for republished events carrying a `Nats-Msg-Id`.
        duplicate_window: Duration::from_mins(2),
        ..Default::default()
    })
    .await
    .map_err(|e| JetStreamError::Stream {
        stream: ROUTING_STREAM.to_string(),
        reason: e.to_string(),
    })?;

    ctx.get_or_create_stream(Config {
        name: HEARTBEAT_STREAM.to_string(),
        subjects: vec![HEARTBEAT_SUBJECTS.to_string()],
        retention: RetentionPolicy::Limits,
        max_age: HEARTBEAT_MAX_AGE,
        // Per-subject cap: one worker's heartbeats can never crowd out another's.
        max_messages_per_subject: HEARTBEAT_MAX_PER_SUBJECT,
        discard: DiscardPolicy::Old,
        storage: StorageType::File,
        ..Default::default()
    })
    .await
    .map_err(|e| JetStreamError::Stream {
        stream: HEARTBEAT_STREAM.to_string(),
        reason: e.to_string(),
    })?;

    ensure_dlq_stream(ctx).await
}

/// Create the dead-letter stream if absent. Bounded and discard-oldest: a full
/// DLQ is itself an operator signal, never silent unbounded growth.
#[cfg(feature = "nats-transport")]
pub async fn ensure_dlq_stream(ctx: &async_nats::jetstream::Context) -> Result<(), JetStreamError> {
    use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy, StorageType};

    ctx.get_or_create_stream(Config {
        name: DLQ_STREAM.to_string(),
        subjects: vec![DLQ_SUBJECTS.to_string()],
        retention: RetentionPolicy::Limits,
        max_messages: DLQ_MAX_MSGS,
        discard: DiscardPolicy::Old,
        storage: StorageType::File,
        ..Default::default()
    })
    .await
    .map(|_| ())
    .map_err(|e| JetStreamError::Stream {
        stream: DLQ_STREAM.to_string(),
        reason: e.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Consumer + delivery
// ---------------------------------------------------------------------------

#[cfg(feature = "nats-transport")]
enum ConsumerInner {
    Core(async_nats::Subscriber),
    JetStream(Box<async_nats::jetstream::consumer::pull::Stream>),
}

/// A bound consumer over one subject.
///
/// [`EventConsumer::next`] yields a [`Delivery`] that has **not** been
/// acknowledged. The caller applies it to engine state and only then calls
/// [`Delivery::ack`].
#[cfg(feature = "nats-transport")]
pub struct EventConsumer {
    inner: ConsumerInner,
    subject: String,
    unit: String,
    context: Option<async_nats::jetstream::Context>,
}

#[cfg(feature = "nats-transport")]
impl std::fmt::Debug for EventConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventConsumer")
            .field("subject", &self.subject)
            .field("unit", &self.unit)
            .field("durable", &self.is_durable())
            .finish()
    }
}

#[cfg(feature = "nats-transport")]
impl EventConsumer {
    /// Subject this consumer is bound to.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Whether deliveries from this consumer are redelivered when unacked.
    pub fn is_durable(&self) -> bool {
        matches!(self.inner, ConsumerInner::JetStream(_))
    }

    /// Next delivery, or `None` when the consumer is closed.
    ///
    /// A transport-level error while pulling is logged and skipped rather than
    /// ending the stream: the pull consumer reconnects on its own, and
    /// returning `None` would silently stop an ingress loop.
    pub async fn next(&mut self) -> Option<Delivery> {
        use futures_util::StreamExt;
        match &mut self.inner {
            ConsumerInner::Core(sub) => sub.next().await.map(|msg| Delivery {
                payload: msg.payload.to_vec(),
                subject: msg.subject.to_string(),
                attempt: 1,
                inner: DeliveryInner::Core,
                unit: self.unit.clone(),
                context: None,
            }),
            ConsumerInner::JetStream(stream) => loop {
                match stream.next().await {
                    None => return None,
                    Some(Err(e)) => {
                        tracing::warn!(
                            subject = %self.subject,
                            error = %e,
                            "JetStream pull error; continuing"
                        );
                        continue;
                    }
                    Some(Ok(msg)) => {
                        let delivery_attempt = msg.info().map(|i| i.delivered).unwrap_or(1);
                        return Some(Delivery {
                            payload: msg.payload.to_vec(),
                            subject: msg.subject.to_string(),
                            attempt: delivery_attempt,
                            inner: DeliveryInner::JetStream(Box::new(msg)),
                            unit: self.unit.clone(),
                            context: self.context.clone(),
                        });
                    }
                }
            },
        }
    }
}

#[cfg(feature = "nats-transport")]
enum DeliveryInner {
    Core,
    JetStream(Box<async_nats::jetstream::Message>),
}

/// One delivered, **not yet acknowledged** event.
///
/// The ack methods take `self` by value so a delivery cannot be acked twice,
/// and so the type system pushes the caller towards the one correct order:
/// apply to engine state, *then* ack. Acking on receipt is the silent-loss
/// pattern this module exists to remove.
#[cfg(feature = "nats-transport")]
pub struct Delivery {
    payload: Vec<u8>,
    subject: String,
    attempt: i64,
    inner: DeliveryInner,
    unit: String,
    context: Option<async_nats::jetstream::Context>,
}

#[cfg(feature = "nats-transport")]
impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("subject", &self.subject)
            .field("delivery_attempt", &self.attempt)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

#[cfg(feature = "nats-transport")]
impl Delivery {
    /// Raw payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Subject the event arrived on.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Delivery attempt number, starting at 1. Always 1 on core NATS, which has
    /// no redelivery.
    pub fn delivery_attempt(&self) -> i64 {
        self.attempt
    }

    /// `true` when this delivery has exhausted [`MAX_DELIVER`] and must be
    /// dead-lettered rather than naked again.
    pub fn is_poison(&self) -> bool {
        matches!(self.inner, DeliveryInner::JetStream(_)) && self.attempt >= MAX_DELIVER
    }

    /// Decode the payload as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }

    /// When the server received this message. `None` on core NATS.
    ///
    /// Used by the heartbeat consumer to refuse to apply a heartbeat that is
    /// older than the liveness timeout: replaying a stale heartbeat after a
    /// coordinator restart would resurrect a worker that is actually dead.
    pub fn age(&self) -> Option<Duration> {
        match &self.inner {
            DeliveryInner::Core => None,
            DeliveryInner::JetStream(msg) => {
                // Compared through unix seconds so this module needs no `time`
                // dependency of its own.
                let published = msg.info().ok()?.published.unix_timestamp();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?
                    .as_secs() as i64;
                Some(Duration::from_secs(
                    now.saturating_sub(published).max(0) as u64
                ))
            }
        }
    }

    /// Acknowledge: the event has been applied to engine state and must not be
    /// redelivered. Call this **after** the apply, never before.
    pub async fn ack(self) -> Result<(), JetStreamError> {
        match self.inner {
            DeliveryInner::Core => Ok(()),
            DeliveryInner::JetStream(msg) => msg
                .ack()
                .await
                .map_err(|e| JetStreamError::Ack(e.to_string())),
        }
    }

    /// Negatively acknowledge: the apply failed, redeliver after a backoff.
    ///
    /// On core NATS this is a no-op with a warning — there is nothing to
    /// redeliver, which is exactly why core loses events.
    pub async fn nak(self) -> Result<(), JetStreamError> {
        match self.inner {
            DeliveryInner::Core => {
                tracing::warn!(
                    subject = %self.subject,
                    "apply failed on core NATS substrate — the event is lost, there is no \
                     redelivery. Set VARPULIS_NATS_SUBSTRATE=jetstream for at-least-once."
                );
                Ok(())
            }
            DeliveryInner::JetStream(msg) => msg
                .ack_with(async_nats::jetstream::AckKind::Nak(Some(NAK_BACKOFF)))
                .await
                .map_err(|e| JetStreamError::Ack(e.to_string())),
        }
    }

    /// Park a poison event in the DLQ, then ack the original.
    ///
    /// Publish-before-ack: the original is acked **only** once JetStream has
    /// confirmed the death envelope is stored. If the DLQ publish fails the
    /// original is naked instead, so a failed park never vaporises the event.
    pub async fn dead_letter(self, reason: &str) -> Result<(), JetStreamError> {
        let (msg, ctx) = match (self.inner, self.context) {
            (DeliveryInner::JetStream(msg), Some(ctx)) => (msg, ctx),
            // Core has no DLQ and no redelivery; say so rather than pretending.
            (DeliveryInner::Core, _) => {
                tracing::error!(
                    subject = %self.subject,
                    reason,
                    "poison event dropped on core NATS substrate — no dead-letter queue exists \
                     without JetStream"
                );
                return Ok(());
            }
            (DeliveryInner::JetStream(msg), None) => {
                // Cannot happen (a JetStream delivery always carries its
                // context) but nak rather than ack if it ever does.
                let _ = msg
                    .ack_with(async_nats::jetstream::AckKind::Nak(Some(NAK_BACKOFF)))
                    .await;
                return Err(JetStreamError::Ack(
                    "JetStream delivery without a context; naked instead of dead-lettering".into(),
                ));
            }
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let envelope = serde_json::json!({
            "original_subject": self.subject,
            "unit": self.unit,
            "attempts": self.attempt,
            "dead_at": now,
            "last_error": reason,
            "payload": String::from_utf8_lossy(&self.payload),
        });
        let body = serde_json::to_vec(&envelope).map_err(JetStreamError::Serialize)?;
        let subject = dlq_subject(&self.unit);

        let park = async {
            let ack = ctx
                .publish(subject.clone(), body.into())
                .await
                .map_err(|e| e.to_string())?;
            ack.await.map_err(|e| e.to_string())
        }
        .await;

        match park {
            Ok(_) => {
                tracing::error!(
                    subject = %self.subject,
                    dlq_subject = %subject,
                    attempts = self.attempt,
                    reason,
                    "dead-lettered after exhausting deliveries"
                );
                msg.ack()
                    .await
                    .map_err(|e| JetStreamError::Ack(e.to_string()))
            }
            Err(e) => {
                tracing::error!(
                    subject = %self.subject,
                    error = %e,
                    "DLQ publish failed; naking so the event stays in the stream"
                );
                msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(NAK_BACKOFF)))
                    .await
                    .map_err(|e| JetStreamError::Ack(e.to_string()))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (pure — the broker-backed gates live in tests/nats_jetstream_routing.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substrate_defaults_to_core() {
        // The backwards-compatibility promise: an existing deployment that sets
        // nothing keeps fire-and-forget core pub/sub on a plain nats-server.
        assert_eq!(Substrate::default(), Substrate::Core);
        assert!(!Substrate::default().is_durable());
    }

    #[test]
    fn substrate_parses_names_and_aliases() {
        assert_eq!(Substrate::parse("core").unwrap(), Substrate::Core);
        assert_eq!(Substrate::parse("  CORE ").unwrap(), Substrate::Core);
        assert_eq!(Substrate::parse("pubsub").unwrap(), Substrate::Core);
        assert_eq!(Substrate::parse("jetstream").unwrap(), Substrate::JetStream);
        assert_eq!(Substrate::parse("JS").unwrap(), Substrate::JetStream);
        assert!(Substrate::parse("jetstrem").is_err());
        assert!(Substrate::parse("").is_err());
    }

    #[test]
    fn substrate_round_trips_through_as_str() {
        for s in [Substrate::Core, Substrate::JetStream] {
            assert_eq!(Substrate::parse(s.as_str()).unwrap(), s);
        }
    }

    #[test]
    fn route_durable_name_is_stable_and_unique() {
        let a = route_durable_name("row0", "varpulis.cluster.pipeline.g1.ingress.row0");
        let b = route_durable_name("row0", "varpulis.cluster.pipeline.g1.ingress.row0");
        assert_eq!(a, b, "durable name must be stable across restarts");
        assert_eq!(a, "route_row0__varpulis_cluster_pipeline_g1_ingress_row0");

        // Two pipelines on the same custom subject must fan out, not
        // load-balance against each other.
        let p1 = route_durable_name("alpha", "custom.subject");
        let p2 = route_durable_name("beta", "custom.subject");
        assert_ne!(p1, p2);
    }

    #[test]
    fn route_durable_name_stays_within_length_limit() {
        let long_subject = format!("varpulis.cluster.pipeline.{}", "x".repeat(300));
        let name = route_durable_name("some-pipeline", &long_subject);
        assert!(
            name.len() <= DURABLE_NAME_MAX,
            "durable name {} chars exceeds limit",
            name.len()
        );
        // Still deterministic, and still distinct from a neighbouring subject.
        assert_eq!(name, route_durable_name("some-pipeline", &long_subject));
        let other = format!("{long_subject}y");
        assert_ne!(name, route_durable_name("some-pipeline", &other));
    }

    #[test]
    fn durable_names_contain_no_nats_wildcards() {
        // '.', '*' and '>' are illegal in a consumer name.
        for name in [
            route_durable_name("p.1", "a.b.*"),
            route_durable_name("p", "varpulis.cluster.pipeline.>"),
            heartbeat_durable_name("coord.one"),
        ] {
            assert!(!name.contains('.'), "{name}");
            assert!(!name.contains('*'), "{name}");
            assert!(!name.contains('>'), "{name}");
            assert!(!name.contains(' '), "{name}");
        }
    }

    #[test]
    fn heartbeat_durable_is_per_coordinator() {
        assert_ne!(
            heartbeat_durable_name("coord-a"),
            heartbeat_durable_name("coord-b"),
            "a shared durable would load-balance heartbeats between coordinators, \
             hiding half of each worker's liveness from each of them"
        );
        assert_eq!(heartbeat_durable_name("coord-a"), "hb_coord-a");
    }

    #[test]
    fn dlq_root_is_a_sibling_of_the_cluster_root() {
        // JetStream forbids overlapping stream subjects, and the DLQ must not be
        // evictable by the hot path's max_age nor readable by a routing wildcard.
        assert!(!DLQ_SUBJECTS.starts_with("varpulis.cluster."));
        assert!(ROUTING_SUBJECTS.starts_with("varpulis.cluster."));
        assert!(HEARTBEAT_SUBJECTS.starts_with("varpulis.cluster."));
        // ...and the two cluster streams do not overlap each other.
        assert_ne!(ROUTING_SUBJECTS, HEARTBEAT_SUBJECTS);
    }

    #[test]
    fn dlq_subject_sanitizes_unit_names() {
        assert_eq!(dlq_subject("row0"), "varpulis.dlq.row0");
        assert_eq!(
            dlq_subject("coordinator:c1"),
            "varpulis.dlq.coordinator_c1",
            "a ':' or '.' in a unit name would otherwise fan the DLQ into subjects \
             the stream is not bound to"
        );
    }

    #[test]
    fn retention_window_outlives_consumer_reaping() {
        // A consumer must never be reaped while it still has retrievable work.
        assert!(CONSUMER_INACTIVE_THRESHOLD > ROUTING_MAX_AGE);
        // And the heartbeat window must outlive any plausible coordinator restart.
        assert!(HEARTBEAT_MAX_AGE >= Duration::from_mins(1));
    }
}
