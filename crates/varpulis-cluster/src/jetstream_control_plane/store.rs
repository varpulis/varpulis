//! The KV substrate: a thin, typed, compare-and-swap wrapper over a
//! JetStream KV bucket.
//!
//! Every mutation goes through [`ControlPlane::write`] with an explicit
//! [`Expect`]. There is no unconditional-write API on purpose: a blind `put`
//! is precisely the "last writer wins, no matter how stale" semantic that
//! makes a fenced-out worker's write land, and it is the thing this module
//! exists to remove. The one blind path, [`ControlPlane::force_put`], is
//! `#[doc(hidden)]` and used only by the fail-before harness.

use std::collections::BTreeMap;
use std::time::Duration;

use async_nats::jetstream::kv;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::keys::ControlKey;

/// Default bucket name. Override with [`ControlPlaneConfig::bucket`].
pub const DEFAULT_BUCKET: &str = "VARPULIS_CONTROL";

/// Environment variable selecting the NATS URL for the control plane.
pub const ENV_URL: &str = "VARPULIS_CONTROL_PLANE_URL";
/// Environment variable selecting the bucket name.
pub const ENV_BUCKET: &str = "VARPULIS_CONTROL_PLANE_BUCKET";
/// Environment variable selecting the entry TTL (seconds).
pub const ENV_TTL_SECS: &str = "VARPULIS_CONTROL_PLANE_TTL_SECS";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from the JetStream control plane.
#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    /// A compare-and-swap was refused: the key had moved on since the caller
    /// read it. **This is the fencing signal** — a caller that sees this must
    /// re-read and recompute, and a *writer* that sees it has been fenced out
    /// and must stop writing.
    #[error("compare-and-swap refused for {key}: expected revision {expected}, key has moved on")]
    Cas { key: String, expected: String },

    /// Transport / JetStream failure (broker down, bucket missing, …).
    #[error("control plane transport error on {op}: {source}")]
    Transport {
        op: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A stored value could not be decoded into the expected type.
    #[error("control plane decode error for {key}: {source}")]
    Decode {
        key: String,
        #[source]
        source: serde_json::Error,
    },
}

impl ControlPlaneError {
    /// True when this is a refused compare-and-swap (i.e. a fenced write).
    pub fn is_cas_conflict(&self) -> bool {
        matches!(self, Self::Cas { .. })
    }

    fn transport<E: std::error::Error + Send + Sync + 'static>(op: &'static str, e: E) -> Self {
        Self::Transport {
            op,
            source: Box::new(e),
        }
    }
}

/// Result alias for control-plane operations.
pub type Result<T> = std::result::Result<T, ControlPlaneError>;

// ---------------------------------------------------------------------------
// Revision-carrying value
// ---------------------------------------------------------------------------

/// A value read from the bucket together with the KV revision it was read at.
///
/// The revision is the fencing token: hand it back to
/// [`ControlPlane::write`] and the write is accepted only if nothing has
/// touched the key since. See [`super::fence`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub value: T,
    pub revision: u64,
}

impl<T> Versioned<T> {
    pub fn new(value: T, revision: u64) -> Self {
        Self { value, revision }
    }

    /// Map the value, preserving the revision.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Versioned<U> {
        Versioned {
            value: f(self.value),
            revision: self.revision,
        }
    }
}

/// The precondition a write is subject to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The key must not exist (JetStream `create`). Used to *claim* an id.
    Absent,
    /// The key must be at exactly this revision (JetStream `update`).
    Revision(u64),
}

impl std::fmt::Display for Expect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str("absent"),
            Self::Revision(r) => write!(f, "{r}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How to open the control-plane bucket.
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    /// NATS URL, e.g. `nats://127.0.0.1:4222`.
    pub url: String,
    /// KV bucket name.
    pub bucket: String,
    /// Entry TTL. Every control-plane record is refreshed by its owner well
    /// inside this window; an owner that dies stops refreshing and its record
    /// ages out, which is what makes crash failover bounded without any
    /// external failure detector. `Duration::ZERO` disables expiry.
    pub ttl: Duration,
    /// JetStream replica count for the bucket (`1` for a single-node broker,
    /// `3` for a NATS cluster). This is where the control plane's durability
    /// now comes from — it replaces Raft's own quorum.
    pub num_replicas: usize,
    /// How many historical revisions to keep per key. `>= 2` so an operator
    /// can see what a fenced writer tried to do.
    pub history: i64,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_string(),
            bucket: DEFAULT_BUCKET.to_string(),
            ttl: Duration::from_secs(30),
            num_replicas: 1,
            history: 4,
        }
    }
}

impl ControlPlaneConfig {
    /// Read the configuration from the environment.
    ///
    /// Returns `None` when [`ENV_URL`] is unset — that is the "selection"
    /// signal: the JetStream control plane is off unless a URL is given, so
    /// linking the feature in does not change a deployment's behaviour.
    pub fn from_env() -> Option<Self> {
        let url = std::env::var(ENV_URL).ok().filter(|u| !u.is_empty())?;
        let mut cfg = Self {
            url,
            ..Default::default()
        };
        if let Ok(b) = std::env::var(ENV_BUCKET) {
            if !b.is_empty() {
                cfg.bucket = b;
            }
        }
        if let Some(secs) = std::env::var(ENV_TTL_SECS)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            cfg.ttl = Duration::from_secs(secs);
        }
        Some(cfg)
    }
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

/// A JetStream KV bucket holding the cluster control plane.
#[derive(Debug, Clone)]
pub struct ControlPlane {
    store: kv::Store,
    bucket: String,
}

impl ControlPlane {
    /// Connect to NATS and open (creating if absent) the control bucket.
    pub async fn connect(cfg: &ControlPlaneConfig) -> Result<Self> {
        let client = async_nats::connect(&cfg.url)
            .await
            .map_err(|e| ControlPlaneError::transport("connect", e))?;
        Self::open(async_nats::jetstream::new(client), cfg).await
    }

    /// Open (creating if absent) the control bucket on an existing JetStream
    /// context. Use this when the process already holds a NATS connection.
    pub async fn open(
        js: async_nats::jetstream::Context,
        cfg: &ControlPlaneConfig,
    ) -> Result<Self> {
        let kv_cfg = kv::Config {
            bucket: cfg.bucket.clone(),
            description: "Varpulis cluster control plane".to_string(),
            history: cfg.history.clamp(1, 64),
            max_age: cfg.ttl,
            num_replicas: cfg.num_replicas.max(1),
            ..Default::default()
        };
        let store = match js.create_key_value(kv_cfg).await {
            Ok(s) => s,
            // Already exists with different config (another coordinator won
            // the race, or an operator pre-provisioned it): bind to it.
            Err(_) => js
                .get_key_value(&cfg.bucket)
                .await
                .map_err(|e| ControlPlaneError::transport("open_bucket", e))?,
        };
        Ok(Self {
            store,
            bucket: cfg.bucket.clone(),
        })
    }

    /// The bucket this control plane is bound to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Read a key, returning its value and the revision it was read at.
    pub async fn get<T: DeserializeOwned>(&self, key: &ControlKey) -> Result<Option<Versioned<T>>> {
        let wire = key.to_string();
        let entry = self
            .store
            .entry(&wire)
            .await
            .map_err(|e| ControlPlaneError::transport("entry", e))?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        // A deleted key still yields an entry carrying the tombstone; treat it
        // as absent but keep the revision available via `revision_of`.
        if entry.operation != kv::Operation::Put {
            return Ok(None);
        }
        let value = serde_json::from_slice::<T>(&entry.value).map_err(|source| {
            ControlPlaneError::Decode {
                key: wire.clone(),
                source,
            }
        })?;
        Ok(Some(Versioned::new(value, entry.revision)))
    }

    /// The current revision of a key, including a tombstoned one.
    ///
    /// A delete does not reset the key's sequence — it appends a tombstone —
    /// so this is what a writer must CAS against after a delete, and it is why
    /// "delete then re-create" produces a *higher* revision rather than
    /// recycling the old one. That is what makes the revision usable as a
    /// monotone fencing token across a worker's death and rebirth.
    pub async fn revision_of(&self, key: &ControlKey) -> Result<Option<u64>> {
        let wire = key.to_string();
        let entry = self
            .store
            .entry(&wire)
            .await
            .map_err(|e| ControlPlaneError::transport("entry", e))?;
        Ok(entry.map(|e| e.revision))
    }

    /// Write a value subject to `expect`, returning the new revision.
    ///
    /// Fails with [`ControlPlaneError::Cas`] when the precondition does not
    /// hold. There is deliberately no "just write it" variant.
    pub async fn write<T: Serialize>(
        &self,
        key: &ControlKey,
        value: &T,
        expect: Expect,
    ) -> Result<u64> {
        let wire = key.to_string();
        let bytes = serde_json::to_vec(value).map_err(|source| ControlPlaneError::Decode {
            key: wire.clone(),
            source,
        })?;
        match expect {
            Expect::Absent => {
                self.store
                    .create(&wire, bytes.into())
                    .await
                    .map_err(|e| match e.kind() {
                        kv::CreateErrorKind::AlreadyExists => ControlPlaneError::Cas {
                            key: wire.clone(),
                            expected: expect.to_string(),
                        },
                        _ => ControlPlaneError::transport("create", e),
                    })
            }
            Expect::Revision(rev) => {
                self.store
                    .update(&wire, bytes.into(), rev)
                    .await
                    .map_err(|e| match e.kind() {
                        kv::UpdateErrorKind::WrongLastRevision => ControlPlaneError::Cas {
                            key: wire.clone(),
                            expected: expect.to_string(),
                        },
                        _ => ControlPlaneError::transport("update", e),
                    })
            }
        }
    }

    /// Unconditional write. **Not** part of the control-plane contract.
    ///
    /// This is the Raft-log semantic — "apply whatever arrives, last writer
    /// wins" — reproduced so the fail-before/pass-after harness can show a
    /// stale writer being *accepted* without it. Production code must use
    /// [`ControlPlane::write`].
    #[doc(hidden)]
    pub async fn force_put<T: Serialize>(&self, key: &ControlKey, value: &T) -> Result<u64> {
        let wire = key.to_string();
        let bytes = serde_json::to_vec(value).map_err(|source| ControlPlaneError::Decode {
            key: wire.clone(),
            source,
        })?;
        self.store
            .put(&wire, bytes.into())
            .await
            .map_err(|e| ControlPlaneError::transport("put", e))
    }

    /// Delete a key at a known revision (compare-and-delete).
    pub async fn delete_at(&self, key: &ControlKey, revision: u64) -> Result<()> {
        let wire = key.to_string();
        self.store
            .delete_expect_revision(&wire, Some(revision))
            .await
            .map_err(|e| match e.kind() {
                kv::UpdateErrorKind::WrongLastRevision => ControlPlaneError::Cas {
                    key: wire.clone(),
                    expected: revision.to_string(),
                },
                _ => ControlPlaneError::transport("delete", e),
            })
    }

    /// Delete a key regardless of revision.
    ///
    /// Used for operator-driven removals (`DeregisterWorker`, `GroupRemoved`,
    /// …) where the intent is "gone, whatever state it was in". It is still
    /// safe with respect to fencing: a delete *advances* the key's revision,
    /// so it fences every outstanding writer rather than admitting one.
    pub async fn delete(&self, key: &ControlKey) -> Result<()> {
        let wire = key.to_string();
        self.store
            .delete(&wire)
            .await
            .map_err(|e| ControlPlaneError::transport("delete", e))
    }

    /// Every live key in the bucket, as raw JSON with revisions.
    ///
    /// This is the level-triggered read the reconciler runs each tick: it
    /// re-derives the whole world from durable state rather than trusting an
    /// event stream it might have missed a message on.
    pub async fn snapshot(&self) -> Result<Snapshot> {
        // `keys()` is a `LastPerSubject` ordered consumer that reports
        // `num_pending` up front, so it terminates cleanly — including on an
        // empty bucket, where a plain `watch_all` would block forever waiting
        // for a first message. Values are then fetched per key with a direct
        // get. That is N+1 round trips; the control plane is a few hundred
        // keys, and the alternative (a wildcard history replay) cannot be
        // expressed because `>` is not a legal KV key.
        let mut names = self
            .store
            .keys()
            .await
            .map_err(|e| ControlPlaneError::transport("keys", e))?;

        let mut wire_keys: Vec<String> = Vec::new();
        while let Some(item) = names.next().await {
            match item {
                Ok(k) => wire_keys.push(k),
                Err(e) => return Err(ControlPlaneError::transport("keys_next", e)),
            }
        }

        let mut entries: BTreeMap<String, Versioned<serde_json::Value>> = BTreeMap::new();
        for wire in wire_keys {
            let entry = self
                .store
                .entry(&wire)
                .await
                .map_err(|e| ControlPlaneError::transport("entry", e))?;
            let Some(entry) = entry else { continue };
            if entry.operation != kv::Operation::Put {
                continue; // deleted between the listing and the read
            }
            // An unreadable payload skips the key rather than failing the
            // whole snapshot: one corrupt record must not blind the
            // reconciler to every other entity.
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&entry.value) {
                entries.insert(wire, Versioned::new(v, entry.revision));
            }
        }
        Ok(Snapshot { entries })
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A point-in-time read of every live control-plane key.
///
/// Not a globally-consistent cut across keys — JetStream KV offers no such
/// thing — and the reconciler does not need one; see the module docs for why.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub entries: BTreeMap<String, Versioned<serde_json::Value>>,
}

impl Snapshot {
    /// Number of live keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Raw entry for a key.
    pub fn raw(&self, key: &ControlKey) -> Option<&Versioned<serde_json::Value>> {
        self.entries.get(&key.to_string())
    }

    /// Typed read of a key.
    pub fn get<T: DeserializeOwned>(&self, key: &ControlKey) -> Option<Versioned<T>> {
        let raw = self.raw(key)?;
        let value = serde_json::from_value::<T>(raw.value.clone()).ok()?;
        Some(Versioned::new(value, raw.revision))
    }

    /// Every typed key in a namespace, paired with its value and revision.
    ///
    /// Keys that fail to parse or decode are skipped: a bucket written by a
    /// newer version must degrade to "I don't know about that", never to a
    /// panic or a wrong answer.
    pub fn iter_namespace<'a, T: DeserializeOwned + 'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (ControlKey, Versioned<T>)> + 'a {
        self.entries.iter().filter_map(move |(wire, raw)| {
            let key = ControlKey::parse(wire)?;
            if key.prefix() != prefix {
                return None;
            }
            let value = serde_json::from_value::<T>(raw.value.clone()).ok()?;
            Some((key, Versioned::new(value, raw.revision)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expect_renders_for_error_messages() {
        assert_eq!(Expect::Absent.to_string(), "absent");
        assert_eq!(Expect::Revision(7).to_string(), "7");
    }

    #[test]
    fn cas_errors_are_distinguishable_from_transport_errors() {
        let cas = ControlPlaneError::Cas {
            key: "workers/w1".into(),
            expected: "3".into(),
        };
        assert!(cas.is_cas_conflict());
        let transport = ControlPlaneError::Decode {
            key: "workers/w1".into(),
            source: serde_json::from_str::<u64>("nope").unwrap_err(),
        };
        assert!(!transport.is_cas_conflict());
    }

    #[test]
    fn from_env_is_off_unless_a_url_is_set() {
        // The selection contract: no URL ⇒ no JetStream control plane.
        // (Read the real env; CI does not set it.)
        if std::env::var(ENV_URL).is_err() {
            assert!(ControlPlaneConfig::from_env().is_none());
        }
    }

    #[test]
    fn snapshot_namespace_iteration_skips_unknown_prefixes() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "workers/w1".to_string(),
            Versioned::new(serde_json::json!({"id": "w1"}), 3),
        );
        entries.insert(
            "future/thing".to_string(),
            Versioned::new(serde_json::json!({"x": 1}), 4),
        );
        let snap = Snapshot { entries };
        let found: Vec<_> = snap
            .iter_namespace::<serde_json::Value>(super::super::keys::WORKERS)
            .collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, ControlKey::Worker("w1".into()));
        assert_eq!(found[0].1.revision, 3);
    }
}
