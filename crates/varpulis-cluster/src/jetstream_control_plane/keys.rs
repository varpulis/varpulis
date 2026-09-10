//! Key layout for the JetStream KV control plane.
//!
//! ```text
//! workers/<id>          RegisterWorker, DeregisterWorker, WorkerStatusChanged,
//!                       WorkerPipelinesUpdated, WorkerMetricsUpdated
//! groups/<name>         GroupDeployed, GroupUpdated, GroupRemoved
//! connectors/<name>     ConnectorCreated, ConnectorUpdated, ConnectorRemoved
//! migrations/<id>       MigrationStarted, MigrationUpdated, MigrationRemoved
//! checkpoints/<group>   CheckpointCompleted, CheckpointAborted
//! models/<name>         ModelRegistered, ModelRemoved
//! scaling/policy        ScalingPolicySet
//! control/leader        coordinator lease (see [`super::leader`])
//! ```
//!
//! ## Why `/` and not `.`
//!
//! NATS KV accepts `[-/_=.a-zA-Z0-9]` in a key, and `.` is the *subject*
//! separator. Using `/` keeps every key a **single subject token**, which
//! means a hostile id can never inject extra subject levels and escape its
//! namespace — an id is escaped into the token, not spread across tokens.
//! The cost is that a server-side prefix wildcard (`workers.*`) is not
//! available; the reconciler does not need one because it watches the whole
//! bucket (`>`) and filters in-process. The control plane is a few hundred
//! keys, so this is not a scaling concern.
//!
//! ## Id escaping
//!
//! Worker ids, group names and connector names come from user input (VPL,
//! registration payloads) and may contain characters NATS rejects. Each id is
//! escaped into `[-_.a-zA-Z0-9]` — note: **no `/`** — with `=XX` hex escapes.
//! The mapping is injective, so `decode(encode(x)) == x` for every `x`, and
//! distinct ids can never collide onto one key.

use std::fmt;

/// Namespace prefixes. Kept as constants so the reconciler, the command
/// mapper and the snapshot loader cannot drift apart.
pub const WORKERS: &str = "workers";
pub const GROUPS: &str = "groups";
pub const CONNECTORS: &str = "connectors";
pub const MIGRATIONS: &str = "migrations";
pub const CHECKPOINTS: &str = "checkpoints";
pub const MODELS: &str = "models";
pub const SCALING: &str = "scaling";
pub const CONTROL: &str = "control";

/// The singleton scaling-policy key (`scaling/policy`).
pub const SCALING_POLICY: &str = "scaling/policy";
/// The coordinator lease key (`control/leader`).
pub const LEADER: &str = "control/leader";

/// A typed control-plane key.
///
/// `Display` renders the wire key (with the id escaped);
/// [`ControlKey::parse`] recovers the typed form from a wire key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlKey {
    Worker(String),
    Group(String),
    Connector(String),
    Migration(String),
    Checkpoint(String),
    Model(String),
    ScalingPolicy,
    Leader,
}

impl ControlKey {
    /// The namespace prefix this key lives under.
    pub fn prefix(&self) -> &'static str {
        match self {
            Self::Worker(_) => WORKERS,
            Self::Group(_) => GROUPS,
            Self::Connector(_) => CONNECTORS,
            Self::Migration(_) => MIGRATIONS,
            Self::Checkpoint(_) => CHECKPOINTS,
            Self::Model(_) => MODELS,
            Self::ScalingPolicy => SCALING,
            Self::Leader => CONTROL,
        }
    }

    /// The unescaped entity id, or `None` for the singleton keys.
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::Worker(v)
            | Self::Group(v)
            | Self::Connector(v)
            | Self::Migration(v)
            | Self::Checkpoint(v)
            | Self::Model(v) => Some(v),
            Self::ScalingPolicy | Self::Leader => None,
        }
    }

    /// Parse a wire key back into its typed form.
    ///
    /// Returns `None` for keys outside the layout above (an unknown prefix, a
    /// missing id, or an id whose escaping is malformed) — the snapshot loader
    /// skips those rather than guessing, so a bucket shared with a future
    /// version does not corrupt this one's view.
    pub fn parse(wire: &str) -> Option<Self> {
        if wire == SCALING_POLICY {
            return Some(Self::ScalingPolicy);
        }
        if wire == LEADER {
            return Some(Self::Leader);
        }
        let (prefix, encoded) = wire.split_once('/')?;
        if encoded.is_empty() || encoded.contains('/') {
            return None;
        }
        let id = decode_id(encoded)?;
        Some(match prefix {
            WORKERS => Self::Worker(id),
            GROUPS => Self::Group(id),
            CONNECTORS => Self::Connector(id),
            MIGRATIONS => Self::Migration(id),
            CHECKPOINTS => Self::Checkpoint(id),
            MODELS => Self::Model(id),
            _ => return None,
        })
    }
}

impl fmt::Display for ControlKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScalingPolicy => f.write_str(SCALING_POLICY),
            Self::Leader => f.write_str(LEADER),
            other => {
                let id = other.id().unwrap_or_default();
                write!(f, "{}/{}", other.prefix(), encode_id(id))
            }
        }
    }
}

/// Escape an entity id into the NATS-KV-safe alphabet `[-_.a-zA-Z0-9]`.
///
/// `/` and `=` and everything outside the alphabet become `=XX` (upper-hex of
/// each UTF-8 byte). The result never contains `/`, so a key produced by
/// [`ControlKey`]'s `Display` always has exactly two `/`-separated segments.
///
/// A **trailing** `.` is escaped too: NATS rejects a key that ends with `.`,
/// and the id is the tail of the key, so `id = "w."` would otherwise produce
/// the invalid key `workers/w.`.
pub fn encode_id(id: &str) -> String {
    let mut out = String::with_capacity(id.len());
    let last = id.len().saturating_sub(1);
    for (i, b) in id.bytes().enumerate() {
        let safe = b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || (b == b'.' && i != last);
        if safe {
            out.push(b as char);
        } else {
            out.push('=');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// Inverse of [`encode_id`]. `None` when the escaping is malformed.
pub fn decode_id(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'=' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = (bytes[i + 1] as char).to_digit(16)? as u8;
            let lo = (bytes[i + 2] as char).to_digit(16)? as u8;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_plain_ids() {
        for id in ["w1", "worker-0", "group_a", "a.b.c", "UPPER123"] {
            assert_eq!(encode_id(id), id, "plain ids must pass through unescaped");
            assert_eq!(decode_id(&encode_id(id)).as_deref(), Some(id));
        }
    }

    #[test]
    fn escapes_characters_nats_rejects() {
        // Space, `*`, `>` and `$` are all illegal or dangerous in a subject.
        assert_eq!(encode_id("a b"), "a=20b");
        assert_eq!(encode_id("a*b"), "a=2Ab");
        assert_eq!(encode_id("a>b"), "a=3Eb");
        assert_eq!(encode_id("$sys"), "=24sys");
    }

    #[test]
    fn escapes_the_namespace_separator_so_ids_cannot_escape_their_prefix() {
        // A worker calling itself "../control/leader" must not be able to
        // write the coordinator lease key.
        let hostile = "../control/leader";
        let key = ControlKey::Worker(hostile.to_string()).to_string();
        assert!(
            !key["workers/".len()..].contains('/'),
            "id leaked a `/`: {key}"
        );
        assert_eq!(key, "workers/..=2Fcontrol=2Fleader");
        assert_eq!(
            ControlKey::parse(&key),
            Some(ControlKey::Worker(hostile.to_string()))
        );
    }

    #[test]
    fn escapes_a_trailing_dot_because_nats_rejects_keys_ending_in_one() {
        assert_eq!(encode_id("w."), "w=2E");
        assert_eq!(ControlKey::Worker("w.".into()).to_string(), "workers/w=2E");
        assert_eq!(decode_id("w=2E").as_deref(), Some("w."));
        // A non-trailing dot stays readable.
        assert_eq!(encode_id("a.b"), "a.b");
    }

    #[test]
    fn escape_marker_is_itself_escaped_so_encoding_is_injective() {
        // "a=20b" must not decode to "a b": if it did, two distinct ids would
        // share one key.
        let literal = "a=20b";
        assert_eq!(encode_id(literal), "a=3D20b");
        assert_eq!(decode_id(&encode_id(literal)).as_deref(), Some(literal));
        assert_ne!(encode_id(literal), encode_id("a b"));
    }

    #[test]
    fn round_trips_non_ascii() {
        let id = "wörker-日本";
        assert_eq!(decode_id(&encode_id(id)).as_deref(), Some(id));
    }

    #[test]
    fn parses_every_namespace() {
        let cases = [
            (ControlKey::Worker("w1".into()), "workers/w1"),
            (ControlKey::Group("g1".into()), "groups/g1"),
            (ControlKey::Connector("c1".into()), "connectors/c1"),
            (ControlKey::Migration("m1".into()), "migrations/m1"),
            (ControlKey::Checkpoint("g1".into()), "checkpoints/g1"),
            (ControlKey::Model("m".into()), "models/m"),
            (ControlKey::ScalingPolicy, "scaling/policy"),
            (ControlKey::Leader, "control/leader"),
        ];
        for (typed, wire) in cases {
            assert_eq!(typed.to_string(), wire);
            assert_eq!(ControlKey::parse(wire), Some(typed));
        }
    }

    #[test]
    fn rejects_unknown_and_malformed_keys() {
        assert_eq!(ControlKey::parse("nope/x"), None);
        assert_eq!(ControlKey::parse("workers"), None);
        assert_eq!(ControlKey::parse("workers/"), None);
        assert_eq!(ControlKey::parse("workers/a/b"), None);
        assert_eq!(ControlKey::parse("workers/=Z0"), None);
        assert_eq!(ControlKey::parse("workers/=2"), None);
    }

    #[test]
    fn encoded_keys_are_valid_nats_kv_keys() {
        // Mirrors async_nats::jetstream::kv::is_valid_key: non-empty, no
        // leading/trailing `.`, and only `[-/_=.a-zA-Z0-9]`.
        let ids = [
            "w1",
            "a b",
            "$sys",
            "../control/leader",
            "wörker-日本",
            "x=y",
            "w.",
            ".",
        ];
        for id in ids {
            let key = ControlKey::Worker(id.to_string()).to_string();
            assert!(!key.is_empty());
            assert!(!key.starts_with('.') && !key.ends_with('.'), "{key}");
            assert!(
                key.bytes()
                    .all(|b| b.is_ascii_alphanumeric()
                        || matches!(b, b'-' | b'/' | b'_' | b'=' | b'.')),
                "key {key} contains a byte NATS KV rejects"
            );
        }
    }
}
