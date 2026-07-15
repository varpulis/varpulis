//! Streaming JSON → [`Event`] decoder with field-name interning.
//!
//! [`helpers::json_to_event`](crate::helpers::json_to_event) first parses the
//! payload into an intermediate `serde_json::Value` tree, then walks that tree
//! allocating a second copy of every key and string. At connector throughput
//! (100k+ events/sec) that intermediate tree is the single largest source of
//! allocations on the ingest path.
//!
//! [`EventDecoder`] instead deserializes the payload **directly** into an
//! [`Event`] via [`serde::de::DeserializeSeed`]:
//!
//! - Field names and event types are interned in per-decoder caches, so a
//!   steady-state stream (same schema on every record) performs zero key
//!   allocations — each key is an `Arc<str>` clone of the cached entry.
//! - String values are copied once, straight from the deserializer's borrowed
//!   slice into the `Box<str>` the [`Value`] keeps.
//! - The same resource limits as `json_to_event` are enforced
//!   ([`limits::MAX_FIELDS_PER_EVENT`], [`limits::MAX_STRING_VALUE_BYTES`],
//!   [`limits::MAX_JSON_DEPTH`], [`limits::MAX_ARRAY_ELEMENTS`]), and the
//!   event-time extraction rules are identical (`@timestamp` RFC3339, then
//!   `ts`, then `timestamp` as epoch milliseconds).
//!
//! The decoder is intentionally `&mut self` and cache-carrying: create one per
//! connector consume task and reuse it for every record.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rustc_hash::{FxBuildHasher, FxHashMap};
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use tracing::warn;
use varpulis_core::{Event, Value};

use crate::limits;

/// Interning caches stop growing past this many distinct entries to bound
/// memory under adversarial (random-key) input. Entries past the cap still
/// decode correctly — they just allocate instead of hitting the cache.
const MAX_INTERNED_ENTRIES: usize = 4_096;

/// Keys longer than this are never interned (unlikely to repeat).
const MAX_INTERNED_KEY_LEN: usize = 128;

/// How a top-level key participates in event-time extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyTag {
    Plain,
    EventType,
    AtTimestamp,
    Ts,
    Timestamp,
}

fn classify_key(key: &str) -> KeyTag {
    match key {
        "event_type" => KeyTag::EventType,
        "@timestamp" => KeyTag::AtTimestamp,
        "ts" => KeyTag::Ts,
        "timestamp" => KeyTag::Timestamp,
        _ => KeyTag::Plain,
    }
}

/// Reusable JSON → [`Event`] decoder. One instance per consume task; the
/// interning caches make steady-state decoding key-allocation-free.
#[derive(Debug)]
pub struct EventDecoder {
    /// Field-name cache (all nesting levels share it).
    keys: FxHashMap<Box<str>, (Arc<str>, KeyTag)>,
    /// Event-type cache (`event_type` payload values and connector defaults).
    types: FxHashMap<Box<str>, Arc<str>>,
    /// Field count of the last decoded event — used to pre-size the next
    /// event's map, since streams overwhelmingly carry a fixed schema.
    capacity_hint: usize,
}

impl Default for EventDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl EventDecoder {
    /// Create an empty decoder.
    pub fn new() -> Self {
        Self {
            keys: FxHashMap::default(),
            types: FxHashMap::default(),
            capacity_hint: 8,
        }
    }

    /// Decode a JSON payload into an [`Event`].
    ///
    /// `default_event_type` is used when the payload has no string
    /// `event_type` field. Returns `Err` on malformed JSON (including
    /// trailing garbage, matching `serde_json::from_slice` strictness).
    pub fn decode(
        &mut self,
        default_event_type: &str,
        payload: &[u8],
    ) -> Result<Event, serde_json::Error> {
        let mut de = serde_json::Deserializer::from_slice(payload);
        let event = EventSeed {
            decoder: self,
            default_event_type,
        }
        .deserialize(&mut de)?;
        de.end()?;
        Ok(event)
    }

    fn intern_key(&mut self, raw: &str) -> (Arc<str>, KeyTag) {
        if let Some(entry) = self.keys.get(raw) {
            return entry.clone();
        }
        let tag = classify_key(raw);
        let arc: Arc<str> = Arc::from(raw);
        if raw.len() <= MAX_INTERNED_KEY_LEN && self.keys.len() < MAX_INTERNED_ENTRIES {
            self.keys.insert(Box::from(raw), (arc.clone(), tag));
        }
        (arc, tag)
    }

    fn intern_type(&mut self, raw: &str) -> Arc<str> {
        if let Some(arc) = self.types.get(raw) {
            return arc.clone();
        }
        let arc: Arc<str> = Arc::from(raw);
        if raw.len() <= MAX_INTERNED_KEY_LEN && self.types.len() < MAX_INTERNED_ENTRIES {
            self.types.insert(Box::from(raw), arc.clone());
        }
        arc
    }
}

// ---------------------------------------------------------------------------
// Top-level seed: a JSON document → Event
// ---------------------------------------------------------------------------

struct EventSeed<'a, 'b> {
    decoder: &'a mut EventDecoder,
    default_event_type: &'b str,
}

impl<'de> DeserializeSeed<'de> for EventSeed<'_, '_> {
    type Value = Event;

    fn deserialize<D>(self, deserializer: D) -> Result<Event, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for EventSeed<'_, '_> {
    type Value = Event;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }

    // Non-object payloads decode to an empty event with the default type,
    // matching `json_to_event`'s `as_object() else Event::new(..)` fallback.
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Event, E> {
        Ok(self.empty_event())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Event, E> {
        Ok(self.empty_event())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Event, E> {
        Ok(self.empty_event())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Event, E> {
        Ok(self.empty_event())
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Event, E> {
        Ok(self.empty_event())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Event, E> {
        Ok(self.empty_event())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Event, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(self.empty_event())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Event, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut data: IndexMap<Arc<str>, Value, FxBuildHasher> =
            IndexMap::with_capacity_and_hasher(self.decoder.capacity_hint, FxBuildHasher);
        let mut event_type: Option<Arc<str>> = None;
        let mut at_timestamp: Option<DateTime<Utc>> = None;
        let mut ts_millis: Option<i64> = None;
        let mut timestamp_millis: Option<i64> = None;
        let mut overflowed = false;

        while let Some((key, tag)) = map.next_key_seed(KeySeed {
            decoder: self.decoder,
        })? {
            if tag == KeyTag::EventType {
                // Consumed but never stored as a data field. Non-string
                // values fall back to the default event type.
                if let Some(ty) = map.next_value_seed(TypeSeed {
                    decoder: self.decoder,
                })? {
                    event_type = Some(ty);
                }
                continue;
            }

            if data.len() >= limits::MAX_FIELDS_PER_EVENT {
                // Cap reached: consume and drop the remaining fields.
                if !overflowed {
                    overflowed = true;
                    warn!(
                        "Event exceeded max field count ({}), remaining fields dropped",
                        limits::MAX_FIELDS_PER_EVENT
                    );
                }
                map.next_value::<IgnoredAny>()?;
                continue;
            }

            let Some(value) = map.next_value_seed(ValueSeed {
                decoder: self.decoder,
                depth: limits::MAX_JSON_DEPTH,
            })?
            else {
                continue;
            };

            match tag {
                KeyTag::AtTimestamp => {
                    if let Value::Str(s) = &value {
                        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                            at_timestamp = Some(dt.with_timezone(&Utc));
                        }
                    }
                }
                KeyTag::Ts => {
                    if let Value::Int(n) = value {
                        ts_millis = Some(n);
                    }
                }
                KeyTag::Timestamp => {
                    if let Value::Int(n) = value {
                        timestamp_millis = Some(n);
                    }
                }
                KeyTag::Plain | KeyTag::EventType => {}
            }

            data.insert(key, value);
        }

        self.decoder.capacity_hint = data.len().clamp(4, limits::MAX_FIELDS_PER_EVENT);

        // Event-time priority: @timestamp (RFC3339), then ts, then timestamp
        // (both epoch millis) — identical to helpers::extract_event_time.
        let event_time = at_timestamp.or_else(|| {
            ts_millis
                .or(timestamp_millis)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
        });

        let event_type =
            event_type.unwrap_or_else(|| self.decoder.intern_type(self.default_event_type));

        Ok(match event_time {
            Some(ts) => Event::from_fields_with_timestamp(event_type, ts, data),
            None => Event::from_fields(event_type, data),
        })
    }
}

impl EventSeed<'_, '_> {
    fn empty_event(self) -> Event {
        Event::new(self.decoder.intern_type(self.default_event_type))
    }
}

// ---------------------------------------------------------------------------
// Key seed: interns a map key
// ---------------------------------------------------------------------------

struct KeySeed<'a> {
    decoder: &'a mut EventDecoder,
}

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = (Arc<str>, KeyTag);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for KeySeed<'_> {
    type Value = (Arc<str>, KeyTag);

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a field name")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(self.decoder.intern_key(v))
    }
}

// ---------------------------------------------------------------------------
// Type seed: interns a string `event_type` value; `None` for non-strings
// ---------------------------------------------------------------------------

struct TypeSeed<'a> {
    decoder: &'a mut EventDecoder,
}

impl<'de> DeserializeSeed<'de> for TypeSeed<'_> {
    type Value = Option<Arc<str>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for TypeSeed<'_> {
    type Value = Option<Arc<str>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an event type string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(Some(self.decoder.intern_type(v)))
    }

    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Value seed: JSON value → varpulis Value with depth/size limits
// ---------------------------------------------------------------------------

struct ValueSeed<'a> {
    decoder: &'a mut EventDecoder,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    /// `None` means "dropped by a resource limit" — the caller skips the field.
    type Value = Option<Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if self.depth == 0 {
            IgnoredAny::deserialize(deserializer)?;
            return Ok(None);
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValueSeed<'_> {
    type Value = Option<Value>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(Some(Value::Bool(v)))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(Some(Value::Int(v)))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
        // Same fallback as `serde_json::Number::as_i64().or(as_f64())`.
        Ok(Some(
            i64::try_from(v).map_or(Value::Float(v as f64), Value::Int),
        ))
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(Some(Value::Float(v)))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        if v.len() > limits::MAX_STRING_VALUE_BYTES {
            warn!(
                len = v.len(),
                "String value exceeds max size ({}), truncated",
                limits::MAX_STRING_VALUE_BYTES
            );
            let truncated = &v[..v.floor_char_boundary(limits::MAX_STRING_VALUE_BYTES)];
            Ok(Some(Value::Str(truncated.into())))
        } else {
            Ok(Some(Value::Str(v.into())))
        }
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(Some(Value::Null))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(4).min(64));
        let mut seen = 0usize;
        loop {
            if seen >= limits::MAX_ARRAY_ELEMENTS {
                let mut extra = 0usize;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    extra += 1;
                }
                if extra > 0 {
                    warn!(
                        len = seen + extra,
                        "Array exceeds max elements ({}), truncated",
                        limits::MAX_ARRAY_ELEMENTS
                    );
                }
                break;
            }
            let Some(element) = seq.next_element_seed(ValueSeed {
                decoder: self.decoder,
                depth: self.depth - 1,
            })?
            else {
                break;
            };
            seen += 1;
            if let Some(v) = element {
                values.push(v);
            }
        }
        Ok(Some(Value::array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut nested: IndexMap<Arc<str>, Value, FxBuildHasher> =
            IndexMap::with_capacity_and_hasher(map.size_hint().unwrap_or(4).min(64), FxBuildHasher);
        while let Some((key, _)) = map.next_key_seed(KeySeed {
            decoder: self.decoder,
        })? {
            if nested.len() >= limits::MAX_FIELDS_PER_EVENT {
                map.next_value::<IgnoredAny>()?;
                continue;
            }
            if let Some(v) = map.next_value_seed(ValueSeed {
                decoder: self.decoder,
                depth: self.depth - 1,
            })? {
                nested.insert(key, v);
            }
        }
        Ok(Some(Value::map(nested)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::json_to_event;

    fn decode(payload: &str) -> Event {
        EventDecoder::new()
            .decode("KafkaEvent", payload.as_bytes())
            .unwrap()
    }

    /// The decoder must produce the same Event as the legacy two-pass path.
    fn assert_equivalent(payload: &str) {
        let new = decode(payload);
        let json: serde_json::Value = serde_json::from_slice(payload.as_bytes()).unwrap();
        let old = json_to_event(
            json.get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("KafkaEvent"),
            &json,
        );
        assert_eq!(new.event_type, old.event_type, "event_type for {payload}");
        assert_eq!(new.data, old.data, "data for {payload}");
        // Timestamps only comparable when the payload pins one; otherwise
        // both fall back to (different) Utc::now() readings.
        if payload.contains("@timestamp") || payload.contains("\"ts\"") {
            assert_eq!(new.timestamp, old.timestamp, "timestamp for {payload}");
        }
    }

    #[test]
    fn equivalent_to_json_to_event() {
        assert_equivalent(
            r#"{"ts": 1775990400000, "symbol": "AAPL", "price": 50.5, "volume": 100}"#,
        );
        assert_equivalent(r#"{"event_type": "Tick", "price": 1}"#);
        assert_equivalent(r#"{"@timestamp": "2030-01-01T00:00:00Z", "device_id": "dev_0"}"#);
        assert_equivalent(
            r#"{"@timestamp": "2030-01-01T00:00:00Z", "ts": 1775990400000, "device_id": "dev_0"}"#,
        );
        assert_equivalent(r#"{"nested": {"a": 1, "b": [1, 2.5, "x", null, true]}, "ts": 5}"#);
        assert_equivalent(r#"{"neg": -42, "big": 18446744073709551615, "f": 1e10, "ts": 1}"#);
        assert_equivalent(r#"{"event_type": 123, "x": 1, "ts": 2}"#);
        assert_equivalent("{}");
        assert_equivalent("[1, 2, 3]");
        assert_equivalent(r#""just a string""#);
        assert_equivalent("42");
        assert_equivalent("null");
        assert_equivalent(r#"{"esc\"aped": "va\"lue", "ts": 3}"#);
    }

    #[test]
    fn timestamp_field_priority() {
        let e = decode(r#"{"timestamp": 1000, "ts": 2000}"#);
        assert_eq!(
            e.timestamp.timestamp_millis(),
            2000,
            "ts wins over timestamp"
        );
        let e = decode(r#"{"timestamp": 1000}"#);
        assert_eq!(e.timestamp.timestamp_millis(), 1000);
    }

    #[test]
    fn interning_reuses_keys_and_types() {
        let mut dec = EventDecoder::new();
        let a = dec
            .decode("T", br#"{"event_type": "Tick", "price": 1}"#)
            .unwrap();
        let b = dec
            .decode("T", br#"{"event_type": "Tick", "price": 2}"#)
            .unwrap();
        assert!(Arc::ptr_eq(&a.event_type, &b.event_type));
        let (ka, _) = a.data.get_key_value("price").unwrap();
        let (kb, _) = b.data.get_key_value("price").unwrap();
        assert!(Arc::ptr_eq(ka, kb));
    }

    #[test]
    fn malformed_json_is_an_error() {
        let mut dec = EventDecoder::new();
        assert!(dec.decode("T", b"{not json").is_err());
        assert!(dec.decode("T", b"{\"a\": 1} trailing").is_err());
        assert!(dec.decode("T", b"").is_err());
    }

    #[test]
    fn field_cap_drops_excess_fields() {
        let mut payload = String::from("{");
        for i in 0..(limits::MAX_FIELDS_PER_EVENT + 10) {
            if i > 0 {
                payload.push(',');
            }
            payload.push_str(&format!("\"k{i}\": {i}"));
        }
        payload.push('}');
        let e = decode(&payload);
        assert_eq!(e.data.len(), limits::MAX_FIELDS_PER_EVENT);
    }

    #[test]
    fn depth_cap_drops_deep_subtrees() {
        // Build nesting deeper than MAX_JSON_DEPTH: {"a":{"a":{... 40 deep}}}
        let mut payload = String::new();
        for _ in 0..40 {
            payload.push_str("{\"a\":");
        }
        payload.push('1');
        payload.push_str(&"}".repeat(40));
        let e = decode(&payload);
        // Must parse without stack overflow; the over-deep subtree is dropped.
        assert!(e.data.contains_key("a"));
    }

    #[test]
    fn long_string_truncated() {
        let long = "x".repeat(limits::MAX_STRING_VALUE_BYTES + 10);
        let e = decode(&format!(r#"{{"s": "{long}"}}"#));
        match e.data.get("s").unwrap() {
            Value::Str(s) => assert_eq!(s.len(), limits::MAX_STRING_VALUE_BYTES),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn event_type_not_stored_in_data() {
        let e = decode(r#"{"event_type": "Tick", "price": 1}"#);
        assert_eq!(&*e.event_type, "Tick");
        assert!(!e.data.contains_key("event_type"));
    }
}
