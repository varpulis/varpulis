//! Shared security utilities for Varpulis.
//!
//! Provides constant-time string comparison, common body-size limits,
//! and a [`SecretString`] wrapper that zeroizes memory on drop.

use std::fmt;
use std::hash::{Hash, Hasher};

use zeroize::Zeroize;

/// Maximum body size for normal JSON endpoints (1 MB).
pub const JSON_BODY_LIMIT: u64 = 1024 * 1024;

/// Maximum body size for large payloads: inject-batch, models, snapshots (16 MB).
pub const LARGE_BODY_LIMIT: u64 = 16 * 1024 * 1024;

/// A string that zeroizes its contents when dropped.
///
/// Use this for passwords, API keys, private keys, and other secrets
/// to prevent credential leakage from memory dumps / core files.
///
/// Implements `Deref<Target=str>` for ergonomic use but **never** prints
/// the secret value in Debug/Display (always shows `[REDACTED]`).
#[derive(Clone, Zeroize)]
#[zeroize(drop)]
pub struct SecretString(String);

impl SecretString {
    /// Creates a new secret string from the given value.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Expose the secret value.  Use sparingly — only when the value
    /// must be compared or transmitted.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl PartialEq for SecretString {
    fn eq(&self, other: &Self) -> bool {
        constant_time_compare(&self.0, &other.0)
    }
}

impl Eq for SecretString {}

impl Hash for SecretString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SecretString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl serde::Serialize for SecretString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("[REDACTED]")
    }
}

impl<'de> serde::Deserialize<'de> for SecretString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(SecretString)
    }
}

impl schemars::JsonSchema for SecretString {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("SecretString")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Appears as a plain string in the schema
        String::json_schema(generator)
    }
}

/// Constant-time string comparison to prevent timing attacks.
///
/// Unlike a naive implementation, this function does **not** short-circuit on
/// mismatched lengths — it always iterates over the longer of the two inputs
/// so that an attacker cannot learn the expected key length via timing.
pub fn constant_time_compare(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();

    let len = a.len().max(b.len());

    // Lengths differ → mismatch, but we still iterate to avoid leaking length.
    let mut result = (a.len() != b.len()) as u8;

    for i in 0..len {
        let x = if i < a.len() { a[i] } else { 0 };
        let y = if i < b.len() { b[i] } else { 0 };
        result |= x ^ y;
    }

    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equal_strings() {
        assert!(constant_time_compare("abc", "abc"));
        assert!(constant_time_compare("", ""));
        assert!(constant_time_compare(
            "longer-string-123",
            "longer-string-123"
        ));
    }

    #[test]
    fn test_different_strings() {
        assert!(!constant_time_compare("abc", "abd"));
    }

    #[test]
    fn test_different_lengths() {
        assert!(!constant_time_compare("abc", "ab"));
        assert!(!constant_time_compare("abc", "abcd"));
        assert!(!constant_time_compare("", "a"));
        assert!(!constant_time_compare("a", ""));
    }

    #[test]
    fn test_body_limits() {
        assert_eq!(JSON_BODY_LIMIT, 1024 * 1024);
        assert_eq!(LARGE_BODY_LIMIT, 16 * 1024 * 1024);
    }
}

/// Whether a configuration key names a secret.
///
/// One predicate for the whole workspace. There used to be two that disagreed —
/// `varpulis-connectors::credentials::is_sensitive_field` and
/// `varpulis-connector-api::types::is_secret_key` — and between them they
/// missed `webhook_url` (for Slack the URL *is* the credential), `auth`, `pwd`,
/// `credential`, `dsn` and `connection_string`. A key that one caller redacts
/// and another does not is worse than no redaction, because the operator
/// believes the control applies everywhere.
///
/// `*_location` is deliberately excluded: `ssl_ca_location` and friends name a
/// path on disk, not a secret, and redacting them makes a misconfiguration
/// impossible to diagnose.
#[must_use]
pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    if k.ends_with("_location") {
        return false;
    }
    const NEEDLES: &[&str] = &[
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "apikey",
        "api_key",
        "credential",
        "webhook_url",
        "connection_string",
        "dsn",
        "private_key",
        "auth",
    ];
    NEEDLES.iter().any(|n| k.contains(n)) || k.contains("key")
}

#[cfg(test)]
mod secret_key_tests {
    use super::is_secret_key;

    #[test]
    fn catches_what_the_two_old_predicates_missed_between_them() {
        for k in [
            "webhook_url",
            "auth",
            "pwd",
            "credential",
            "dsn",
            "connection_string",
            "sasl_password",
            "api_key",
            "apiKey",
            "ssl_key_password",
            "private_key",
        ] {
            assert!(is_secret_key(k), "`{k}` must be treated as a secret");
        }
    }

    #[test]
    fn leaves_paths_and_plain_settings_alone() {
        for k in [
            "ssl_ca_location",
            "ssl_certificate_location",
            "ssl_key_location",
            "bootstrap_servers",
            "topic",
            "group_id",
            "host",
            "port",
            "username",
        ] {
            assert!(
                !is_secret_key(k),
                "`{k}` is not a secret; redacting it hides misconfiguration"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// VPL source redaction
// ---------------------------------------------------------------------------

/// The value substituted for a secret when a VPL source is rendered for a
/// client. Deliberately not a usable credential, and easy to grep for.
pub const REDACTED_VALUE: &str = "[REDACTED]";

/// Redact secret parameter values inside VPL `connector` declarations.
///
/// A cluster-level named connector is rendered back into VPL as
/// `connector kafka_signals = kafka(brokers: "…", sasl_password: "…")` and
/// prepended to a tenant's pipeline source before deployment. That enriched
/// source is what gets stored, and it is what the pipeline read APIs return —
/// so a tenant who *avoided* inline secrets by using a named connector still
/// got those secrets handed back by `GET /api/v1/pipelines/{id}`.
///
/// The redaction belongs on the serialised shape, not on `Debug`. `Debug`
/// protects the logs; `Serialize` is what feeds the REST API, and applying the
/// control only to the former is the mistake this codebase kept making.
///
/// Scope is deliberately narrow — only the parenthesised parameter list of a
/// `connector` declaration. VPL uses secret-looking identifiers elsewhere for
/// entirely non-secret things (`registry_key:` in a Sysmon detection's `emit`
/// record, `key:` as the join key of `.enrich(…)`), and rewriting those would
/// corrupt the detection logic the operator is trying to read.
#[must_use]
pub fn redact_vpl_secrets(source: &str) -> String {
    let spans = connector_secret_params(source);
    if spans.is_empty() {
        return source.to_string();
    }
    let mut out = String::with_capacity(source.len() + spans.len() * REDACTED_VALUE.len());
    let mut last = 0usize;
    for span in &spans {
        out.push_str(&source[last..span.value.start]);
        out.push('"');
        out.push_str(REDACTED_VALUE);
        out.push('"');
        last = span.value.end;
    }
    out.push_str(&source[last..]);
    out
}

/// Names of the secret parameters that carry a literal value inside a
/// `connector` declaration in `source`.
///
/// Used to refuse input that would fan a credential out to readers who must not
/// see it — a global pipeline template is copied verbatim into every tenant, so
/// one inline SASL password in it is handed to all of them.
#[must_use]
pub fn vpl_inline_secret_params(source: &str) -> Vec<String> {
    let mut names: Vec<String> = connector_secret_params(source)
        .into_iter()
        .filter(|s| !source[s.value.clone()].trim_matches('"').is_empty())
        .map(|s| source[s.key].to_string())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Whether `source` carries a redaction placeholder where a credential belongs.
///
/// A client that reads a redacted source and posts it straight back would
/// otherwise deploy `[REDACTED]` as the password and fail at connect time with
/// no indication why. Callers reject such a source instead.
#[must_use]
pub fn vpl_has_redacted_secret(source: &str) -> bool {
    connector_secret_params(source)
        .into_iter()
        .any(|s| source[s.value].trim_matches('"') == REDACTED_VALUE)
}

/// A secret parameter located inside a `connector` declaration.
struct SecretParamSpan {
    key: std::ops::Range<usize>,
    value: std::ops::Range<usize>,
}

/// Locate `key: value` pairs whose key names a secret and which sit inside the
/// argument list of a `connector <name> = <type>( … )` declaration.
fn connector_secret_params(source: &str) -> Vec<SecretParamSpan> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut spans = Vec::new();
    let mut brackets: Vec<u8> = Vec::new();
    // Depth of the paren group opened by a `connector` declaration, when we are
    // inside one.
    let mut connector_depth: Option<usize> = None;
    // Saw the `connector` keyword; waiting for its `(`.
    let mut pending_connector = false;
    let mut pos = 0usize;

    while pos < len {
        match bytes[pos] {
            b'"' => pos = string_literal_end(bytes, pos),
            b'#' => pos = line_end(bytes, pos),
            b'/' if pos + 1 < len && bytes[pos + 1] == b'/' => pos = line_end(bytes, pos),
            b'(' | b'[' | b'{' => {
                brackets.push(bytes[pos]);
                if pending_connector && bytes[pos] == b'(' {
                    connector_depth = Some(brackets.len());
                }
                pending_connector = false;
                pos += 1;
            }
            b')' | b']' | b'}' => {
                if connector_depth == Some(brackets.len()) {
                    connector_depth = None;
                }
                brackets.pop();
                pending_connector = false;
                pos += 1;
            }
            byte if is_vpl_ident_byte(byte) => {
                let key_start = pos;
                while pos < len && is_vpl_ident_byte(bytes[pos]) {
                    pos += 1;
                }
                let key_end = pos;
                let ident = &source[key_start..key_end];
                if ident == "connector" && connector_depth.is_none() {
                    pending_connector = true;
                    continue;
                }
                if connector_depth != Some(brackets.len()) || !is_secret_key(ident) {
                    continue;
                }

                // `key` must be followed by `:` for this to be a parameter.
                let mut colon = pos;
                while colon < len && bytes[colon].is_ascii_whitespace() {
                    colon += 1;
                }
                if colon >= len || bytes[colon] != b':' {
                    continue;
                }

                let mut value_start = colon + 1;
                while value_start < len
                    && (bytes[value_start] == b' ' || bytes[value_start] == b'\t')
                {
                    value_start += 1;
                }
                if value_start >= len {
                    continue;
                }

                let value_end = if bytes[value_start] == b'"' {
                    string_literal_end(bytes, value_start)
                } else {
                    // A bare literal — `to_vpl_declaration` emits numeric
                    // parameter values unquoted — runs to the next separator.
                    let mut end = value_start;
                    while end < len
                        && !matches!(bytes[end], b',' | b')' | b']' | b'}' | b'\n' | b'\r')
                    {
                        end += 1;
                    }
                    while end > value_start && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\t') {
                        end -= 1;
                    }
                    end
                };

                if value_end > value_start {
                    spans.push(SecretParamSpan {
                        key: key_start..key_end,
                        value: value_start..value_end,
                    });
                    pos = value_end;
                }
            }
            _ => pos += 1,
        }
    }

    spans
}

const fn is_vpl_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Index just past the closing quote of the string literal starting at `start`,
/// or `bytes.len()` when the literal is unterminated.
fn string_literal_end(bytes: &[u8], start: usize) -> usize {
    let mut pos = start + 1;
    while pos < bytes.len() {
        match bytes[pos] {
            b'\\' => pos += 2,
            b'"' => return pos + 1,
            _ => pos += 1,
        }
    }
    bytes.len()
}

/// Index of the newline ending the line containing `start`, or `bytes.len()`.
fn line_end(bytes: &[u8], start: usize) -> usize {
    let mut pos = start;
    while pos < bytes.len() && bytes[pos] != b'\n' {
        pos += 1;
    }
    pos
}

#[cfg(test)]
mod vpl_redaction_tests {
    use super::*;

    const INJECTED: &str = concat!(
        "connector kafka_signals = kafka(brokers: \"broker:9092\", ",
        "sasl_username: \"svc\", sasl_password: \"hunter2-the-real-one\")\n",
        "\n",
        "stream Alerts = Login.from(kafka_signals, topic: \"auth\")\n",
    );

    #[test]
    fn strips_the_credential_the_coordinator_injected() {
        let out = redact_vpl_secrets(INJECTED);
        assert!(
            !out.contains("hunter2-the-real-one"),
            "credential survived redaction: {out}"
        );
        assert!(out.contains("sasl_password: \"[REDACTED]\""), "{out}");
        // Non-secret parameters must survive, or the operator cannot diagnose.
        assert!(out.contains("brokers: \"broker:9092\""), "{out}");
        assert!(out.contains("sasl_username: \"svc\""), "{out}");
        assert!(out.contains("topic: \"auth\""), "{out}");
    }

    #[test]
    fn leaves_detection_logic_alone() {
        // `registry_key` is a Sysmon field and `key` is `.enrich`'s join key.
        // Both match `is_secret_key`; neither is a secret. Rewriting them
        // corrupts the rule an operator is reading.
        let src = "stream S = E.enrich(ProductDB, key: o.product_id, fields: [name])\n\
                   stream P = R.emit(Alert { registry_key: r.TargetObject })\n";
        assert_eq!(redact_vpl_secrets(src), src);
    }

    #[test]
    fn handles_unquoted_and_escaped_values() {
        let src = "connector c = kafka(api_key: 1234567, token: \"a\\\"b\")\n";
        let out = redact_vpl_secrets(src);
        assert!(!out.contains("1234567"), "{out}");
        assert!(!out.contains("a\\\"b"), "{out}");
        assert_eq!(
            out,
            "connector c = kafka(api_key: \"[REDACTED]\", token: \"[REDACTED]\")\n"
        );
    }

    #[test]
    fn multiline_declarations_are_covered() {
        let src = "connector AuthKafka = kafka (\n    brokers: \"b:9092\",\n    sasl_password: \"s3cret\"\n)\n";
        let out = redact_vpl_secrets(src);
        assert!(!out.contains("s3cret"), "{out}");
        assert!(out.contains("brokers: \"b:9092\""), "{out}");
    }

    #[test]
    fn reports_inline_secret_parameters() {
        assert_eq!(
            vpl_inline_secret_params(INJECTED),
            vec!["sasl_password".to_string()]
        );
        assert!(vpl_inline_secret_params("stream S = E.from(c, topic: \"t\")").is_empty());
        // An empty value is a placeholder, not a credential.
        assert!(vpl_inline_secret_params("connector c = kafka(sasl_password: \"\")").is_empty());
    }

    #[test]
    fn detects_a_round_tripped_placeholder() {
        assert!(vpl_has_redacted_secret(&redact_vpl_secrets(INJECTED)));
        assert!(!vpl_has_redacted_secret(INJECTED));
    }

    #[test]
    fn ignores_comments_and_strings() {
        let src = "# connector c = kafka(password: \"not-real\")\n\
                   stream S = E.filter(msg == \"connector x = kafka(password: 1)\")\n";
        assert_eq!(redact_vpl_secrets(src), src);
    }
}
