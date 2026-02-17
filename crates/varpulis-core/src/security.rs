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
