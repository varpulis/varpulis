//! Shared security utilities for Varpulis.
//!
//! Provides constant-time string comparison and common body-size limits
//! used across the CLI and cluster crates.

/// Maximum body size for normal JSON endpoints (1 MB).
pub const JSON_BODY_LIMIT: u64 = 1024 * 1024;

/// Maximum body size for large payloads: inject-batch, models, snapshots (16 MB).
pub const LARGE_BODY_LIMIT: u64 = 16 * 1024 * 1024;

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
