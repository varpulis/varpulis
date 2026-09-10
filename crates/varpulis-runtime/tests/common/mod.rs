//! Shared helpers for the broker-backed integration tests.
//!
//! Included with `mod common;` rather than published from the crate, because
//! it is test scaffolding and has no business in the library's public API.

#![allow(dead_code)] // each test binary uses a different subset

/// Record that a test could not run, and fail when the caller said it should
/// have been able to.
///
/// A broker-backed test must never report success because the broker was
/// absent. These files printed "Skipping test: Kafka not available" and
/// returned, which libtest scores as a pass — so the suites standing behind
/// the exactly-once protocol, the TLS handshakes, and the reconnect-after-
/// outage fixes could all report green having verified nothing.
///
/// Every CI job that provisions a broker sets `VARPULIS_REQUIRE_BROKERS=1`;
/// under that flag a missing broker is a hard failure. Without it — a
/// developer box with nothing running — the abstention is reported loudly on
/// stderr and in the GitHub job summary, so nobody mistakes it for a pass.
///
/// Mirrors the same helper in the connector suites and the chaos harness
/// deliberately: one environment variable turns every abstention in the
/// repository into a failure.
#[track_caller]
pub fn abstain(test: &str, reason: &str) {
    assert!(
        std::env::var_os("VARPULIS_REQUIRE_BROKERS").is_none(),
        "VARPULIS_REQUIRE_BROKERS=1 but {test} could not reach its broker: \
         {reason}. This test stands behind a merged fix — it must not pass by \
         abstaining. Fix the broker fixture instead of relaxing the gate."
    );
    eprintln!("SKIPPED(no-broker) {test}: {reason}");
    if let Ok(summary) = std::env::var("GITHUB_STEP_SUMMARY") {
        use std::io::Write as _;
        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(summary) {
            let _ = writeln!(f, "- :warning: **SKIPPED (no broker)** `{test}` — {reason}");
        }
    }
}
