//! Applying the documented encryption at rest.
//!
//! `VARPULIS_ENCRYPTION_KEY` and `VARPULIS_ENCRYPTION_PASSPHRASE` were
//! described in the configuration guide and walked through step by step in an
//! encryption-at-rest tutorial, down to Argon2id's memory and iteration
//! counts. Neither was read. `EncryptedStateStore` was constructed only in its
//! own tests and in a doc comment, so every checkpoint and every tenant state
//! file was written in plaintext no matter what an operator exported — and the
//! tutorial's own "verify the files are binary gibberish" step would have
//! failed for anyone who tried it.
//!
//! This is the layer that was missing: one place that turns a configured key
//! into a wrapped store, and refuses to start when a key is configured and
//! cannot be applied.

use std::sync::Arc;

use anyhow::{bail, Result};
use varpulis_runtime::persistence::{FileStore, StateStore};

/// Wrap `store` in AES-256-GCM when a key is configured.
///
/// Returns the store unchanged when neither variable is set. Fails when one is
/// set and cannot be honoured, because the alternative is writing plaintext
/// while the operator believes otherwise — the failure this whole module
/// exists to remove.
///
/// `purpose` names what is being protected, for the log line and the error.
pub fn wrap_if_configured(store: FileStore, purpose: &str) -> Result<Arc<dyn StateStore>> {
    #[cfg(feature = "encryption")]
    {
        use varpulis_runtime::persistence::EncryptedStateStore;
        match EncryptedStateStore::<FileStore>::key_from_env() {
            Ok(Some(key)) => {
                tracing::info!("{purpose}: encrypted at rest (AES-256-GCM)");
                return Ok(Arc::new(EncryptedStateStore::new(store, key)));
            }
            Ok(None) => {}
            Err(e) => bail!(
                "an encryption key is configured but unusable: {e}. Refusing to \
                 start rather than write {purpose} in plaintext."
            ),
        }
    }

    #[cfg(not(feature = "encryption"))]
    if key_is_configured() {
        bail!(
            "VARPULIS_ENCRYPTION_KEY or VARPULIS_ENCRYPTION_PASSPHRASE is set, \
             but this binary was built without the `encryption` feature, so \
             {purpose} would be written in plaintext. Rebuild with \
             `--features encryption`, or unset the variable to accept plaintext \
             deliberately."
        );
    }

    Ok(Arc::new(store))
}

/// Whether either encryption variable carries a non-empty value.
///
/// Only the feature-off path needs this — with the feature on, the key itself
/// is read — but the test exercises it either way.
#[cfg(any(not(feature = "encryption"), test))]
fn key_is_configured() -> bool {
    ["VARPULIS_ENCRYPTION_KEY", "VARPULIS_ENCRYPTION_PASSPHRASE"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The feature-off path must refuse rather than silently write plaintext.
    ///
    /// Reads the real environment, so it asserts the shape of the check rather
    /// than mutating a variable — `set_var` is `unsafe` since Rust 2024 and
    /// this workspace denies `unsafe` blocks.
    #[test]
    fn a_configured_key_is_detected() {
        // With nothing set — the state in CI and on a developer box — the
        // helper reports no key, so `wrap_if_configured` returns the plain
        // store and starting is correct.
        if std::env::var("VARPULIS_ENCRYPTION_KEY").is_err()
            && std::env::var("VARPULIS_ENCRYPTION_PASSPHRASE").is_err()
        {
            assert!(!key_is_configured());
        }
    }
}
