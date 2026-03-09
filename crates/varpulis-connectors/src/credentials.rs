//! Connector credentials management with YAML profiles and optional AES-256-GCM encryption.
//!
//! This module provides secure credential storage for connector configurations.
//! Credentials are stored in a YAML file with named profiles that can be merged
//! into [`ConnectorConfig`] instances at runtime.
//!
//! # Features
//!
//! - **YAML profiles**: Named credential sets with typed connector properties
//! - **Encryption**: AES-256-GCM encryption for sensitive fields (behind `encryption` feature)
//! - **Permission validation**: Enforces secure file permissions on credentials and certificates
//! - **Sensitive field detection**: Automatically identifies password, secret, and token fields
//!
//! # Credentials File Format
//!
//! ```yaml
//! version: 1
//! require_encryption: false
//! profiles:
//!   production-kafka:
//!     connector_type: kafka
//!     properties:
//!       url: "broker1:9092,broker2:9092"
//!       sasl_password: "ENC[AES256-GCM,<base64>]"
//!       ssl_keystore_location: /etc/varpulis/certs/keystore.p12
//!   staging-mqtt:
//!     properties:
//!       username: "svc-varpulis"
//!       password: "ENC[AES256-GCM,<base64>]"
//! ```
//!
//! # Master Key
//!
//! The master key for encryption/decryption is loaded from environment variables:
//! - `VARPULIS_MASTER_KEY`: Hex-encoded 32-byte key (64 hex chars)
//! - `VARPULIS_MASTER_KEY_FILE`: Path to a file containing the hex-encoded key

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::types::ConnectorConfig;

// ──────────────────────────────────────────────
// Error type
// ──────────────────────────────────────────────

/// Errors that can occur during credential operations.
#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    /// Credentials file not found at the specified path.
    #[error("Credentials file not found: {0}")]
    FileNotFound(String),

    /// File permissions are too permissive (must be 0600 or 0400).
    #[error("Insecure file permissions on {path}: mode {mode:#o} (max allowed: {max_mode:#o})")]
    PermissionInsecure {
        /// Path to the offending file.
        path: String,
        /// Actual permission mode bits.
        mode: u32,
        /// Maximum allowed permission mode bits.
        max_mode: u32,
    },

    /// Failed to parse the YAML credentials file.
    #[error("Failed to parse credentials file: {0}")]
    ParseError(String),

    /// Requested profile does not exist in the credentials file.
    #[error("Profile not found: {0}")]
    ProfileNotFound(String),

    /// Decryption of an encrypted value failed.
    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),

    /// An encrypted value was encountered but no master key is available.
    #[error("Master key required to decrypt value for field '{0}'")]
    MasterKeyRequired(String),

    /// Profile connector_type does not match the config connector_type.
    #[error("Connector type mismatch: profile specifies '{profile}' but config is '{config}'")]
    TypeMismatch {
        /// Connector type declared in the profile.
        profile: String,
        /// Connector type from the runtime config.
        config: String,
    },

    /// Certificate file permissions are too permissive.
    #[error("Insecure certificate permissions on {path}: mode {mode:#o} (must be 0600 or 0400)")]
    CertPermissionInsecure {
        /// Path to the certificate or key file.
        path: String,
        /// Actual permission mode bits.
        mode: u32,
    },

    /// Generic I/O error.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

// ──────────────────────────────────────────────
// YAML data model
// ──────────────────────────────────────────────

/// Top-level credentials file structure.
///
/// Contains versioning, an encryption enforcement flag, and a map of named profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialsFile {
    /// Schema version (currently 1).
    #[serde(default = "default_version")]
    pub version: u32,

    /// When true, all sensitive fields must be encrypted — plaintext values
    /// for password/secret/token/key fields will be rejected.
    #[serde(default)]
    pub require_encryption: bool,

    /// Named credential profiles.
    #[serde(default)]
    pub profiles: IndexMap<String, ConnectorProfile>,
}

fn default_version() -> u32 {
    1
}

/// A single credential profile that can be merged into a [`ConnectorConfig`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorProfile {
    /// Optional connector type constraint. When set, the profile can only be
    /// merged into configs of the same type.
    pub connector_type: Option<String>,

    /// Key-value properties to merge into the connector config.
    #[serde(default)]
    pub properties: IndexMap<String, String>,
}

// ──────────────────────────────────────────────
// Sensitive field detection
// ──────────────────────────────────────────────

/// Check whether a field name refers to a sensitive value.
///
/// A field is sensitive if its lowercase form contains "password", "secret",
/// "token", or "key" — but NOT if it ends with "_location" (those are file paths,
/// e.g. `ssl_key_location`).
pub fn is_sensitive_field(name: &str) -> bool {
    let lower = name.to_lowercase();
    if lower.ends_with("_location") {
        return false;
    }
    lower.contains("password")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains("key")
}

/// Check whether a string value is in the encrypted envelope format.
pub fn is_encrypted(value: &str) -> bool {
    value.starts_with("ENC[AES256-GCM,") && value.ends_with(']')
}

// ──────────────────────────────────────────────
// File permission validation (Unix only)
// ──────────────────────────────────────────────

/// Validate that a file's Unix permissions do not exceed `max_mode`.
///
/// Returns `Ok(())` if the file's permission bits (masked to 0o777) are a
/// subset of `max_mode`. For example, `max_mode = 0o600` allows owner
/// read/write but rejects group or other access.
///
/// On non-Unix platforms this is a no-op.
#[cfg(unix)]
pub fn validate_file_permissions(path: &Path, max_mode: u32) -> Result<(), CredentialsError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CredentialsError::FileNotFound(path.display().to_string())
        } else {
            CredentialsError::IoError(e)
        }
    })?;

    let mode = metadata.permissions().mode() & 0o777;
    // Check that no bits outside max_mode are set
    if mode & !max_mode != 0 {
        return Err(CredentialsError::PermissionInsecure {
            path: path.display().to_string(),
            mode,
            max_mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn validate_file_permissions(_path: &Path, _max_mode: u32) -> Result<(), CredentialsError> {
    // Permission checks are not available on non-Unix platforms
    Ok(())
}

/// Validate permissions on certificate files referenced in connector properties.
///
/// Scans for property keys matching `ssl_*_location`, `tls_*_location`, or
/// `*_cert_location` / `*_key_location` and verifies each referenced file
/// has permissions no more permissive than 0o600.
pub fn validate_cert_permissions(
    properties: &IndexMap<String, String>,
) -> Result<(), CredentialsError> {
    for (key, value) in properties {
        let lower_key = key.to_lowercase();
        let is_cert_path = lower_key.starts_with("ssl_") && lower_key.ends_with("_location")
            || lower_key.starts_with("tls_") && lower_key.ends_with("_location")
            || lower_key.ends_with("_cert_location")
            || lower_key.ends_with("_key_location");

        if is_cert_path {
            let path = Path::new(value);
            if path.exists() {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let metadata = std::fs::metadata(path)?;
                    let mode = metadata.permissions().mode() & 0o777;
                    if mode & !0o600 != 0 {
                        return Err(CredentialsError::CertPermissionInsecure {
                            path: path.display().to_string(),
                            mode,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────
// Master key loading
// ──────────────────────────────────────────────

/// Load the 32-byte master key from environment variables.
///
/// Checks `VARPULIS_MASTER_KEY` first (hex-encoded, 64 chars), then falls back
/// to reading from the file specified by `VARPULIS_MASTER_KEY_FILE`.
///
/// Returns `None` if neither variable is set.
pub fn load_master_key() -> Result<Option<[u8; 32]>, CredentialsError> {
    // Try direct hex key first
    if let Ok(hex_key) = std::env::var("VARPULIS_MASTER_KEY") {
        let key = parse_hex_key(&hex_key)?;
        return Ok(Some(key));
    }

    // Try key file
    if let Ok(key_file) = std::env::var("VARPULIS_MASTER_KEY_FILE") {
        let path = Path::new(&key_file);
        if !path.exists() {
            return Err(CredentialsError::FileNotFound(key_file));
        }
        validate_file_permissions(path, 0o600)?;
        let contents = std::fs::read_to_string(path)?;
        let key = parse_hex_key(contents.trim())?;
        return Ok(Some(key));
    }

    Ok(None)
}

/// Parse a hex-encoded 32-byte key.
fn parse_hex_key(hex: &str) -> Result<[u8; 32], CredentialsError> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err(CredentialsError::DecryptionFailed(format!(
            "Master key must be 64 hex characters (32 bytes), got {} chars",
            hex.len()
        )));
    }

    let mut key = [0u8; 32];
    for i in 0..32 {
        key[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|e| {
            CredentialsError::DecryptionFailed(format!("Invalid hex in master key: {}", e))
        })?;
    }
    Ok(key)
}

// ──────────────────────────────────────────────
// Encryption (behind `encryption` feature)
// ──────────────────────────────────────────────

/// Encrypt a plaintext string using AES-256-GCM, returning the envelope format
/// `ENC[AES256-GCM,<base64(nonce || ciphertext)>]`.
#[cfg(feature = "encryption")]
pub fn encrypt_value(plaintext: &str, key: &[u8; 32]) -> Result<String, CredentialsError> {
    use aes_gcm::aead::{Aead, OsRng};
    use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
    use base64::Engine;

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CredentialsError::DecryptionFailed(format!("AES key error: {}", e)))?;

    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| CredentialsError::DecryptionFailed(format!("Encryption failed: {}", e)))?;

    // Concatenate nonce (12 bytes) + ciphertext, then base64-encode
    let mut blob = Vec::with_capacity(12 + ciphertext.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ciphertext);

    let encoded = base64::engine::general_purpose::STANDARD.encode(&blob);
    Ok(format!("ENC[AES256-GCM,{}]", encoded))
}

/// Decrypt an envelope-formatted encrypted value, returning the plaintext string.
///
/// Expects format `ENC[AES256-GCM,<base64(nonce || ciphertext)>]`.
#[cfg(feature = "encryption")]
pub fn decrypt_value(encrypted: &str, key: &[u8; 32]) -> Result<String, CredentialsError> {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
    use base64::Engine;

    if !is_encrypted(encrypted) {
        return Err(CredentialsError::DecryptionFailed(
            "Value is not in ENC[AES256-GCM,...] format".to_string(),
        ));
    }

    // Extract base64 payload between "ENC[AES256-GCM," and "]"
    let b64 = &encrypted["ENC[AES256-GCM,".len()..encrypted.len() - 1];

    let blob = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| CredentialsError::DecryptionFailed(format!("Base64 decode failed: {}", e)))?;

    if blob.len() < 12 {
        return Err(CredentialsError::DecryptionFailed(
            "Encrypted data too short (missing nonce)".to_string(),
        ));
    }

    let (nonce_bytes, ciphertext) = blob.split_at(12);
    let nonce_arr: [u8; 12] = nonce_bytes
        .try_into()
        .map_err(|_| CredentialsError::DecryptionFailed("Invalid nonce length".to_string()))?;
    let nonce = Nonce::from(nonce_arr);

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CredentialsError::DecryptionFailed(format!("AES key error: {}", e)))?;

    let plaintext_bytes = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| CredentialsError::DecryptionFailed(format!("Decryption failed: {}", e)))?;

    String::from_utf8(plaintext_bytes)
        .map_err(|e| CredentialsError::DecryptionFailed(format!("Invalid UTF-8: {}", e)))
}

// ──────────────────────────────────────────────
// CredentialsStore
// ──────────────────────────────────────────────

/// Manages credential profiles and provides merge operations for connector configs.
///
/// The store holds a parsed [`CredentialsFile`] and an optional master key for
/// decrypting encrypted property values.
///
/// # Example
///
/// ```rust,no_run
/// use varpulis_connectors::credentials::CredentialsStore;
/// use varpulis_connectors::ConnectorConfig;
///
/// let store = CredentialsStore::from_yaml(r#"
///     version: 1
///     profiles:
///       prod-kafka:
///         connector_type: kafka
///         properties:
///           sasl_username: admin
///           sasl_password: secret123
/// "#).unwrap();
///
/// let mut config = ConnectorConfig::new("kafka", "broker:9092");
/// store.merge_profile("prod-kafka", &mut config).unwrap();
/// assert_eq!(config.properties.get("sasl_username").unwrap(), "admin");
/// ```
#[derive(Debug)]
pub struct CredentialsStore {
    /// The parsed credentials file.
    pub credentials: CredentialsFile,
    /// Optional 32-byte master key for AES-256-GCM decryption.
    master_key: Option<[u8; 32]>,
}

impl CredentialsStore {
    /// Create a store from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self, CredentialsError> {
        let credentials: CredentialsFile =
            serde_yaml::from_str(yaml).map_err(|e| CredentialsError::ParseError(e.to_string()))?;
        Ok(Self {
            credentials,
            master_key: None,
        })
    }

    /// Create a store by reading and parsing a YAML file from disk.
    ///
    /// Validates that the file permissions are no more permissive than 0o600.
    pub fn from_file(path: &Path) -> Result<Self, CredentialsError> {
        if !path.exists() {
            return Err(CredentialsError::FileNotFound(path.display().to_string()));
        }
        validate_file_permissions(path, 0o600)?;
        let contents = std::fs::read_to_string(path)?;
        Self::from_yaml(&contents)
    }

    /// Set the master key for encryption/decryption operations.
    pub fn with_master_key(mut self, key: [u8; 32]) -> Self {
        self.master_key = Some(key);
        self
    }

    /// Try to load the master key from environment variables.
    ///
    /// See [`load_master_key`] for details on the environment variables checked.
    pub fn try_load_master_key(mut self) -> Result<Self, CredentialsError> {
        self.master_key = load_master_key()?;
        Ok(self)
    }

    /// Get a reference to a profile by name.
    pub fn get_profile(&self, name: &str) -> Result<&ConnectorProfile, CredentialsError> {
        self.credentials
            .profiles
            .get(name)
            .ok_or_else(|| CredentialsError::ProfileNotFound(name.to_string()))
    }

    /// List all profile names.
    pub fn profile_names(&self) -> Vec<&str> {
        self.credentials
            .profiles
            .keys()
            .map(|s| s.as_str())
            .collect()
    }

    /// Merge a named profile's properties into a [`ConnectorConfig`].
    ///
    /// This performs the following steps:
    /// 1. Look up the profile by name
    /// 2. If the profile has a `connector_type`, verify it matches the config
    /// 3. If `require_encryption` is set, verify sensitive fields are encrypted
    /// 4. Decrypt any encrypted values (requires master key)
    /// 5. Merge properties into the config (profile values override existing)
    pub fn merge_profile(
        &self,
        profile_name: &str,
        config: &mut ConnectorConfig,
    ) -> Result<(), CredentialsError> {
        let profile = self.get_profile(profile_name)?;

        // Type check
        if let Some(ref profile_type) = profile.connector_type {
            if profile_type != &config.connector_type {
                return Err(CredentialsError::TypeMismatch {
                    profile: profile_type.clone(),
                    config: config.connector_type.clone(),
                });
            }
        }

        // Process each property
        for (key, value) in &profile.properties {
            let resolved = self.resolve_value(key, value)?;
            config.properties.insert(key.clone(), resolved);
        }

        Ok(())
    }

    /// Resolve a single property value, decrypting if necessary and enforcing
    /// the `require_encryption` policy.
    fn resolve_value(&self, field: &str, value: &str) -> Result<String, CredentialsError> {
        // Enforce encryption requirement on sensitive fields
        if self.credentials.require_encryption && is_sensitive_field(field) && !is_encrypted(value)
        {
            return Err(CredentialsError::MasterKeyRequired(format!(
                "Field '{}' must be encrypted (require_encryption is set)",
                field
            )));
        }

        if is_encrypted(value) {
            #[cfg(feature = "encryption")]
            {
                let key = self
                    .master_key
                    .as_ref()
                    .ok_or_else(|| CredentialsError::MasterKeyRequired(field.to_string()))?;
                decrypt_value(value, key)
            }
            #[cfg(not(feature = "encryption"))]
            {
                Err(CredentialsError::DecryptionFailed(format!(
                    "Cannot decrypt field '{}': encryption feature not enabled",
                    field
                )))
            }
        } else {
            Ok(value.to_string())
        }
    }
}

// ──────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const PLAINTEXT_YAML: &str = r"
version: 1
require_encryption: false
profiles:
  prod-kafka:
    connector_type: kafka
    properties:
      sasl_username: admin
      sasl_password: supersecret
      ssl_key_location: /etc/certs/client.key
  staging-mqtt:
    properties:
      username: svc-varpulis
      password: mqtt-pass
";

    #[test]
    fn parse_plaintext_credentials() {
        let creds: CredentialsFile = serde_yaml::from_str(PLAINTEXT_YAML).unwrap();
        assert_eq!(creds.version, 1);
        assert!(!creds.require_encryption);
        assert_eq!(creds.profiles.len(), 2);
        assert!(creds.profiles.contains_key("prod-kafka"));
        assert!(creds.profiles.contains_key("staging-mqtt"));

        let kafka = &creds.profiles["prod-kafka"];
        assert_eq!(kafka.connector_type.as_deref(), Some("kafka"));
        assert_eq!(kafka.properties.get("sasl_username").unwrap(), "admin");

        let mqtt = &creds.profiles["staging-mqtt"];
        assert!(mqtt.connector_type.is_none());
        assert_eq!(mqtt.properties.get("username").unwrap(), "svc-varpulis");
    }

    #[test]
    fn resolve_profile_properties() {
        let store = CredentialsStore::from_yaml(PLAINTEXT_YAML).unwrap();

        let profile = store.get_profile("prod-kafka").unwrap();
        assert_eq!(profile.connector_type.as_deref(), Some("kafka"));
        assert_eq!(profile.properties.len(), 3);
        assert_eq!(
            profile.properties.get("sasl_password").unwrap(),
            "supersecret"
        );

        let names = store.profile_names();
        assert!(names.contains(&"prod-kafka"));
        assert!(names.contains(&"staging-mqtt"));
    }

    #[test]
    fn merge_profile_into_config() {
        let store = CredentialsStore::from_yaml(PLAINTEXT_YAML).unwrap();

        let mut config = ConnectorConfig::new("kafka", "broker:9092")
            .with_topic("events")
            .with_property("batch_size", "1000");

        store.merge_profile("prod-kafka", &mut config).unwrap();

        // Profile properties are merged
        assert_eq!(config.properties.get("sasl_username").unwrap(), "admin");
        assert_eq!(
            config.properties.get("sasl_password").unwrap(),
            "supersecret"
        );
        assert_eq!(
            config.properties.get("ssl_key_location").unwrap(),
            "/etc/certs/client.key"
        );

        // Original properties are preserved
        assert_eq!(config.properties.get("batch_size").unwrap(), "1000");

        // Top-level fields are unchanged
        assert_eq!(config.connector_type, "kafka");
        assert_eq!(config.url, "broker:9092");
        assert_eq!(config.topic, Some("events".to_string()));
    }

    #[test]
    fn profile_not_found_error() {
        let store = CredentialsStore::from_yaml(PLAINTEXT_YAML).unwrap();
        let mut config = ConnectorConfig::new("kafka", "broker:9092");

        let err = store.merge_profile("nonexistent", &mut config).unwrap_err();
        assert!(
            matches!(err, CredentialsError::ProfileNotFound(ref name) if name == "nonexistent"),
            "Expected ProfileNotFound, got: {err}"
        );
    }

    #[test]
    fn connector_type_mismatch_error() {
        let store = CredentialsStore::from_yaml(PLAINTEXT_YAML).unwrap();

        // prod-kafka profile has connector_type = "kafka", but config is "mqtt"
        let mut config = ConnectorConfig::new("mqtt", "localhost:1883");

        let err = store.merge_profile("prod-kafka", &mut config).unwrap_err();
        assert!(
            matches!(
                err,
                CredentialsError::TypeMismatch {
                    ref profile,
                    ref config,
                } if profile == "kafka" && config == "mqtt"
            ),
            "Expected TypeMismatch, got: {err}"
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_value_roundtrip() {
        let key: [u8; 32] = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ];

        let plaintext = "my-super-secret-password";
        let encrypted = encrypt_value(plaintext, &key).unwrap();

        // Must be in envelope format
        assert!(is_encrypted(&encrypted));
        assert!(encrypted.starts_with("ENC[AES256-GCM,"));
        assert!(encrypted.ends_with(']'));

        // Round-trip
        let decrypted = decrypt_value(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn wrong_key_fails_decryption() {
        let key1: [u8; 32] = [0xAA; 32];
        let key2: [u8; 32] = [0xBB; 32];

        let encrypted = encrypt_value("secret-data", &key1).unwrap();

        let result = decrypt_value(&encrypted, &key2);
        assert!(result.is_err());
        assert!(
            matches!(result, Err(CredentialsError::DecryptionFailed(_))),
            "Expected DecryptionFailed, got: {:?}",
            result
        );
    }

    #[test]
    fn require_encryption_rejects_plaintext_sensitive_fields() {
        let yaml = r"
version: 1
require_encryption: true
profiles:
  insecure:
    properties:
      username: admin
      password: plaintext-bad
";
        let store = CredentialsStore::from_yaml(yaml).unwrap();
        let mut config = ConnectorConfig::new("kafka", "broker:9092");

        let err = store.merge_profile("insecure", &mut config).unwrap_err();
        // password is sensitive and not encrypted, so it should fail
        assert!(
            matches!(err, CredentialsError::MasterKeyRequired(_)),
            "Expected MasterKeyRequired for plaintext sensitive field, got: {err}"
        );
    }

    #[test]
    fn is_encrypted_detection() {
        assert!(is_encrypted("ENC[AES256-GCM,dGVzdA==]"));
        assert!(is_encrypted(
            "ENC[AES256-GCM,c29tZS1sb25nLWJhc2U2NC1lbmNvZGVkLWRhdGE=]"
        ));

        // Not encrypted
        assert!(!is_encrypted("plaintext"));
        assert!(!is_encrypted("ENC[AES128-CBC,data]"));
        assert!(!is_encrypted("ENC[AES256-GCM,data"));
        assert!(!is_encrypted(""));
        assert!(!is_encrypted("ENC[]"));
    }

    #[test]
    fn empty_credentials_file() {
        let yaml = r"
version: 1
profiles: {}
";
        let store = CredentialsStore::from_yaml(yaml).unwrap();
        assert_eq!(store.credentials.version, 1);
        assert!(!store.credentials.require_encryption);
        assert!(store.credentials.profiles.is_empty());
        assert!(store.profile_names().is_empty());
    }

    #[test]
    fn sensitive_field_detection() {
        // Sensitive fields
        assert!(is_sensitive_field("password"));
        assert!(is_sensitive_field("sasl_password"));
        assert!(is_sensitive_field("api_secret"));
        assert!(is_sensitive_field("auth_token"));
        assert!(is_sensitive_field("api_key"));
        assert!(is_sensitive_field("SECRET_VALUE"));
        assert!(is_sensitive_field("myPassword"));

        // NOT sensitive: _location suffix (file paths)
        assert!(!is_sensitive_field("ssl_key_location"));
        assert!(!is_sensitive_field("ssl_keystore_location"));
        assert!(!is_sensitive_field("tls_cert_location"));

        // NOT sensitive: regular fields
        assert!(!is_sensitive_field("username"));
        assert!(!is_sensitive_field("host"));
        assert!(!is_sensitive_field("batch_size"));
        assert!(!is_sensitive_field("topic"));
    }

    #[test]
    fn merge_profile_without_connector_type() {
        // staging-mqtt has no connector_type, so it should merge into any config
        let store = CredentialsStore::from_yaml(PLAINTEXT_YAML).unwrap();

        let mut config = ConnectorConfig::new("mqtt", "localhost:1883");
        store.merge_profile("staging-mqtt", &mut config).unwrap();

        assert_eq!(config.properties.get("username").unwrap(), "svc-varpulis");
        assert_eq!(config.properties.get("password").unwrap(), "mqtt-pass");
    }

    #[test]
    fn parse_hex_key_valid() {
        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let key = parse_hex_key(hex).unwrap();
        assert_eq!(key[0], 0x00);
        assert_eq!(key[15], 0x0f);
        assert_eq!(key[31], 0x1f);
    }

    #[test]
    fn parse_hex_key_invalid_length() {
        let err = parse_hex_key("deadbeef").unwrap_err();
        assert!(
            matches!(err, CredentialsError::DecryptionFailed(ref msg) if msg.contains("64 hex")),
            "Expected length error, got: {err}"
        );
    }

    #[test]
    fn parse_hex_key_invalid_chars() {
        let hex = "zz0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let err = parse_hex_key(hex).unwrap_err();
        assert!(
            matches!(err, CredentialsError::DecryptionFailed(ref msg) if msg.contains("Invalid hex")),
            "Expected hex parse error, got: {err}"
        );
    }
}
