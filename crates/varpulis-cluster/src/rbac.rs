//! Role-based access control for the cluster API.
//!
//! Supports three roles with hierarchical permissions:
//! - **Admin**: Full access (deploy, manage, read, configure)
//! - **Operator**: Deploy pipelines, manage workers, inject events
//! - **Viewer**: Read-only access (list, get, topology, metrics)
//!
//! ## Configuration
//!
//! Single key (backward-compatible): `--api-key admin-secret`
//!
//! Multi-key file (`--api-keys keys.json`):
//! ```json
//! {
//!   "keys": [
//!     { "key": "admin-secret", "role": "admin", "name": "CI deploy" },
//!     { "key": "viewer-token", "role": "viewer", "name": "Grafana" }
//!   ]
//! }
//! ```

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Access role with hierarchical permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Read-only: list workers, get topology, read metrics
    Viewer = 0,
    /// Read + write: deploy/undeploy pipelines, inject events, manage workers
    Operator = 1,
    /// Full access: everything including cluster configuration and deletion
    Admin = 2,
}

impl Role {
    /// Check if this role has at least the given permission level.
    pub fn has_permission(&self, required: Role) -> bool {
        (*self as u8) >= (required as u8)
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Viewer => write!(f, "viewer"),
            Role::Operator => write!(f, "operator"),
            Role::Admin => write!(f, "admin"),
        }
    }
}

impl std::str::FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "viewer" | "read" | "readonly" => Ok(Role::Viewer),
            "operator" | "write" | "deploy" => Ok(Role::Operator),
            "admin" | "root" | "superadmin" => Ok(Role::Admin),
            other => Err(format!(
                "Unknown role: '{}'. Use: viewer, operator, admin",
                other
            )),
        }
    }
}

/// An API key entry with its associated role and optional name.
#[derive(Debug, Clone)]
pub struct ApiKeyEntry {
    pub role: Role,
    pub name: Option<String>,
}

/// RBAC configuration: maps API keys to roles.
#[derive(Debug, Clone)]
pub struct RbacConfig {
    /// Map from API key string to role entry.
    keys: HashMap<String, ApiKeyEntry>,
    /// When true, unauthenticated requests are allowed (no API key required).
    pub allow_anonymous: bool,
    /// Default role for anonymous/unauthenticated requests (when allow_anonymous is true).
    pub anonymous_role: Role,
}

/// JSON format for the API keys file.
#[derive(Deserialize)]
struct ApiKeysFile {
    keys: Vec<ApiKeyFileEntry>,
}

#[derive(Deserialize)]
struct ApiKeyFileEntry {
    key: String,
    role: String,
    name: Option<String>,
}

impl RbacConfig {
    /// Create an RBAC config that allows all requests (no authentication).
    pub fn disabled() -> Self {
        Self {
            keys: HashMap::default(),
            allow_anonymous: true,
            anonymous_role: Role::Admin,
        }
    }

    /// Create an RBAC config with multiple keys and roles.
    pub fn multi_key(keys: HashMap<String, ApiKeyEntry>) -> Self {
        Self {
            keys,
            allow_anonymous: false,
            anonymous_role: Role::Viewer,
        }
    }

    /// Create an RBAC config with a single admin API key (backward-compatible).
    pub fn single_key(key: String) -> Self {
        let mut keys = HashMap::default();
        keys.insert(
            key,
            ApiKeyEntry {
                role: Role::Admin,
                name: Some("default".to_string()),
            },
        );
        Self {
            keys,
            allow_anonymous: false,
            anonymous_role: Role::Viewer,
        }
    }

    /// Load RBAC config from a JSON keys file.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read API keys file '{}': {}", path.display(), e))?;

        let file: ApiKeysFile = serde_json::from_str(&content)
            .map_err(|e| format!("Invalid API keys file format: {}", e))?;

        let mut keys = HashMap::default();
        for entry in file.keys {
            let role: Role = entry.role.parse().map_err(|e: String| {
                format!(
                    "In key '{}': {}",
                    entry.name.as_deref().unwrap_or("unnamed"),
                    e
                )
            })?;
            keys.insert(
                entry.key,
                ApiKeyEntry {
                    role,
                    name: entry.name,
                },
            );
        }

        if keys.is_empty() {
            return Err("API keys file contains no keys".to_string());
        }

        Ok(Self {
            keys,
            allow_anonymous: false,
            anonymous_role: Role::Viewer,
        })
    }

    /// Authenticate an API key and return its role.
    ///
    /// Uses constant-time comparison to prevent timing attacks.
    pub fn authenticate(&self, provided: Option<&str>) -> Option<Role> {
        // When no keys are configured and anonymous access is allowed,
        // all requests are permitted (backward compat with disabled auth).
        if self.allow_anonymous && self.keys.is_empty() {
            return Some(self.anonymous_role);
        }

        match provided {
            Some(key) => {
                // Constant-time scan: check ALL keys to avoid timing leaks
                let mut matched_role = None;
                for (stored_key, entry) in &self.keys {
                    if varpulis_core::security::constant_time_compare(stored_key, key) {
                        matched_role = Some(entry.role);
                    }
                }
                matched_role
            }
            None => {
                if self.allow_anonymous {
                    Some(self.anonymous_role)
                } else {
                    None
                }
            }
        }
    }

    /// Number of registered API keys.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Extract any admin-level key for backward-compatible subsystems
    /// (Raft inter-node auth, WebSocket identify protocol).
    pub fn any_admin_key(&self) -> Option<String> {
        self.keys
            .iter()
            .find(|(_, entry)| entry.role == Role::Admin)
            .map(|(key, _)| key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_hierarchy() {
        assert!(Role::Admin.has_permission(Role::Admin));
        assert!(Role::Admin.has_permission(Role::Operator));
        assert!(Role::Admin.has_permission(Role::Viewer));

        assert!(!Role::Operator.has_permission(Role::Admin));
        assert!(Role::Operator.has_permission(Role::Operator));
        assert!(Role::Operator.has_permission(Role::Viewer));

        assert!(!Role::Viewer.has_permission(Role::Admin));
        assert!(!Role::Viewer.has_permission(Role::Operator));
        assert!(Role::Viewer.has_permission(Role::Viewer));
    }

    #[test]
    fn test_role_parsing() {
        assert_eq!("admin".parse::<Role>().unwrap(), Role::Admin);
        assert_eq!("operator".parse::<Role>().unwrap(), Role::Operator);
        assert_eq!("viewer".parse::<Role>().unwrap(), Role::Viewer);
        assert_eq!("read".parse::<Role>().unwrap(), Role::Viewer);
        assert_eq!("write".parse::<Role>().unwrap(), Role::Operator);
        assert!("invalid".parse::<Role>().is_err());
    }

    #[test]
    fn test_disabled_allows_all() {
        let config = RbacConfig::disabled();
        assert_eq!(config.authenticate(None), Some(Role::Admin));
        assert_eq!(config.authenticate(Some("anything")), Some(Role::Admin)); // disabled = allow all
    }

    #[test]
    fn test_single_key_admin() {
        let config = RbacConfig::single_key("secret".to_string());
        assert_eq!(config.authenticate(Some("secret")), Some(Role::Admin));
        assert_eq!(config.authenticate(Some("wrong")), None);
        assert_eq!(config.authenticate(None), None);
    }

    #[test]
    fn test_multi_key_roles() {
        let mut keys = HashMap::default();
        keys.insert(
            "admin-key".to_string(),
            ApiKeyEntry {
                role: Role::Admin,
                name: Some("admin".to_string()),
            },
        );
        keys.insert(
            "viewer-key".to_string(),
            ApiKeyEntry {
                role: Role::Viewer,
                name: Some("grafana".to_string()),
            },
        );
        let config = RbacConfig {
            keys,
            allow_anonymous: false,
            anonymous_role: Role::Viewer,
        };

        assert_eq!(config.authenticate(Some("admin-key")), Some(Role::Admin));
        assert_eq!(config.authenticate(Some("viewer-key")), Some(Role::Viewer));
        assert_eq!(config.authenticate(Some("unknown")), None);
        assert_eq!(config.authenticate(None), None);
    }

    #[test]
    fn test_from_file() {
        let dir = std::env::temp_dir().join("varpulis_rbac_test.json");
        let content = r#"{
            "keys": [
                { "key": "k1", "role": "admin", "name": "CI" },
                { "key": "k2", "role": "viewer" }
            ]
        }"#;
        std::fs::write(&dir, content).unwrap();

        let config = RbacConfig::from_file(&dir).unwrap();
        assert_eq!(config.key_count(), 2);
        assert_eq!(config.authenticate(Some("k1")), Some(Role::Admin));
        assert_eq!(config.authenticate(Some("k2")), Some(Role::Viewer));

        let _ = std::fs::remove_file(&dir);
    }
}
