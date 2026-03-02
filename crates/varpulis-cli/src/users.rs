//! Local user store with password authentication and session management.
//!
//! Provides username/password authentication with argon2 password hashing,
//! in-memory session tracking, and JSON file persistence for user records.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Stored user record (serialized to JSON file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredUser {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub display_name: String,
    pub email: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub disabled: bool,
}

/// Session metadata for tracking parallel sessions.
#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub session_id: String,
    pub user_id: String,
    pub username: String,
    pub role: String,
    pub created_at: Instant,
    pub last_activity: Instant,
    pub absolute_expiry: Instant,
}

/// Session management configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub idle_timeout: Duration,
    pub absolute_timeout: Duration,
    pub max_parallel_sessions: usize,
    pub renewal_window: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30 * 60), // 30 minutes
            absolute_timeout: Duration::from_secs(24 * 3600), // 24 hours
            max_parallel_sessions: 5,
            renewal_window: Duration::from_secs(5 * 60), // 5 minutes before expiry
        }
    }
}

/// In-memory user + session store backed by a JSON file.
#[derive(Debug)]
pub struct UserStore {
    users: HashMap<String, StoredUser>,
    sessions: HashMap<String, SessionRecord>,
    file_path: PathBuf,
    config: SessionConfig,
}

pub type SharedUserStore = Arc<RwLock<UserStore>>;

// ---------------------------------------------------------------------------
// Persistence format
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct UsersFile {
    users: Vec<StoredUser>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl UserStore {
    /// Load from JSON file or create empty store.
    pub fn new(file_path: PathBuf, config: SessionConfig) -> Self {
        let users = if file_path.exists() {
            match std::fs::read_to_string(&file_path) {
                Ok(contents) => match serde_json::from_str::<UsersFile>(&contents) {
                    Ok(data) => {
                        let mut map = HashMap::new();
                        for user in data.users {
                            map.insert(user.username.clone(), user);
                        }
                        tracing::info!("Loaded {} users from {}", map.len(), file_path.display());
                        map
                    }
                    Err(e) => {
                        tracing::error!("Failed to parse users file: {}", e);
                        HashMap::new()
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to read users file: {}", e);
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };

        Self {
            users,
            sessions: HashMap::new(),
            file_path,
            config,
        }
    }

    /// Save users to JSON file (atomic write via temp file + rename).
    pub fn save(&self) -> Result<(), String> {
        let data = UsersFile {
            users: self.users.values().cloned().collect(),
        };
        let json =
            serde_json::to_string_pretty(&data).map_err(|e| format!("Serialize error: {e}"))?;

        // Ensure parent directory exists
        if let Some(parent) = self.file_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        // Atomic write: write to temp file, then rename
        let tmp_path = self.file_path.with_extension("tmp");
        std::fs::write(&tmp_path, &json).map_err(|e| format!("Write error: {e}"))?;
        std::fs::rename(&tmp_path, &self.file_path).map_err(|e| format!("Rename error: {e}"))?;

        Ok(())
    }

    /// Check if the store has any users.
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// Create a new user with argon2 password hashing.
    pub fn create_user(
        &mut self,
        username: &str,
        password: &str,
        display_name: &str,
        email: &str,
        role: &str,
    ) -> Result<StoredUser, String> {
        if self.users.contains_key(username) {
            return Err(format!("User '{username}' already exists"));
        }

        if username.is_empty() || username.len() > 64 {
            return Err("Username must be 1-64 characters".to_string());
        }

        if password.len() < 8 {
            return Err("Password must be at least 8 characters".to_string());
        }

        let password_hash = hash_password(password)?;

        let now = Utc::now();
        let user = StoredUser {
            id: uuid::Uuid::new_v4().to_string(),
            username: username.to_string(),
            password_hash,
            display_name: display_name.to_string(),
            email: email.to_string(),
            role: role.to_string(),
            created_at: now,
            updated_at: now,
            disabled: false,
        };

        self.users.insert(username.to_string(), user.clone());
        self.save()
            .map_err(|e| format!("Failed to persist user: {e}"))?;

        Ok(user)
    }

    /// Verify a username/password combination. Returns the user on success.
    pub fn verify_password(&self, username: &str, password: &str) -> Result<StoredUser, String> {
        let user = self
            .users
            .get(username)
            .ok_or_else(|| "Invalid username or password".to_string())?;

        if user.disabled {
            return Err("Account is disabled".to_string());
        }

        if !verify_password(password, &user.password_hash)? {
            return Err("Invalid username or password".to_string());
        }

        Ok(user.clone())
    }

    /// Create a new session for a user. Evicts oldest session if max exceeded.
    pub fn create_session(&mut self, user: &StoredUser) -> SessionRecord {
        let now = Instant::now();

        // Count existing sessions for this user
        let user_sessions: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.user_id == user.id)
            .map(|(id, _)| id.clone())
            .collect();

        // Evict oldest sessions if at max
        if user_sessions.len() >= self.config.max_parallel_sessions {
            let mut sessions_with_time: Vec<_> = user_sessions
                .iter()
                .filter_map(|id| self.sessions.get(id).map(|s| (id.clone(), s.created_at)))
                .collect();
            sessions_with_time.sort_by_key(|(_, t)| *t);

            // Remove oldest sessions to make room
            let to_remove = user_sessions.len() - self.config.max_parallel_sessions + 1;
            for (id, _) in sessions_with_time.iter().take(to_remove) {
                self.sessions.remove(id);
            }
        }

        let session = SessionRecord {
            session_id: uuid::Uuid::new_v4().to_string(),
            user_id: user.id.clone(),
            username: user.username.clone(),
            role: user.role.clone(),
            created_at: now,
            last_activity: now,
            absolute_expiry: now + self.config.absolute_timeout,
        };

        self.sessions
            .insert(session.session_id.clone(), session.clone());
        session
    }

    /// Validate a session: check idle + absolute timeout, update last_activity.
    pub fn validate_session(&mut self, session_id: &str) -> Option<&SessionRecord> {
        let now = Instant::now();
        let config = self.config.clone();

        let session = self.sessions.get_mut(session_id)?;

        // Check absolute expiry
        if now >= session.absolute_expiry {
            self.sessions.remove(session_id);
            return None;
        }

        // Check idle timeout
        if now.duration_since(session.last_activity) > config.idle_timeout {
            self.sessions.remove(session_id);
            return None;
        }

        // Update last_activity (re-borrow after checks pass)
        let session = self.sessions.get_mut(session_id)?;
        session.last_activity = now;
        Some(session)
    }

    /// Check if a session is within the renewal window (close to expiry).
    pub fn needs_renewal(&self, session_id: &str) -> bool {
        if let Some(session) = self.sessions.get(session_id) {
            let now = Instant::now();
            let time_remaining = session
                .absolute_expiry
                .checked_duration_since(now)
                .unwrap_or(Duration::ZERO);
            time_remaining <= self.config.renewal_window
        } else {
            false
        }
    }

    /// Revoke a single session.
    pub fn revoke_session(&mut self, session_id: &str) -> bool {
        self.sessions.remove(session_id).is_some()
    }

    /// Revoke all sessions for a user.
    pub fn revoke_all_user_sessions(&mut self, user_id: &str) -> usize {
        let to_remove: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.user_id == user_id)
            .map(|(id, _)| id.clone())
            .collect();
        let count = to_remove.len();
        for id in to_remove {
            self.sessions.remove(&id);
        }
        count
    }

    /// Remove expired sessions (called periodically).
    pub fn cleanup_expired(&mut self) -> usize {
        let now = Instant::now();
        let config = self.config.clone();
        let before = self.sessions.len();
        self.sessions.retain(|_, s| {
            now < s.absolute_expiry && now.duration_since(s.last_activity) <= config.idle_timeout
        });
        before - self.sessions.len()
    }

    /// List all users (without password hashes).
    pub fn list_users(&self) -> Vec<UserSummary> {
        self.users
            .values()
            .map(|u| UserSummary {
                id: u.id.clone(),
                username: u.username.clone(),
                display_name: u.display_name.clone(),
                email: u.email.clone(),
                role: u.role.clone(),
                disabled: u.disabled,
                created_at: u.created_at,
            })
            .collect()
    }

    /// Get a user by username.
    pub fn get_user(&self, username: &str) -> Option<&StoredUser> {
        self.users.get(username)
    }

    /// Get a user by ID.
    pub fn get_user_by_id(&self, user_id: &str) -> Option<&StoredUser> {
        self.users.values().find(|u| u.id == user_id)
    }

    /// Update a user's details (admin operation).
    pub fn update_user(
        &mut self,
        user_id: &str,
        display_name: Option<&str>,
        email: Option<&str>,
        role: Option<&str>,
        disabled: Option<bool>,
    ) -> Result<StoredUser, String> {
        let user = self
            .users
            .values_mut()
            .find(|u| u.id == user_id)
            .ok_or_else(|| "User not found".to_string())?;

        if let Some(name) = display_name {
            user.display_name = name.to_string();
        }
        if let Some(email) = email {
            user.email = email.to_string();
        }
        if let Some(role) = role {
            user.role = role.to_string();
        }
        if let Some(disabled) = disabled {
            user.disabled = disabled;
        }
        user.updated_at = Utc::now();

        let updated = user.clone();
        self.save().map_err(|e| format!("Failed to persist: {e}"))?;

        Ok(updated)
    }

    /// Change a user's password.
    pub fn change_password(&mut self, user_id: &str, new_password: &str) -> Result<(), String> {
        if new_password.len() < 8 {
            return Err("Password must be at least 8 characters".to_string());
        }

        let user = self
            .users
            .values_mut()
            .find(|u| u.id == user_id)
            .ok_or_else(|| "User not found".to_string())?;

        user.password_hash = hash_password(new_password)?;
        user.updated_at = Utc::now();

        self.save().map_err(|e| format!("Failed to persist: {e}"))?;
        Ok(())
    }

    /// Delete a user by ID.
    pub fn delete_user(&mut self, user_id: &str) -> Result<(), String> {
        let username = self
            .users
            .values()
            .find(|u| u.id == user_id)
            .map(|u| u.username.clone())
            .ok_or_else(|| "User not found".to_string())?;

        self.users.remove(&username);
        self.revoke_all_user_sessions(user_id);
        self.save().map_err(|e| format!("Failed to persist: {e}"))?;

        Ok(())
    }

    /// Get session config (for JWT TTL).
    pub const fn session_config(&self) -> &SessionConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// User summary (safe to return via API, no password hash)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSummary {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub email: String,
    pub role: String,
    pub disabled: bool,
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Password hashing (argon2)
// ---------------------------------------------------------------------------

fn hash_password(password: &str) -> Result<String, String> {
    use argon2::password_hash::rand_core::OsRng;
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default(); // Argon2id with safe defaults

    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("Password hashing failed: {e}"))
}

fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    use argon2::password_hash::PasswordHash;
    use argon2::{Argon2, PasswordVerifier};

    let parsed_hash = PasswordHash::new(hash).map_err(|e| format!("Invalid password hash: {e}"))?;

    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> UserStore {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");
        let _ = dir.keep();
        UserStore::new(path, SessionConfig::default())
    }

    #[test]
    fn test_create_and_verify_user() {
        let mut store = temp_store();
        let user = store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();
        assert_eq!(user.username, "alice");
        assert_eq!(user.role, "admin");

        // Verify correct password
        let verified = store.verify_password("alice", "password123").unwrap();
        assert_eq!(verified.id, user.id);

        // Verify wrong password
        assert!(store.verify_password("alice", "wrong").is_err());
    }

    #[test]
    fn test_duplicate_username() {
        let mut store = temp_store();
        store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();
        assert!(store
            .create_user("alice", "other123456", "Alice2", "a2@test.com", "viewer")
            .is_err());
    }

    #[test]
    fn test_short_password_rejected() {
        let mut store = temp_store();
        assert!(store
            .create_user("alice", "short", "Alice", "alice@test.com", "admin")
            .is_err());
    }

    #[test]
    fn test_session_lifecycle() {
        let mut store = temp_store();
        let user = store
            .create_user("bob", "password123", "Bob", "bob@test.com", "operator")
            .unwrap();

        let session = store.create_session(&user);
        assert!(!session.session_id.is_empty());

        // Validate session
        assert!(store.validate_session(&session.session_id).is_some());

        // Revoke session
        assert!(store.revoke_session(&session.session_id));
        assert!(store.validate_session(&session.session_id).is_none());
    }

    #[test]
    fn test_max_parallel_sessions() {
        let config = SessionConfig {
            max_parallel_sessions: 2,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");
        let _ = dir.keep();
        let mut store = UserStore::new(path, config);

        let user = store
            .create_user("carol", "password123", "Carol", "carol@test.com", "viewer")
            .unwrap();

        let s1 = store.create_session(&user);
        let s2 = store.create_session(&user);
        let s3 = store.create_session(&user); // should evict s1

        // s1 should be evicted (oldest)
        assert!(store.validate_session(&s1.session_id).is_none());
        assert!(store.validate_session(&s2.session_id).is_some());
        assert!(store.validate_session(&s3.session_id).is_some());
    }

    #[test]
    fn test_list_users() {
        let mut store = temp_store();
        store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();
        store
            .create_user("bob", "password456", "Bob", "bob@test.com", "viewer")
            .unwrap();

        let list = store.list_users();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");

        // Create store with users
        {
            let mut store = UserStore::new(path.clone(), SessionConfig::default());
            store
                .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
                .unwrap();
        }

        // Load from same file
        let store = UserStore::new(path, SessionConfig::default());
        assert_eq!(store.list_users().len(), 1);
        assert!(store.verify_password("alice", "password123").is_ok());
    }

    #[test]
    fn test_update_user() {
        let mut store = temp_store();
        let user = store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();

        let updated = store
            .update_user(&user.id, Some("Alice Updated"), None, Some("viewer"), None)
            .unwrap();

        assert_eq!(updated.display_name, "Alice Updated");
        assert_eq!(updated.role, "viewer");
    }

    #[test]
    fn test_delete_user() {
        let mut store = temp_store();
        let user = store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();

        store.delete_user(&user.id).unwrap();
        assert!(store.list_users().is_empty());
    }

    #[test]
    fn test_disabled_user_cannot_login() {
        let mut store = temp_store();
        let user = store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();
        store
            .update_user(&user.id, None, None, None, Some(true))
            .unwrap();

        assert!(store.verify_password("alice", "password123").is_err());
    }

    #[test]
    fn test_change_password() {
        let mut store = temp_store();
        let user = store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();

        store.change_password(&user.id, "newpassword456").unwrap();
        assert!(store.verify_password("alice", "password123").is_err());
        assert!(store.verify_password("alice", "newpassword456").is_ok());
    }

    #[test]
    fn test_revoke_all_user_sessions() {
        let mut store = temp_store();
        let user = store
            .create_user("alice", "password123", "Alice", "alice@test.com", "admin")
            .unwrap();

        store.create_session(&user);
        store.create_session(&user);
        store.create_session(&user);

        let revoked = store.revoke_all_user_sessions(&user.id);
        assert_eq!(revoked, 3);
    }
}
