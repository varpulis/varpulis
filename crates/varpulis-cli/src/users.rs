//! Session management and password hashing utilities for local authentication.
//!
//! Provides in-memory session tracking with idle/absolute timeouts,
//! and argon2id password hashing for local username/password auth.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

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
            idle_timeout: Duration::from_mins(30),      // 30 minutes
            absolute_timeout: Duration::from_hours(24), // 24 hours
            max_parallel_sessions: 5,
            renewal_window: Duration::from_mins(5), // 5 minutes before expiry
        }
    }
}

/// In-memory session manager (no file persistence).
#[derive(Debug)]
pub struct SessionManager {
    sessions: HashMap<String, SessionRecord>,
    config: SessionConfig,
}

pub type SharedSessionManager = Arc<RwLock<SessionManager>>;

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl SessionManager {
    /// Create a new session manager with the given configuration.
    pub fn new(config: SessionConfig) -> Self {
        Self {
            sessions: HashMap::new(),
            config,
        }
    }

    /// Create a new session for a user. Evicts oldest session if max exceeded.
    pub fn create_session(&mut self, user_id: &str, username: &str, role: &str) -> SessionRecord {
        let now = Instant::now();

        // Count existing sessions for this user
        let user_sessions: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.user_id == user_id)
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
            user_id: user_id.to_string(),
            username: username.to_string(),
            role: role.to_string(),
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

pub fn hash_password(password: &str) -> Result<String, String> {
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

pub fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
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

    #[test]
    fn test_session_lifecycle() {
        let mut mgr = SessionManager::new(SessionConfig::default());
        let session = mgr.create_session("user-1", "bob", "operator");
        assert!(!session.session_id.is_empty());

        // Validate session
        assert!(mgr.validate_session(&session.session_id).is_some());

        // Revoke session
        assert!(mgr.revoke_session(&session.session_id));
        assert!(mgr.validate_session(&session.session_id).is_none());
    }

    #[test]
    fn test_max_parallel_sessions() {
        let config = SessionConfig {
            max_parallel_sessions: 2,
            ..Default::default()
        };
        let mut mgr = SessionManager::new(config);

        let s1 = mgr.create_session("user-1", "carol", "viewer");
        let s2 = mgr.create_session("user-1", "carol", "viewer");
        let s3 = mgr.create_session("user-1", "carol", "viewer"); // should evict s1

        // s1 should be evicted (oldest)
        assert!(mgr.validate_session(&s1.session_id).is_none());
        assert!(mgr.validate_session(&s2.session_id).is_some());
        assert!(mgr.validate_session(&s3.session_id).is_some());
    }

    #[test]
    fn test_revoke_all_user_sessions() {
        let mut mgr = SessionManager::new(SessionConfig::default());
        mgr.create_session("user-1", "alice", "admin");
        mgr.create_session("user-1", "alice", "admin");
        mgr.create_session("user-1", "alice", "admin");

        let revoked = mgr.revoke_all_user_sessions("user-1");
        assert_eq!(revoked, 3);
    }

    #[test]
    fn test_password_hash_and_verify() {
        let hash = hash_password("password123").unwrap();
        assert!(verify_password("password123", &hash).unwrap());
        assert!(!verify_password("wrong", &hash).unwrap());
    }

    #[test]
    fn test_short_password_still_hashes() {
        // Validation of password length is the caller's responsibility
        let hash = hash_password("short").unwrap();
        assert!(verify_password("short", &hash).unwrap());
    }
}
