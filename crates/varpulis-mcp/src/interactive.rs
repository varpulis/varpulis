//! MCP session manager for interactive Varpulis sessions.
//!
//! Wraps [`InteractiveSession`] instances behind an `Arc<RwLock<McpSessionManager>>`
//! so that MCP tool handlers can create, drive, and clean up sessions concurrently.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use varpulis_runtime::interactive::{InteractiveSession, SessionCommand, SessionResponse};

/// Idle timeout after which sessions are reaped.
const SESSION_TIMEOUT: Duration = Duration::from_secs(600);

/// Thread-safe handle to the session manager, suitable for embedding in the
/// MCP server struct (which must be `Clone + Send + Sync`).
pub type SharedSessionManager = Arc<RwLock<McpSessionManager>>;

/// Manages interactive sessions for MCP clients.
pub struct McpSessionManager {
    sessions: HashMap<String, SessionEntry>,
}

struct SessionEntry {
    session: InteractiveSession,
    last_active: Instant,
    event_buffer: Vec<SessionResponse>,
}

impl std::fmt::Debug for McpSessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpSessionManager")
            .field("session_count", &self.sessions.len())
            .finish()
    }
}

impl Default for McpSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl McpSessionManager {
    /// Create a new, empty session manager.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Create a new interactive session, optionally loading VPL upfront.
    ///
    /// Returns the session ID and any responses from the initial load.
    pub fn start_session(&mut self, vpl: Option<String>) -> (String, Vec<SessionResponse>) {
        // Reap expired sessions on each creation to keep memory bounded.
        self.reap_expired();

        let session_id = uuid::Uuid::new_v4().to_string();
        let mut session = InteractiveSession::new();
        let responses = if let Some(vpl) = vpl {
            session.handle_command(SessionCommand::LoadVpl { vpl })
        } else {
            Vec::new()
        };

        self.sessions.insert(
            session_id.clone(),
            SessionEntry {
                session,
                last_active: Instant::now(),
                event_buffer: Vec::new(),
            },
        );

        (session_id, responses)
    }

    /// Send a command to an existing session.
    pub fn send_command(
        &mut self,
        session_id: &str,
        command: SessionCommand,
    ) -> Result<Vec<SessionResponse>, String> {
        let entry = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session not found: {session_id}"))?;
        entry.last_active = Instant::now();
        let responses = entry.session.handle_command(command);
        // Buffer any output events so they can be retrieved later via get_events.
        entry.event_buffer.extend(responses.clone());
        Ok(responses)
    }

    /// Retrieve buffered output events, draining up to `limit` entries.
    pub fn get_events(
        &mut self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionResponse>, String> {
        let entry = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session not found: {session_id}"))?;
        entry.last_active = Instant::now();

        // Also drain any generator output that may have arrived.
        let gen_responses = entry.session.poll_generator();
        entry.event_buffer.extend(gen_responses);

        let drain_count = limit.min(entry.event_buffer.len());
        let events: Vec<SessionResponse> = entry.event_buffer.drain(..drain_count).collect();
        Ok(events)
    }

    /// Remove sessions that have been idle longer than `SESSION_TIMEOUT`.
    pub fn reap_expired(&mut self) {
        self.sessions
            .retain(|_, entry| entry.last_active.elapsed() < SESSION_TIMEOUT);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_session_without_vpl() {
        let mut mgr = McpSessionManager::new();
        let (id, responses) = mgr.start_session(None);
        assert!(!id.is_empty());
        assert!(responses.is_empty());
        assert!(mgr.sessions.contains_key(&id));
    }

    #[test]
    fn test_start_session_with_vpl() {
        let mut mgr = McpSessionManager::new();
        let (id, responses) = mgr.start_session(Some("stream A = Tick .emit(v: x)".to_string()));
        assert!(!id.is_empty());
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Loaded { streams, .. } => {
                assert!(streams.contains(&"A".to_string()));
            }
            other => panic!("Expected Loaded, got {other:?}"),
        }
    }

    #[test]
    fn test_send_command_inject() {
        let mut mgr = McpSessionManager::new();
        let (id, _) = mgr.start_session(Some("stream Echo = Tick .emit(price: price)".to_string()));

        let result = mgr.send_command(
            &id,
            SessionCommand::Inject {
                event_type: "Tick".into(),
                data: serde_json::json!({"price": 42.5}),
            },
        );
        assert!(result.is_ok());
        let responses = result.unwrap();
        let has_output = responses
            .iter()
            .any(|r| matches!(r, SessionResponse::Output { .. }));
        assert!(has_output, "Expected Output response, got: {responses:?}");
    }

    #[test]
    fn test_send_command_to_unknown_session() {
        let mut mgr = McpSessionManager::new();
        let result = mgr.send_command("nonexistent-id", SessionCommand::GetStreams);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Session not found"));
    }

    #[test]
    fn test_reap_expired() {
        let mut mgr = McpSessionManager::new();
        let (id, _) = mgr.start_session(None);

        // Manually backdate the session's last_active to simulate expiry.
        mgr.sessions.get_mut(&id).unwrap().last_active = Instant::now() - Duration::from_secs(700);

        mgr.reap_expired();
        assert!(mgr.sessions.is_empty());
    }
}
