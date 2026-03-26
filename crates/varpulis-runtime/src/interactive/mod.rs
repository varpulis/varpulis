//! Interactive session core for Varpulis.
//!
//! Provides the protocol types and [`InteractiveSession`] that serve as the
//! foundation for JSON-line, TUI, and MCP transports.

pub mod protocol;
pub mod session;

pub use protocol::{SessionCommand, SessionResponse};
pub use session::InteractiveSession;
