//! Varpulis MCP Server
//!
//! Exposes the Varpulis CEP engine to AI agents via the Model Context Protocol.
//! Provides tools for VPL validation, pipeline deployment, metrics querying,
//! and alert investigation.

pub mod client;
pub mod error;
pub mod prompts;
pub mod resources;
pub mod server;
pub mod tools;

pub use client::CoordinatorClient;
pub use server::VarpulisMcpServer;
