//! VarpulisQL Language Server
//!
//! Provides IDE features for VarpulisQL via the Language Server Protocol:
//! - Real-time diagnostics
//! - Hover documentation
//! - Code completion
//! - Go-to-definition
//! - Semantic tokens

mod completion;
mod diagnostics;
mod hover;
mod semantic;
mod server;

use tower_lsp::{LspService, Server};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Initialize logging to stderr (stdout is used for LSP communication)
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "varpulis_lsp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    tracing::info!("Starting VarpulisQL Language Server");

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(server::Backend::new);
    Server::new(stdin, stdout, socket).serve(service).await;
}
