//! Varpulis MCP Server binary.
//!
//! Exposes the Varpulis CEP engine to AI agents via the Model Context Protocol.
//! Supports stdio transport (for Claude Desktop, Cursor, etc.).

use clap::Parser;
use rmcp::ServiceExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use varpulis_mcp::{CoordinatorClient, VarpulisMcpServer};

#[derive(Parser, Debug)]
#[command(
    name = "varpulis-mcp",
    about = "MCP server for the Varpulis CEP engine"
)]
struct Args {
    /// Coordinator API URL
    #[arg(
        long,
        default_value = "http://localhost:9100",
        env = "VARPULIS_COORDINATOR_URL"
    )]
    coordinator_url: String,

    /// API key for coordinator authentication
    #[arg(long, env = "VARPULIS_API_KEY")]
    api_key: Option<String>,

    /// Transport mode: "stdio"
    #[arg(long, default_value = "stdio", env = "VARPULIS_MCP_TRANSPORT")]
    transport: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logging goes to stderr (stdout reserved for MCP in stdio mode)
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "varpulis_mcp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let args = Args::parse();

    tracing::info!(
        coordinator_url = %args.coordinator_url,
        transport = %args.transport,
        "Starting Varpulis MCP server"
    );

    let client = CoordinatorClient::new(args.coordinator_url, args.api_key);
    let server = VarpulisMcpServer::new(client);

    match args.transport.as_str() {
        "stdio" => {
            let service = server
                .serve(rmcp::transport::stdio())
                .await
                .inspect_err(|e| {
                    tracing::error!("Failed to start MCP server: {}", e);
                })?;
            service.waiting().await?;
        }
        other => {
            anyhow::bail!(
                "Unsupported transport: {}. Only 'stdio' is currently supported.",
                other
            );
        }
    }

    Ok(())
}
