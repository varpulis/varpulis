//! MCP server implementation for Varpulis.

use crate::client::CoordinatorClient;
use crate::prompts;
use crate::resources;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{tool_handler, ErrorData as McpError, RoleServer, ServerHandler};

/// The Varpulis MCP server.
#[derive(Clone)]
pub struct VarpulisMcpServer {
    pub(crate) client: CoordinatorClient,
    pub(crate) tool_router: ToolRouter<Self>,
}

impl VarpulisMcpServer {
    pub fn new(client: CoordinatorClient) -> Self {
        Self {
            client,
            tool_router: Self::create_tool_router(),
        }
    }
}

#[tool_handler]
impl ServerHandler for VarpulisMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Varpulis CEP Engine MCP server. Provides tools for deploying, monitoring, \
                 and debugging real-time stream processing pipelines written in VPL \
                 (Varpulis Processing Language)."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
            server_info: Implementation {
                name: "varpulis-mcp".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    // ─── Resources ───────────────────────────────────────────────────

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult {
            resources: resources::list_resources(),
            next_cursor: None,
            ..Default::default()
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        Ok(ListResourceTemplatesResult {
            resource_templates: resources::list_resource_templates(),
            next_cursor: None,
            ..Default::default()
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        resources::read_resource(&request.uri, &self.client)
            .await
            .map_err(|e| McpError::internal_error(e, None))
    }

    // ─── Prompts ─────────────────────────────────────────────────────

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        Ok(ListPromptsResult {
            prompts: prompts::list_prompts(),
            next_cursor: None,
            ..Default::default()
        })
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let args = request.arguments.unwrap_or_default();
        prompts::get_prompt(&request.name, &args).map_err(|e| McpError::invalid_params(e, None))
    }
}
