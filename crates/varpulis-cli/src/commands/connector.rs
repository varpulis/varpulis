use anyhow::Result;
use clap::Subcommand;
use comfy_table::{presets::UTF8_FULL, ContentArrangement, Table};
use owo_colors::OwoColorize;
use varpulis_connector_api::component;

#[derive(Subcommand)]
pub enum ConnectorAction {
    /// List all available connectors
    List,
    /// Show detailed information about a connector
    Info {
        /// Connector type identifier (e.g., "kafka", "mqtt")
        connector_type: String,
    },
    /// Test connectivity to a service
    Test {
        /// Connector type identifier (e.g., "kafka", "mqtt")
        connector_type: String,
        /// Service URL to test connectivity against
        #[arg(long)]
        url: String,
    },
}

/// Entry point dispatching to the appropriate sub-action.
pub fn run_connector(action: ConnectorAction) -> Result<()> {
    match action {
        ConnectorAction::List => list(),
        ConnectorAction::Info { connector_type } => info(&connector_type),
        ConnectorAction::Test {
            connector_type,
            url,
        } => test(&connector_type, &url),
    }
}

/// Display all registered connectors in a formatted table.
fn list() -> Result<()> {
    let mut components = component::list_components();
    components.sort_by_key(|c| c.connector_type);

    if components.is_empty() {
        println!("No connectors registered.");
        return Ok(());
    }

    let check = "\u{2713}".green().to_string();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Type", "Description", "Source", "Sink", "Managed"]);

    for c in &components {
        table.add_row(vec![
            c.connector_type.to_string(),
            c.description.to_string(),
            if c.supports_source {
                check.clone()
            } else {
                String::new()
            },
            if c.supports_sink {
                check.clone()
            } else {
                String::new()
            },
            if c.supports_managed {
                check.clone()
            } else {
                String::new()
            },
        ]);
    }

    println!("{table}");
    println!("\n{} connector(s) registered.", components.len());
    Ok(())
}

/// Show detailed information for a specific connector type.
fn info(connector_type: &str) -> Result<()> {
    let factory = component::find_factory(connector_type).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown connector type: '{}'. Run 'varpulis connector list' to see available connectors.",
            connector_type
        )
    })?;

    let c = factory.info();
    let check = "\u{2713}".green().to_string();
    let cross = "\u{2717}";

    println!("Connector: {}", c.connector_type);
    println!("Name:        {}", c.display_name);
    println!("Description: {}", c.description);
    if !c.feature_flag.is_empty() {
        println!("Feature:     {}", c.feature_flag);
    }
    println!(
        "Supports:    Source {}  Sink {}  Managed {}",
        if c.supports_source { &check } else { cross },
        if c.supports_sink { &check } else { cross },
        if c.supports_managed { &check } else { cross },
    );

    if !c.config_params.is_empty() {
        println!("\nConfiguration Parameters:");
        for p in c.config_params {
            let req = if p.required { " (required)" } else { "" };
            let def = match p.default_value {
                Some(v) => format!(" [default: {v}]"),
                None => String::new(),
            };
            println!("  {}{} — {}{}", p.name, req, p.description, def);
        }
    }

    // Generate an example VPL snippet based on known required params
    let required_params: Vec<&str> = c
        .config_params
        .iter()
        .filter(|p| p.required)
        .map(|p| p.name)
        .collect();

    println!("\nExample VPL:");
    if required_params.is_empty() {
        println!(
            "  connector My{} = {}()",
            capitalize(c.connector_type),
            c.connector_type
        );
    } else {
        println!(
            "  connector My{} = {}(",
            capitalize(c.connector_type),
            c.connector_type
        );
        for (i, name) in required_params.iter().enumerate() {
            let trailing = if i + 1 < required_params.len() {
                ","
            } else {
                ""
            };
            println!("      {}: \"...\"{}", name, trailing);
        }
        println!("  )");
    }

    Ok(())
}

/// Basic connectivity test for a connector type.
fn test(connector_type: &str, url: &str) -> Result<()> {
    // Verify the connector type exists first
    let _factory = component::find_factory(connector_type).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown connector type: '{}'. Run 'varpulis connector list' to see available connectors.",
            connector_type
        )
    })?;

    println!(
        "Connectivity test not yet implemented for '{}' (url: {})",
        connector_type, url
    );
    println!("This feature will be available in a future release.");

    Ok(())
}

/// Capitalize the first letter of a string.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_returns_non_empty() {
        let components = component::list_components();
        assert!(
            !components.is_empty(),
            "Expected at least one registered connector"
        );
    }

    #[test]
    fn test_info_http_connector() {
        let factory = component::find_factory("http");
        assert!(
            factory.is_some(),
            "HTTP connector should always be available"
        );
        let info = factory.unwrap().info();
        assert_eq!(info.connector_type, "http");
        assert!(info.supports_source);
        assert!(info.supports_sink);
    }

    #[test]
    fn test_info_unknown_connector_returns_error() {
        let result = info("nonexistent_connector_xyz");
        assert!(result.is_err());
    }

    #[test]
    fn test_capitalize() {
        assert_eq!(capitalize("kafka"), "Kafka");
        assert_eq!(capitalize("http"), "Http");
        assert_eq!(capitalize(""), "");
        assert_eq!(capitalize("a"), "A");
    }

    #[test]
    fn test_list_command_runs() {
        // Should not panic or error
        let result = list();
        assert!(result.is_ok());
    }

    #[test]
    fn test_test_unknown_connector_returns_error() {
        let result = test("nonexistent_connector_xyz", "localhost:1234");
        assert!(result.is_err());
    }
}
