use anyhow::Result;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum FederationAction {
    /// Show federation status
    Status,
    /// List all regions
    Regions,
    /// Add a region to the federation
    AddRegion {
        /// Region name
        #[arg(long)]
        name: String,
        /// Coordinator URL for the region
        #[arg(long)]
        url: String,
        /// NATS URL for the region
        #[arg(long)]
        nats_url: Option<String>,
        /// Region priority (lower = higher)
        #[arg(long, default_value = "100")]
        priority: u32,
    },
    /// Remove a region from the federation
    RemoveRegion {
        /// Region name to remove
        name: String,
    },
    /// Show global pipeline catalog
    Catalog,
}

pub async fn run_federation(
    action: FederationAction,
    coordinator: &str,
    api_key: Option<&str>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let base = format!("{}/api/v1/federation", coordinator.trim_end_matches('/'));

    let request_builder = |method: reqwest::Method, url: &str| {
        let mut rb = client.request(method, url);
        if let Some(key) = api_key {
            rb = rb.header("Authorization", format!("Bearer {key}"));
        }
        rb
    };

    match action {
        FederationAction::Status => {
            let resp = request_builder(reqwest::Method::GET, &format!("{base}/status"))
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("Federation status failed: {}", resp.status());
            }
            let body: serde_json::Value = resp.json().await?;
            println!("{}", serde_json::to_string_pretty(&body)?);
        }

        FederationAction::Regions => {
            let resp = request_builder(reqwest::Method::GET, &format!("{base}/regions"))
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("Federation regions failed: {}", resp.status());
            }
            let body: serde_json::Value = resp.json().await?;
            println!("{}", serde_json::to_string_pretty(&body)?);
        }

        FederationAction::AddRegion {
            name,
            url,
            nats_url,
            priority,
        } => {
            let payload = serde_json::json!({
                "name": name,
                "coordinator_url": url,
                "nats_url": nats_url.unwrap_or_default(),
                "priority": priority,
            });
            let resp = request_builder(reqwest::Method::POST, &format!("{base}/regions"))
                .json(&payload)
                .send()
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Add region failed ({status}): {body}");
            }
            println!("Region '{name}' added successfully.");
        }

        FederationAction::RemoveRegion { name } => {
            let resp = request_builder(reqwest::Method::DELETE, &format!("{base}/regions/{name}"))
                .send()
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Remove region failed ({status}): {body}");
            }
            println!("Region '{name}' removed successfully.");
        }

        FederationAction::Catalog => {
            let resp = request_builder(reqwest::Method::GET, &format!("{base}/catalog"))
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("Federation catalog failed: {}", resp.status());
            }
            let body: serde_json::Value = resp.json().await?;
            println!("{}", serde_json::to_string_pretty(&body)?);
        }
    }

    Ok(())
}
