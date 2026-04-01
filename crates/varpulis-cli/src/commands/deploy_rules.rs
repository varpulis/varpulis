use std::path::Path;

use anyhow::Result;
use owo_colors::OwoColorize;
use varpulis_cli::client::VarpulisClient;
use varpulis_cli::output;

use super::detect::collect_vpl_files;

/// Deploy a directory of VPL rule files to a running Varpulis server.
pub async fn run_deploy_rules(
    dir: &Path,
    server: &str,
    api_key: &str,
    undeploy_existing: bool,
) -> Result<()> {
    let client = VarpulisClient::new(server, api_key);

    // Discover VPL files
    let mut vpl_files = Vec::new();
    collect_vpl_files(dir, &mut vpl_files)?;
    vpl_files.sort();

    if vpl_files.is_empty() {
        anyhow::bail!("no .vpl files found in '{}'", dir.display());
    }

    output::header("Deploy Security Rules");
    println!("Server:  {server}");
    println!(
        "Rules:   {} file(s) from {}",
        vpl_files.len(),
        dir.display()
    );
    println!();

    // Undeploy existing pipelines if requested
    if undeploy_existing {
        println!("{}", "Removing existing pipelines...".yellow());
        match client.list_pipelines().await {
            Ok(list) => {
                for pipeline in &list.pipelines {
                    match client.delete_pipeline(&pipeline.id).await {
                        Ok(()) => println!("  Removed: {} ({})", pipeline.name, pipeline.id),
                        Err(e) => eprintln!("  Failed to remove {}: {e}", pipeline.name),
                    }
                }
                if !list.pipelines.is_empty() {
                    println!("  Removed {} existing pipelines", list.pipelines.len());
                }
            }
            Err(e) => eprintln!("  Warning: could not list pipelines: {e}"),
        }
        println!();
    }

    // Deploy each rule file
    let mut deployed = 0u64;
    let mut failed = 0u64;

    for path in &vpl_files {
        let name = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let source = std::fs::read_to_string(path)?;

        match client.deploy_pipeline(&name, &source).await {
            Ok(resp) => {
                deployed += 1;
                output::success(&format!("  Deployed: {} (id: {})", name, resp.id));
            }
            Err(e) => {
                failed += 1;
                output::error(&format!("  Failed: {} — {e}", name));
            }
        }
    }

    // Summary
    println!();
    if failed == 0 {
        output::success(&format!(
            "{deployed} rule(s) deployed successfully to {server}"
        ));
    } else {
        output::error(&format!(
            "{deployed} deployed, {failed} failed out of {} total",
            deployed + failed
        ));
    }

    Ok(())
}
