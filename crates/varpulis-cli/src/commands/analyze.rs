use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Color, Table};
use owo_colors::OwoColorize;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::info;
use varpulis_connectors::credentials::CredentialsStore;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;
use varpulis_runtime::SharedEvent;

use super::detect::collect_vpl_files;
use crate::output;

/// Output format for analyze results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnalyzeFormat {
    Table,
    Json,
    Markdown,
}

impl std::str::FromStr for AnalyzeFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "json" => Ok(Self::Json),
            "markdown" | "md" => Ok(Self::Markdown),
            _ => Err(format!(
                "unknown format '{s}', expected: table, json, markdown"
            )),
        }
    }
}

/// Coverage verdict for a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Verdict {
    /// Detects both baseline and evasion.
    Resilient,
    /// Detects baseline but misses evasion.
    Evadable,
    /// Misses baseline (rule is broken or wrong dataset).
    Broken,
    /// No detections at all.
    Silent,
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resilient => write!(f, "RESILIENT"),
            Self::Evadable => write!(f, "EVADABLE"),
            Self::Broken => write!(f, "BROKEN"),
            Self::Silent => write!(f, "SILENT"),
        }
    }
}

/// Coverage result for a single rule file.
#[derive(Debug, Clone, Serialize)]
pub struct RuleCoverage {
    pub rule_file: String,
    pub rule_name: String,
    pub mitre: String,
    pub baseline_alerts: u64,
    pub evasion_alerts: u64,
    pub verdict: Verdict,
}

/// Run a VPL rule file against events, returning the count of alerts and
/// the rule name / MITRE technique extracted from the first alert.
async fn run_and_count(
    rule_path: &Path,
    events: &[Event],
    credentials_store: &Option<Arc<CredentialsStore>>,
) -> Result<(u64, String, String)> {
    let program_source = std::fs::read_to_string(rule_path)?;
    let program = parse(&program_source).map_err(|e| {
        anyhow::anyhow!(
            "Parse error in {}: {e}",
            rule_path.file_name().unwrap_or_default().to_string_lossy()
        )
    })?;
    let program = Arc::new(program);

    let (output_tx, mut output_rx) = mpsc::channel::<SharedEvent>(1000);

    let mut engine = {
        let mut b = Engine::builder().shared_output(output_tx.clone());
        if let Some(ref store) = credentials_store {
            b = b.credentials(Arc::clone(store));
        }
        b.build()
    };
    engine
        .load_with_source(&program_source, &program)
        .map_err(|e| {
            anyhow::anyhow!(
                "Load error in {}: {e}",
                rule_path.file_name().unwrap_or_default().to_string_lossy()
            )
        })?;

    // Collect alerts
    let collected: Arc<tokio::sync::Mutex<Vec<SharedEvent>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let collected_clone = collected.clone();

    let collector = tokio::spawn(async move {
        while let Some(event) = output_rx.recv().await {
            collected_clone.lock().await.push(event);
        }
    });

    // Process events
    let events_owned: Vec<Event> = events.to_vec();
    if !engine.has_sink_operations() {
        engine
            .process_batch_sync(events_owned)
            .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
    } else {
        engine
            .process_batch(events_owned)
            .await
            .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
    }

    drop(output_tx);
    drop(engine);
    let _ = collector.await;

    let events_vec = collected.lock().await;
    let count = events_vec.len() as u64;

    // Extract rule name and MITRE from first alert
    let (rule_name, mitre) = if let Some(first) = events_vec.first() {
        let rule = first
            .get("rule")
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| first.event_type.to_string());
        let mitre = first
            .get("mitre")
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        (rule, mitre)
    } else {
        // No alerts — infer rule name from filename
        let name = rule_path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        (name, String::new())
    };

    Ok((count, rule_name, mitre))
}

/// Run the analyze (red mode) command.
pub async fn run_analyze(
    rules_path: &Path,
    baseline_path: &Path,
    evasion_path: &Path,
    format: AnalyzeFormat,
    credentials_store: Option<Arc<CredentialsStore>>,
) -> Result<()> {
    // Discover rule files
    let rule_files: Vec<PathBuf> = if rules_path.is_file() {
        vec![rules_path.to_path_buf()]
    } else if rules_path.is_dir() {
        let mut files = Vec::new();
        collect_vpl_files(rules_path, &mut files)?;
        files.sort();
        if files.is_empty() {
            anyhow::bail!("no .vpl files found in '{}'", rules_path.display());
        }
        files
    } else {
        anyhow::bail!(
            "rules path '{}' is not a file or directory",
            rules_path.display()
        );
    };

    output::header("Varpulis Evasion Analysis");
    println!(
        "Rules:    {} file(s) from {}",
        rule_files.len(),
        rules_path.display()
    );
    println!("Baseline: {}", baseline_path.display());
    println!("Evasion:  {}", evasion_path.display());
    println!();

    // Load both datasets
    let baseline_source = std::fs::read_to_string(baseline_path)?;
    let baseline_timed = EventFileParser::parse(&baseline_source)
        .map_err(|e| anyhow::anyhow!("Baseline event file error: {e}"))?;
    let baseline_events: Vec<Event> = baseline_timed.into_iter().map(|te| te.event).collect();

    let evasion_source = std::fs::read_to_string(evasion_path)?;
    let evasion_timed = EventFileParser::parse(&evasion_source)
        .map_err(|e| anyhow::anyhow!("Evasion event file error: {e}"))?;
    let evasion_events: Vec<Event> = evasion_timed.into_iter().map(|te| te.event).collect();

    println!(
        "Loaded {} baseline events, {} evasion events",
        baseline_events.len(),
        evasion_events.len()
    );
    println!();

    let start = std::time::Instant::now();

    let spinner = output::create_spinner(&format!(
        "Analyzing {} rules against baseline + evasion datasets...",
        rule_files.len()
    ));

    // Run each rule against both datasets
    let mut results: Vec<RuleCoverage> = Vec::new();

    for rule_path in &rule_files {
        info!("Analyzing rule: {}", rule_path.display());

        let baseline_result = run_and_count(rule_path, &baseline_events, &credentials_store).await;
        let evasion_result = run_and_count(rule_path, &evasion_events, &credentials_store).await;

        match (baseline_result, evasion_result) {
            (Ok((baseline_alerts, rule_name, mitre)), Ok((evasion_alerts, _, mitre2))) => {
                let mitre = if mitre.is_empty() { mitre2 } else { mitre };

                let verdict = match (baseline_alerts > 0, evasion_alerts > 0) {
                    (true, true) => Verdict::Resilient,
                    (true, false) => Verdict::Evadable,
                    (false, true) => Verdict::Broken,
                    (false, false) => Verdict::Silent,
                };

                results.push(RuleCoverage {
                    rule_file: rule_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string(),
                    rule_name,
                    mitre,
                    baseline_alerts,
                    evasion_alerts,
                    verdict,
                });
            }
            (Err(e), _) | (_, Err(e)) => {
                eprintln!(
                    "  Warning: skipping {} ({})",
                    rule_path.file_name().unwrap_or_default().to_string_lossy(),
                    e
                );
            }
        }
    }

    spinner.finish_and_clear();

    let duration = start.elapsed();

    // Render output
    match format {
        AnalyzeFormat::Json => {
            for result in &results {
                println!(
                    "{}",
                    serde_json::to_string(result).unwrap_or_else(|_| "{}".to_string())
                );
            }
        }
        AnalyzeFormat::Markdown => {
            println!("# Evasion Analysis Report\n");
            println!("| Rule | MITRE | Baseline | Evasion | Verdict |");
            println!("|------|-------|----------|---------|---------|");
            for r in &results {
                let baseline_str = if r.baseline_alerts > 0 {
                    format!("DETECT ({})", r.baseline_alerts)
                } else {
                    "MISS".to_string()
                };
                let evasion_str = if r.evasion_alerts > 0 {
                    format!("DETECT ({})", r.evasion_alerts)
                } else {
                    "MISS".to_string()
                };
                println!(
                    "| {} | {} | {} | {} | **{}** |",
                    r.rule_name, r.mitre, baseline_str, evasion_str, r.verdict
                );
            }
            println!();
        }
        AnalyzeFormat::Table => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec![
                Cell::new("Rule"),
                Cell::new("MITRE"),
                Cell::new("Baseline"),
                Cell::new("Evasion"),
                Cell::new("Verdict"),
            ]);

            for r in &results {
                let baseline_cell = if r.baseline_alerts > 0 {
                    Cell::new(format!("DETECT ({})", r.baseline_alerts)).fg(Color::Green)
                } else {
                    Cell::new("MISS").fg(Color::Red)
                };
                let evasion_cell = if r.evasion_alerts > 0 {
                    Cell::new(format!("DETECT ({})", r.evasion_alerts)).fg(Color::Green)
                } else {
                    Cell::new("MISS").fg(Color::Red)
                };
                let verdict_color = match r.verdict {
                    Verdict::Resilient => Color::Green,
                    Verdict::Evadable => Color::Red,
                    Verdict::Broken => Color::DarkRed,
                    Verdict::Silent => Color::DarkGrey,
                };

                table.add_row(vec![
                    Cell::new(&r.rule_name),
                    Cell::new(&r.mitre),
                    baseline_cell,
                    evasion_cell,
                    Cell::new(r.verdict.to_string()).fg(verdict_color),
                ]);
            }

            println!("{table}");

            // Summary
            println!();
            let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
            for r in &results {
                *counts
                    .entry(match r.verdict {
                        Verdict::Resilient => "resilient",
                        Verdict::Evadable => "evadable",
                        Verdict::Broken => "broken",
                        Verdict::Silent => "silent",
                    })
                    .or_default() += 1;
            }

            let resilient = counts.get("resilient").copied().unwrap_or(0);
            let evadable = counts.get("evadable").copied().unwrap_or(0);
            let total = results.len() as u64;

            let summary = if evadable > 0 {
                format!("{}/{} rules evadable", evadable, total)
                    .red()
                    .bold()
                    .to_string()
            } else {
                format!("{}/{} rules resilient", resilient, total)
                    .green()
                    .bold()
                    .to_string()
            };
            println!("{summary} | analyzed in {:.1}s", duration.as_secs_f64());
        }
    }

    Ok(())
}
