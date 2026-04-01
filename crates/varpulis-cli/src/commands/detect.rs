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

use crate::output;

/// Output format for detect results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    /// Colored table for terminal display.
    Table,
    /// One JSON object per alert, newline-delimited.
    Json,
    /// ArcSight Common Event Format (CEF).
    Cef,
    /// Markdown table.
    Markdown,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "json" => Ok(Self::Json),
            "cef" => Ok(Self::Cef),
            "markdown" | "md" => Ok(Self::Markdown),
            _ => Err(format!(
                "unknown format '{s}', expected: table, json, cef, markdown"
            )),
        }
    }
}

/// A structured security alert extracted from an emitted VPL event.
#[derive(Debug, Clone, Serialize)]
pub struct SecurityAlert {
    pub rule: String,
    pub mitre: String,
    pub severity: String,
    pub summary: String,
    #[serde(flatten)]
    pub fields: indexmap::IndexMap<String, serde_json::Value>,
}

impl SecurityAlert {
    /// Extract a security alert from an engine output event.
    pub fn from_event(event: &Event) -> Self {
        let get_str = |key: &str| -> String {
            event
                .get(key)
                .map(|v| match v {
                    Value::Str(s) => s.to_string(),
                    other => other.to_string(),
                })
                .unwrap_or_default()
        };

        let rule = get_str("rule");
        let rule = if rule.is_empty() {
            event.event_type.to_string()
        } else {
            rule
        };

        let mitre = get_str("mitre");
        let severity = get_str("severity");
        let severity = if severity.is_empty() {
            "medium".to_string()
        } else {
            severity
        };
        let summary = get_str("summary");

        // Collect remaining fields
        let skip = ["rule", "mitre", "severity", "summary", "event_type"];
        let mut fields = indexmap::IndexMap::new();
        for (key, val) in &event.data {
            if !skip.contains(&key.as_ref()) {
                fields.insert(key.to_string(), value_to_json(val));
            }
        }

        Self {
            rule,
            mitre,
            severity,
            summary,
            fields,
        }
    }

    /// Format as CEF string.
    pub fn to_cef(&self) -> String {
        let cef_severity = match self.severity.as_str() {
            "critical" => "10",
            "high" => "8",
            "medium" => "5",
            "low" => "3",
            _ => "5",
        };

        let mut extensions = String::new();
        if !self.mitre.is_empty() {
            extensions.push_str(&format!("cat={}", cef_escape(&self.mitre)));
        }
        for (key, val) in &self.fields {
            if !extensions.is_empty() {
                extensions.push(' ');
            }
            let val_str = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            extensions.push_str(&format!("{}={}", cef_escape(key), cef_escape(&val_str)));
        }

        format!(
            "CEF:0|Varpulis|KillChain|1.0|{}|{}|{}|{}",
            cef_escape(&self.rule),
            cef_escape(&self.summary),
            cef_severity,
            extensions
        )
    }
}

fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Int(n) => serde_json::Value::Number((*n).into()),
        Value::Float(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Str(s) => serde_json::Value::String(s.to_string()),
        _ => serde_json::Value::String(val.to_string()),
    }
}

/// Escape special CEF characters.
fn cef_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('=', "\\=")
        .replace('\n', "\\n")
}

/// Recursively collect .vpl files from a directory.
pub fn collect_vpl_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_vpl_files(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "vpl") {
            files.push(path);
        }
    }
    Ok(())
}

/// Run a single VPL rule file against events, collecting alerts.
async fn run_rule_file(
    rule_path: &Path,
    events: &[Event],
    credentials_store: &Option<Arc<CredentialsStore>>,
    verbose: bool,
) -> Result<Vec<SecurityAlert>> {
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

    // Collect output events
    let collected: Arc<tokio::sync::Mutex<Vec<SharedEvent>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let collected_clone = collected.clone();
    let rule_name = rule_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let collector = tokio::spawn(async move {
        while let Some(event) = output_rx.recv().await {
            if verbose {
                eprintln!(
                    "  ALERT [{}]: {} - {}",
                    rule_name,
                    event.event_type,
                    event
                        .get("summary")
                        .map(|v| match v {
                            Value::Str(s) => s.to_string(),
                            other => other.to_string(),
                        })
                        .unwrap_or_default()
                );
            }
            collected_clone.lock().await.push(event);
        }
    });

    // Process events
    let events_owned: Vec<Event> = events.to_vec();
    let use_sync = !engine.has_sink_operations();

    if use_sync {
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
    let alerts: Vec<SecurityAlert> = events_vec
        .iter()
        .map(|e| SecurityAlert::from_event(e))
        .collect();

    Ok(alerts)
}

/// Run the detect (blue mode) command.
pub async fn run_detect(
    rules_path: &Path,
    events_path: &Path,
    format: OutputFormat,
    workers: Option<usize>,
    credentials_store: Option<Arc<CredentialsStore>>,
    verbose: bool,
) -> Result<()> {
    let num_workers = workers.unwrap_or(1);

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build_global()
        .ok();

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

    output::header("Varpulis Kill Chain Detection");
    println!(
        "Rules:   {} file(s) from {}",
        rule_files.len(),
        rules_path.display()
    );
    println!("Events:  {}", events_path.display());
    println!("Workers: {num_workers}");
    println!();

    // Load events once (shared across all rule files)
    let events_source = std::fs::read_to_string(events_path)?;
    let timed_events = EventFileParser::parse(&events_source)
        .map_err(|e| anyhow::anyhow!("Event file error: {e}"))?;
    let total_events = timed_events.len();
    let events: Vec<Event> = timed_events.into_iter().map(|te| te.event).collect();

    let start = std::time::Instant::now();

    // Run each rule file independently (avoids stream name conflicts)
    let mut all_alerts: Vec<SecurityAlert> = Vec::new();
    let mut rules_loaded = 0u64;
    let mut rules_failed = 0u64;

    let spinner = output::create_spinner(&format!(
        "Running {} rule files against {total_events} events...",
        rule_files.len()
    ));

    for rule_path in &rule_files {
        info!("Loading rule: {}", rule_path.display());
        match run_rule_file(rule_path, &events, &credentials_store, verbose).await {
            Ok(alerts) => {
                rules_loaded += 1;
                all_alerts.extend(alerts);
            }
            Err(e) => {
                rules_failed += 1;
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
    let alert_count = all_alerts.len();
    let rate = if duration.as_secs_f64() > 0.0 {
        (total_events as f64 * rules_loaded as f64) / duration.as_secs_f64()
    } else {
        0.0
    };

    // Render output
    match format {
        OutputFormat::Json => {
            for alert in &all_alerts {
                println!(
                    "{}",
                    serde_json::to_string(alert).unwrap_or_else(|_| "{}".to_string())
                );
            }
        }
        OutputFormat::Cef => {
            for alert in &all_alerts {
                println!("{}", alert.to_cef());
            }
        }
        OutputFormat::Markdown => {
            println!("| Rule | MITRE | Severity | Summary |");
            println!("|------|-------|----------|---------|");
            for alert in &all_alerts {
                println!(
                    "| {} | {} | {} | {} |",
                    alert.rule, alert.mitre, alert.severity, alert.summary
                );
            }
        }
        OutputFormat::Table => {
            if rules_failed > 0 {
                eprintln!(
                    "{}",
                    format!("{rules_loaded} rules loaded, {rules_failed} skipped").yellow()
                );
                eprintln!();
            }

            if all_alerts.is_empty() {
                println!("{}", "No alerts detected.".dimmed());
            } else {
                let mut table = Table::new();
                table.load_preset(UTF8_FULL);
                table.set_header(vec![
                    Cell::new("Rule"),
                    Cell::new("MITRE"),
                    Cell::new("Severity"),
                    Cell::new("Summary"),
                ]);

                for alert in &all_alerts {
                    let severity_color = match alert.severity.as_str() {
                        "critical" => Color::Red,
                        "high" => Color::DarkRed,
                        "medium" => Color::Yellow,
                        "low" => Color::Cyan,
                        _ => Color::White,
                    };

                    table.add_row(vec![
                        Cell::new(&alert.rule),
                        Cell::new(&alert.mitre),
                        Cell::new(&alert.severity).fg(severity_color),
                        Cell::new(&alert.summary),
                    ]);
                }

                println!("{table}");
            }

            // Summary line
            println!();
            let status = if alert_count > 0 {
                format!("{alert_count} alert(s) detected")
                    .red()
                    .bold()
                    .to_string()
            } else {
                "Clean - no threats detected".green().bold().to_string()
            };
            println!(
                "{status} | {total_events} events x {rules_loaded} rules in {:.1}s ({:.0} evt/s)",
                duration.as_secs_f64(),
                rate
            );
        }
    }

    Ok(())
}
