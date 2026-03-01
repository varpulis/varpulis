use anyhow::Result;

/// Parse a duration string like "60s", "5m", "1h" into seconds.
fn parse_duration_str(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }
    let (num_part, suffix) = if let Some(stripped) = s.strip_suffix('s') {
        (stripped, "s")
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, "m")
    } else if let Some(stripped) = s.strip_suffix('h') {
        (stripped, "h")
    } else {
        // Assume seconds if no suffix
        (s, "s")
    };
    let value: u64 = num_part
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid duration number: '{num_part}'"))?;
    match suffix {
        "s" => Ok(value),
        "m" => Ok(value * 60),
        "h" => Ok(value * 3600),
        _ => anyhow::bail!("Unknown duration suffix: '{suffix}'"),
    }
}

pub async fn run_generate(
    schema: varpulis_datagen::SchemaType,
    rate: u64,
    duration: &str,
    anomaly_rate: f64,
    seed: Option<u64>,
    format: &str,
) -> Result<()> {
    use std::io::Write;

    if !(0.0..=1.0).contains(&anomaly_rate) {
        anyhow::bail!("--anomaly-rate must be between 0.0 and 1.0, got {anomaly_rate}");
    }
    if rate == 0 {
        anyhow::bail!("--rate must be greater than 0");
    }
    let pretty = match format.to_lowercase().as_str() {
        "json" => true,
        "jsonl" => false,
        other => anyhow::bail!("Unknown format '{other}'. Use 'json' or 'jsonl'"),
    };

    let duration_secs = parse_duration_str(duration)?;

    let _config = varpulis_datagen::GeneratorConfig {
        schema,
        rate,
        duration_secs,
        anomaly_rate,
        seed,
    };

    let mut event_schema = varpulis_datagen::create_schema(schema, seed);

    eprintln!(
        "Generating {} events/s for {}s ({} schema, anomaly_rate={}, seed={})...",
        rate,
        duration_secs,
        schema,
        anomaly_rate,
        seed.map_or("random".to_string(), |s| s.to_string()),
    );

    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    let interval_duration = tokio::time::Duration::from_secs_f64(1.0 / rate as f64);
    let mut interval = tokio::time::interval(interval_duration);
    // Don't try to catch up if we fall behind — just skip missed ticks
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(duration_secs);
    let mut count: u64 = 0;

    loop {
        interval.tick().await;
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        let event = event_schema.next_event();
        let result = if pretty {
            serde_json::to_string_pretty(&event)
        } else {
            serde_json::to_string(&event)
        };
        match result {
            Ok(json) => {
                if writeln!(writer, "{json}").is_err() {
                    // Broken pipe (e.g., piped to head) — exit cleanly
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to serialize event: {e}");
            }
        }
        count += 1;
    }

    // Flush any buffered output
    let _ = writer.flush();
    eprintln!("Generated {count} events.");

    Ok(())
}
