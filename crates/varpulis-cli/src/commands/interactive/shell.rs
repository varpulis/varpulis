//! Human-friendly interactive shell (Python-interpreter style).
//!
//! Type VPL declarations directly — event types, streams, connectors — and they
//! accumulate. Type an event literal to inject it. No `:load` required.
//!
//! ```text
//! $ varpulis interactive
//! vpl> event Sensor:
//! ...>     temp: float
//! ...>     zone: str
//! vpl>
//! vpl> stream Hot = Sensor .where(temp > 100) .emit(temperature: temp, zone: zone)
//! ✓ 1 stream loaded: Hot
//!
//! vpl> Sensor { temp: 150, zone: "A" }
//! → Hot: { temperature: 150, zone: "A" }
//!
//! vpl> Sensor { temp: 50, zone: "B" }
//! (no output — filtered)
//! ```

use std::io::{self, BufRead, Write};

use varpulis_runtime::interactive::{InteractiveSession, SessionCommand, SessionResponse};

use crate::output;

/// VPL keywords that start a multi-line block declaration.
const VPL_KEYWORDS: &[&str] = &[
    "event",
    "stream",
    "connector",
    "fn",
    "let",
    "var",
    "const",
    "config",
    "context",
    "pattern",
    "import",
];

/// Run the human-friendly interactive shell.
pub fn run_shell(
    file: Option<&std::path::Path>,
    generate: Option<&str>,
    rate: u64,
    trace: bool,
) -> anyhow::Result<()> {
    let mut session = InteractiveSession::new();

    // Auto-load file if provided
    if let Some(path) = file {
        let vpl = std::fs::read_to_string(path)?;
        let responses = session.handle_command(SessionCommand::LoadVpl { vpl });
        print_responses(&responses);
    }

    // Auto-start generator
    if let Some(schema) = generate {
        let responses = session.handle_command(SessionCommand::Generate {
            schema: schema.to_string(),
            rate,
            duration: None,
        });
        print_responses(&responses);
    }

    // Enable trace
    if trace {
        session.handle_command(SessionCommand::SetTrace { enabled: true });
    }

    output::success("Varpulis Interactive Shell v0.9.0");
    println!("Type VPL declarations (event, stream, ...) or event literals to inject.");
    println!("Type :help for commands, :quit to exit.\n");

    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut multiline_buffer = String::new();
    let mut in_multiline = false;

    loop {
        // Prompt
        if in_multiline {
            print!("...> ");
        } else {
            print!("vpl> ");
        }
        io::stdout().flush()?;

        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break; // EOF
        }

        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');

        // Multi-line accumulation: blank line finishes the block
        if in_multiline {
            if trimmed.is_empty() {
                // End of multi-line block — submit the accumulated VPL
                in_multiline = false;
                let vpl = std::mem::take(&mut multiline_buffer);
                let responses = session.handle_command(SessionCommand::AppendVpl { vpl });
                print_responses(&responses);
                continue;
            }
            multiline_buffer.push('\n');
            multiline_buffer.push_str(trimmed);
            continue;
        }

        // Empty line
        if trimmed.is_empty() {
            continue;
        }

        // Commands starting with ':'
        if trimmed.starts_with(':') {
            handle_command(trimmed, &mut session);
            if trimmed == ":quit" || trimmed == ":q" {
                break;
            }
            continue;
        }

        // Check if this starts a VPL declaration (multi-line)
        let first_word = trimmed.split_whitespace().next().unwrap_or("");
        if VPL_KEYWORDS.contains(&first_word) {
            // Check if the line ends with ':' (block header like `event Foo:`)
            // or is a complete single-line declaration
            if trimmed.ends_with(':') {
                // Start multi-line accumulation
                in_multiline = true;
                multiline_buffer = trimmed.to_string();
                continue;
            }
            // Single-line VPL declaration (e.g., `stream S = Foo .where(x > 10)`)
            let responses = session.handle_command(SessionCommand::AppendVpl {
                vpl: trimmed.to_string(),
            });
            print_responses(&responses);
            continue;
        }

        // Check if it looks like an event literal: `EventType { field: value }`
        if trimmed.contains('{') && trimmed.contains('}') {
            inject_event_literal(trimmed, &mut session);
            continue;
        }

        // Unknown input — try as VPL anyway
        let responses = session.handle_command(SessionCommand::AppendVpl {
            vpl: trimmed.to_string(),
        });
        print_responses(&responses);
    }

    Ok(())
}

/// Handle `:command` inputs.
fn handle_command(input: &str, session: &mut InteractiveSession) {
    let (cmd, arg) = input
        .split_once(' ')
        .map(|(c, a)| (c, a.trim()))
        .unwrap_or((input, ""));

    match cmd {
        ":quit" | ":q" => {
            output::success("Bye!");
        }
        ":help" | ":h" => {
            println!("Commands:");
            println!("  :help          Show this help");
            println!("  :load <file>   Load a VPL file (replaces current program)");
            println!("  :save <file>   Save current session as a .vpl file");
            println!("  :source        Show accumulated VPL source");
            println!("  :streams       List loaded streams");
            println!("  :topology      Show pipeline graph");
            println!("  :metrics       Show engine metrics");
            println!("  :trace on/off  Toggle trace mode");
            println!("  :gen <schema>  Start datagen (fraud, iot, trading)");
            println!("  :stop          Stop datagen");
            println!("  :reset         Clear all definitions and state");
            println!("  :quit          Exit");
            println!();
            println!("Or just type VPL declarations and event literals directly.");
        }
        ":load" | ":l" => {
            if arg.is_empty() {
                output::error("Usage: :load <file.vpl>");
                return;
            }
            let responses = session.handle_command(SessionCommand::LoadFile {
                path: arg.to_string(),
            });
            print_responses(&responses);
        }
        ":streams" => {
            let responses = session.handle_command(SessionCommand::GetStreams);
            print_responses(&responses);
        }
        ":topology" | ":topo" => {
            let responses = session.handle_command(SessionCommand::GetTopology);
            print_responses(&responses);
        }
        ":metrics" => {
            let responses = session.handle_command(SessionCommand::GetMetrics);
            print_responses(&responses);
        }
        ":trace" => match arg {
            "on" | "true" | "1" => {
                session.handle_command(SessionCommand::SetTrace { enabled: true });
                output::success("Trace enabled");
            }
            "off" | "false" | "0" => {
                session.handle_command(SessionCommand::SetTrace { enabled: false });
                output::success("Trace disabled");
            }
            _ => {
                output::error("Usage: :trace on|off");
            }
        },
        ":gen" | ":generate" => {
            if arg.is_empty() {
                output::error("Usage: :gen fraud|iot|trading");
                return;
            }
            let responses = session.handle_command(SessionCommand::Generate {
                schema: arg.to_string(),
                rate: 1000,
                duration: None,
            });
            print_responses(&responses);
        }
        ":stop" => {
            let responses = session.handle_command(SessionCommand::StopGenerate);
            print_responses(&responses);
        }
        ":save" | ":s" => {
            if arg.is_empty() {
                output::error("Usage: :save <file.vpl>");
                return;
            }
            let responses = session.handle_command(SessionCommand::Save {
                path: arg.to_string(),
            });
            print_responses(&responses);
        }
        ":source" | ":src" => {
            let responses = session.handle_command(SessionCommand::GetSource);
            print_responses(&responses);
        }
        ":reset" => {
            *session = InteractiveSession::new();
            output::success("Session reset — all definitions and state cleared");
        }
        _ => {
            output::error(&format!("Unknown command: {cmd} (type :help for commands)"));
        }
    }
}

/// Parse an event literal like `Sensor { temp: 150, zone: "A" }` and inject it.
fn inject_event_literal(input: &str, session: &mut InteractiveSession) {
    // Split into event_type and fields
    let brace_pos = match input.find('{') {
        Some(p) => p,
        None => {
            output::error("Invalid event literal (expected `EventType { field: value }`)");
            return;
        }
    };

    let event_type = input[..brace_pos].trim();
    let fields_str = input[brace_pos..].trim();

    // Build a JSON object from the .evt-style fields
    // Quick approach: wrap in a JSON-parseable format
    // Convert `{ field: value, field2: "str" }` to JSON
    let json_str = evt_fields_to_json(fields_str);

    let data = match serde_json::from_str::<serde_json::Value>(&json_str) {
        Ok(v) => v,
        Err(_) => {
            // Fallback: try to parse the original as-is
            match serde_json::from_str::<serde_json::Value>(fields_str) {
                Ok(v) => v,
                Err(e) => {
                    output::error(&format!("Cannot parse event fields: {e}"));
                    return;
                }
            }
        }
    };

    let responses = session.handle_command(SessionCommand::Inject {
        event_type: event_type.to_string(),
        data,
    });
    print_responses(&responses);
}

/// Convert .evt-style fields `{ field: value, field2: "str" }` to JSON.
fn evt_fields_to_json(input: &str) -> String {
    let mut result = String::with_capacity(input.len() + 32);
    let mut in_string = false;
    let mut prev_char = '\0';
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        if in_string {
            result.push(ch);
            if ch == '"' && prev_char != '\\' {
                in_string = false;
            }
            prev_char = ch;
            i += 1;
            continue;
        }

        if ch == '"' {
            in_string = true;
            result.push(ch);
            prev_char = ch;
            i += 1;
            continue;
        }

        // Convert unquoted keys: `field:` → `"field":`
        if ch == ':' && !result.ends_with('"') {
            // Find the start of the key (go back to last separator)
            let key_start = result
                .rfind(|c: char| c == '{' || c == ',')
                .map(|p| p + 1)
                .unwrap_or(0);
            let key = result[key_start..].trim().to_string();
            result.truncate(key_start);
            result.push_str(&format!(" \"{key}\""));
            result.push(':');
        } else if ch.is_alphabetic() && !in_string {
            // Check for bare keywords: true, false, null
            let remaining: String = chars[i..].iter().collect();
            if remaining.starts_with("true")
                && !chars.get(i + 4).is_some_and(|c| c.is_alphanumeric())
            {
                result.push_str("true");
                i += 4;
                prev_char = 'e';
                continue;
            } else if remaining.starts_with("false")
                && !chars.get(i + 5).is_some_and(|c| c.is_alphanumeric())
            {
                result.push_str("false");
                i += 5;
                prev_char = 'e';
                continue;
            } else if remaining.starts_with("null")
                && !chars.get(i + 4).is_some_and(|c| c.is_alphanumeric())
            {
                result.push_str("null");
                i += 4;
                prev_char = 'l';
                continue;
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }

        prev_char = ch;
        i += 1;
    }

    result
}

/// Print session responses in human-friendly format.
fn print_responses(responses: &[SessionResponse]) {
    use owo_colors::OwoColorize;

    for resp in responses {
        match resp {
            SessionResponse::Loaded { streams, added, .. } => {
                if output::color_enabled() {
                    print!("{}", "✓ ".green());
                } else {
                    print!("OK ");
                }
                println!("{} stream(s) loaded: {}", streams.len(), streams.join(", "));
                if !added.is_empty() {
                    println!("  added: {}", added.join(", "));
                }
            }
            SessionResponse::Output { stream, event, .. } => {
                if output::color_enabled() {
                    print!("{}", "→ ".green().bold());
                    print!("{}", stream.cyan());
                } else {
                    print!("-> {stream}");
                }
                println!(": {event}");
            }
            SessionResponse::Trace { entries } => {
                for entry in entries {
                    if output::color_enabled() {
                        println!(
                            "  {} {}{}",
                            "trace:".dimmed(),
                            entry.kind.dimmed(),
                            entry
                                .detail
                                .as_ref()
                                .map(|d| format!(" — {d}"))
                                .unwrap_or_default()
                                .dimmed()
                        );
                    } else {
                        println!(
                            "  trace: {}{}",
                            entry.kind,
                            entry
                                .detail
                                .as_ref()
                                .map(|d| format!(" — {d}"))
                                .unwrap_or_default()
                        );
                    }
                }
            }
            SessionResponse::Streams { streams } => {
                if streams.is_empty() {
                    println!("(no streams defined)");
                } else {
                    for s in streams {
                        println!("  {} (source: {}, {} ops)", s.name, s.source, s.ops_count);
                    }
                }
            }
            SessionResponse::Topology { nodes, edges } => {
                println!("Pipeline ({} nodes, {} edges):", nodes.len(), edges.len());
                for node in nodes {
                    println!("  [{}] {}", node.node_type, node.label);
                }
            }
            SessionResponse::Metrics {
                events_processed,
                output_events,
                streams_count,
            } => {
                println!(
                    "Events: {events_processed} processed, {output_events} output, {streams_count} streams"
                );
            }
            SessionResponse::Error { message } => {
                output::error(message);
            }
            SessionResponse::GeneratorStatus {
                running, generated, ..
            } => {
                if *running {
                    println!("Generator running ({generated} events generated)");
                } else {
                    println!("Generator stopped ({generated} events generated)");
                }
            }
            SessionResponse::Saved { path, lines } => {
                output::success(&format!("Saved {lines} lines to {path}"));
            }
            SessionResponse::Source { vpl } => {
                println!("{vpl}");
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evt_fields_to_json_simple() {
        let result = evt_fields_to_json("{ temp: 150, zone: \"A\" }");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["temp"], 150);
        assert_eq!(parsed["zone"], "A");
    }

    #[test]
    fn test_evt_fields_to_json_booleans() {
        let result = evt_fields_to_json("{ active: true, deleted: false }");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["active"], true);
        assert_eq!(parsed["deleted"], false);
    }

    #[test]
    fn test_evt_fields_to_json_null() {
        let result = evt_fields_to_json("{ value: null }");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["value"].is_null());
    }

    #[test]
    fn test_vpl_keywords_detected() {
        assert!(VPL_KEYWORDS.contains(&"event"));
        assert!(VPL_KEYWORDS.contains(&"stream"));
        assert!(VPL_KEYWORDS.contains(&"connector"));
        assert!(!VPL_KEYWORDS.contains(&"Sensor"));
    }
}
