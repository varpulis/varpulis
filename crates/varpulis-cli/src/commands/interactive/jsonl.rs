//! JSON-line transport for the interactive session.
//!
//! Reads [`SessionCommand`] from stdin (one JSON object per line) and writes
//! [`SessionResponse`] to stdout (one JSON object per line). Designed for
//! programmatic drivers (agents, MCP servers, test harnesses).

use tokio::io::{AsyncBufReadExt, BufReader};
use varpulis_runtime::interactive::{InteractiveSession, SessionCommand, SessionResponse};

/// Run the JSON-line interactive session.
///
/// Reads commands from stdin, writes responses to stdout.
/// If `file` is provided, the VPL program is auto-loaded on startup.
/// If `generate` is provided, the event generator is started with the given schema.
/// If `trace` is true, pipeline tracing is enabled.
pub async fn run_jsonl_session(
    file: Option<&std::path::Path>,
    generate: Option<&str>,
    rate: u64,
    trace: bool,
) -> anyhow::Result<()> {
    let mut session = InteractiveSession::new();
    let mut response_rx = session.subscribe();

    // Auto-load VPL file if provided
    if let Some(path) = file {
        let vpl = std::fs::read_to_string(path)?;
        let responses = session.handle_command(SessionCommand::LoadVpl { vpl });
        for resp in &responses {
            write_response(resp);
        }
    }

    // Auto-start generator if provided
    if let Some(schema) = generate {
        let responses = session.handle_command(SessionCommand::Generate {
            schema: schema.to_string(),
            rate,
            duration: None,
        });
        for resp in &responses {
            write_response(resp);
        }
    }

    // Enable trace if requested
    if trace {
        session.handle_command(SessionCommand::SetTrace { enabled: true });
    }

    // Send ready message
    write_response(&SessionResponse::Ready {
        version: env!("CARGO_PKG_VERSION").to_string(),
    });

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    // Generator poll interval (10ms)
    let mut generator_interval = tokio::time::interval(std::time::Duration::from_millis(10));
    generator_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line? {
                    Some(line) if line.trim().is_empty() => continue,
                    Some(line) => {
                        match serde_json::from_str::<SessionCommand>(&line) {
                            Ok(SessionCommand::Quit) => {
                                write_response(&SessionResponse::Bye);
                                break;
                            }
                            Ok(cmd) => {
                                let responses = session.handle_command(cmd);
                                for resp in &responses {
                                    write_response(resp);
                                }
                            }
                            Err(e) => {
                                write_response(&SessionResponse::Error {
                                    message: format!("invalid command: {e}"),
                                });
                            }
                        }
                    }
                    None => break, // EOF
                }
            }
            resp = response_rx.recv() => {
                if let Ok(resp) = resp {
                    write_response(&resp);
                }
            }
            _ = generator_interval.tick() => {
                let responses = session.poll_generator();
                for resp in &responses {
                    write_response(resp);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                write_response(&SessionResponse::Bye);
                break;
            }
        }
    }

    Ok(())
}

/// Serialize a response to a single JSON line on stdout.
fn write_response(resp: &SessionResponse) {
    if let Ok(json) = serde_json::to_string(resp) {
        println!("{json}");
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_response_produces_valid_json() {
        // We cannot capture stdout easily, but we can verify that serialization works
        // for every SessionResponse variant.
        let responses: Vec<SessionResponse> = vec![
            SessionResponse::Ready {
                version: "0.8.0".into(),
            },
            SessionResponse::Loaded {
                streams: vec!["A".into()],
                added: vec!["A".into()],
                removed: vec![],
                preserved: vec![],
            },
            SessionResponse::Output {
                stream: "Alerts".into(),
                event: serde_json::json!({"temp": 100}),
                timestamp: "2026-01-01T00:00:00Z".into(),
            },
            SessionResponse::Metrics {
                events_processed: 42,
                output_events: 10,
                streams_count: 2,
            },
            SessionResponse::Error {
                message: "test error".into(),
            },
            SessionResponse::GeneratorStatus {
                running: true,
                generated: 500,
            },
            SessionResponse::Bye,
        ];

        for resp in &responses {
            let json = serde_json::to_string(resp).expect("serialization should succeed");
            // Verify it round-trips as valid JSON
            let parsed: serde_json::Value =
                serde_json::from_str(&json).expect("should be valid JSON");
            // Every response must have a "type" field
            assert!(
                parsed.get("type").is_some(),
                "response should have 'type' field: {json}"
            );
        }
    }

    #[test]
    fn test_write_response_bye_is_compact() {
        let resp = SessionResponse::Bye;
        let json = serde_json::to_string(&resp).unwrap();
        // Should be a single line with no pretty-printing
        assert!(!json.contains('\n'));
        assert!(json.contains(r#""type":"bye""#));
    }

    #[test]
    fn test_write_response_ready_contains_version() {
        let resp = SessionResponse::Ready {
            version: "0.8.0".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "ready");
        assert_eq!(parsed["version"], "0.8.0");
    }

    #[test]
    fn test_deserialize_invalid_command() {
        let invalid = r#"{"not":"a command"}"#;
        let result = serde_json::from_str::<SessionCommand>(invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_quit_command() {
        let json = r#"{"cmd":"quit"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::Quit));
    }
}
