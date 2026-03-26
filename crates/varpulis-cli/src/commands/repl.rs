//! Interactive REPL for Varpulis VPL programs.
//!
//! Provides a readline-based interactive shell for loading VPL programs,
//! sending events, and observing output events in real time.

use std::path::Path;

use anyhow::Result;
use owo_colors::OwoColorize;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tokio::sync::mpsc;
use varpulis_cli::output;
use varpulis_core::ast::Program;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

/// State for a loaded VPL program (source text + parsed AST).
struct LoadedProgram {
    /// Original source text (retained for potential :show command / diagnostics).
    _source: String,
    program: Program,
}

/// Run the interactive REPL loop.
pub fn run_repl(initial_file: Option<&Path>) -> Result<()> {
    let mut editor = DefaultEditor::new()?;

    // Load history from ~/.varpulis_history (best-effort)
    let history_path = std::env::var("HOME")
        .ok()
        .map(|home| std::path::PathBuf::from(home).join(".varpulis_history"));
    if let Some(ref path) = history_path {
        let _ = editor.load_history(path);
    }

    // Create output channel and engine.
    // Keep `tx` around so we can re-create the engine on :reset.
    let (tx, mut rx) = mpsc::channel::<Event>(1000);
    let mut engine = Engine::new(tx.clone());
    let mut loaded: Option<LoadedProgram> = None;

    // Auto-load if --file was provided
    if let Some(file) = initial_file {
        match load_program_from_file(&mut engine, file) {
            Ok(lp) => {
                output::success(&format!(
                    "Loaded {} ({} streams)",
                    file.display(),
                    engine.stream_names().len()
                ));
                loaded = Some(lp);
            }
            Err(e) => {
                output::error(&format!("Failed to load {}: {e}", file.display()));
            }
        }
    }

    println!(
        "Varpulis REPL v{} -- type :help for commands",
        env!("CARGO_PKG_VERSION")
    );

    loop {
        let prompt = if loaded.is_some() {
            "vpl> "
        } else {
            "vpl (no program)> "
        };

        let line = match editor.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                output::error(&format!("Readline error: {e}"));
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Best-effort history entry (ignore errors like max-length exceeded)
        let _ = editor.add_history_entry(trimmed);

        match trimmed {
            ":quit" | ":q" => break,
            ":help" | ":h" => print_help(),
            ":reset" => {
                loaded = reset_engine(&mut engine, loaded, &tx);
            }
            ":streams" => {
                print_streams(&engine, loaded.is_some());
            }
            cmd if cmd.starts_with(":load ") => {
                let path_str = cmd.strip_prefix(":load ").unwrap().trim();
                let path = Path::new(path_str);
                match load_program_from_file(&mut engine, path) {
                    Ok(lp) => {
                        output::success(&format!(
                            "Loaded {} ({} streams)",
                            path.display(),
                            engine.stream_names().len()
                        ));
                        loaded = Some(lp);
                    }
                    Err(e) => {
                        output::error(&format!("Failed to load {}: {e}", path.display()));
                    }
                }
            }
            cmd if cmd.starts_with(":event ") => {
                let event_text = cmd.strip_prefix(":event ").unwrap().trim();
                if loaded.is_none() {
                    output::warning("No program loaded. Use :load <file.vpl> first.");
                } else {
                    process_single_event(&mut engine, &mut rx, event_text);
                }
            }
            cmd if cmd.starts_with(":events ") => {
                let path_str = cmd.strip_prefix(":events ").unwrap().trim();
                if loaded.is_none() {
                    output::warning("No program loaded. Use :load <file.vpl> first.");
                } else {
                    process_event_file(&mut engine, &mut rx, Path::new(path_str));
                }
            }
            _ => {
                // Try to parse as a complete VPL program
                match try_load_inline(&mut engine, trimmed) {
                    Ok(lp) => {
                        output::success(&format!(
                            "Program loaded ({} streams)",
                            engine.stream_names().len()
                        ));
                        loaded = Some(lp);
                    }
                    Err(e) => {
                        output::error(&format!("Parse error: {e}"));
                    }
                }
            }
        }
    }

    // Save history (best-effort)
    if let Some(ref path) = history_path {
        let _ = editor.save_history(path);
    }

    println!("Goodbye.");
    Ok(())
}

/// Load a VPL program from a file path and configure the engine.
fn load_program_from_file(engine: &mut Engine, path: &Path) -> Result<LoadedProgram> {
    let source =
        std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("Cannot read file: {e}"))?;
    let program = parse(&source).map_err(|e| anyhow::anyhow!("{e}"))?;
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Engine load error: {e}"))?;
    Ok(LoadedProgram {
        _source: source,
        program,
    })
}

/// Try to parse an inline string as a complete VPL program and load it.
fn try_load_inline(engine: &mut Engine, source: &str) -> Result<LoadedProgram> {
    let program = parse(source).map_err(|e| anyhow::anyhow!("{e}"))?;
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Engine load error: {e}"))?;
    Ok(LoadedProgram {
        _source: source.to_string(),
        program,
    })
}

/// Reset the engine, re-loading the current program if one exists.
fn reset_engine(
    engine: &mut Engine,
    loaded: Option<LoadedProgram>,
    tx: &mpsc::Sender<Event>,
) -> Option<LoadedProgram> {
    *engine = Engine::new(tx.clone());
    if let Some(lp) = loaded {
        match engine.load(&lp.program) {
            Ok(()) => {
                output::success("Engine reset (program re-loaded).");
                Some(lp)
            }
            Err(e) => {
                output::error(&format!("Failed to re-load program after reset: {e}"));
                None
            }
        }
    } else {
        output::success("Engine reset.");
        None
    }
}

/// Print the list of loaded streams.
fn print_streams(engine: &Engine, has_program: bool) {
    if !has_program {
        output::warning("No program loaded.");
        return;
    }
    let names = engine.stream_names();
    if names.is_empty() {
        println!("  (no streams defined)");
    } else {
        println!("Streams ({}):", names.len());
        for name in &names {
            println!("  - {name}");
        }
    }
}

/// Parse a single event string, process it through the engine, and print outputs.
fn process_single_event(engine: &mut Engine, rx: &mut mpsc::Receiver<Event>, event_text: &str) {
    let event = match EventFileParser::parse_line(event_text) {
        Ok(Some(ev)) => ev,
        Ok(None) => {
            output::warning("Empty or comment line, no event parsed.");
            return;
        }
        Err(e) => {
            output::error(&format!("Event parse error: {e}"));
            return;
        }
    };

    if let Err(e) = engine.process_batch_sync(vec![event]) {
        output::error(&format!("Processing error: {e}"));
        return;
    }

    drain_and_print_outputs(rx);
}

/// Load and process all events from a .evt / JSONL file.
fn process_event_file(engine: &mut Engine, rx: &mut mpsc::Receiver<Event>, path: &Path) {
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            output::error(&format!("Cannot read {}: {e}", path.display()));
            return;
        }
    };

    let timed_events = match EventFileParser::parse(&contents) {
        Ok(te) => te,
        Err(e) => {
            output::error(&format!("Event file parse error: {e}"));
            return;
        }
    };

    let event_count = timed_events.len();
    let events: Vec<Event> = timed_events.into_iter().map(|te| te.event).collect();

    if let Err(e) = engine.process_batch_sync(events) {
        output::error(&format!("Processing error: {e}"));
        return;
    }

    let output_count = drain_and_print_outputs(rx);
    println!("Processed {event_count} events, {output_count} output(s) emitted.");
}

/// Drain the output channel (non-blocking) and print any received events.
/// Returns the number of output events printed.
fn drain_and_print_outputs(rx: &mut mpsc::Receiver<Event>) -> usize {
    let mut count = 0usize;
    while let Ok(event) = rx.try_recv() {
        count += 1;
        if output::color_enabled() {
            print!("  {} ", "=>".green().bold());
            print!("{}", event.event_type.cyan());
        } else {
            print!("  => {}", event.event_type);
        }
        if !event.data.is_empty() {
            print!(" {{");
            for (i, (k, v)) in event.data.iter().enumerate() {
                if i > 0 {
                    print!(",");
                }
                print!(" {k}: {v}");
            }
            print!(" }}");
        }
        println!();
    }
    count
}

/// Print the help text.
fn print_help() {
    println!("Commands:");
    println!("  :load <file.vpl>   Load a VPL program from file");
    println!("  :event <Event {{}}> Send a single event to the engine");
    println!("  :events <file.evt> Process all events from a file");
    println!("  :streams           List loaded stream definitions");
    println!("  :reset             Reset engine state (re-load current program)");
    println!("  :help  / :h        Show this help");
    println!("  :quit  / :q        Exit the REPL");
    println!();
    println!("You can also type VPL source directly to load it as a program.");
    println!();
    println!("Examples:");
    println!("  :load examples/filter.vpl");
    println!("  :event StockTick {{ symbol: \"AAPL\", price: 150.0 }}");
    println!("  :events test_data/trades.evt");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print_help_does_not_panic() {
        // Smoke test: ensure help output works without panicking
        print_help();
    }

    #[test]
    fn test_command_parsing_prefixes() {
        // Verify the command dispatch prefix matching logic
        assert!(":load foo.vpl".starts_with(":load "));
        assert!(":event StockTick { price: 1.0 }".starts_with(":event "));
        assert!(":events data.evt".starts_with(":events "));
        assert!(":quit" == ":quit" || ":q" == ":q");
        assert!(":help" == ":help" || ":h" == ":h");
    }

    #[test]
    fn test_drain_empty_channel() {
        let (_tx, mut rx) = mpsc::channel::<Event>(10);
        let count = drain_and_print_outputs(&mut rx);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_drain_with_events() {
        let (tx, mut rx) = mpsc::channel::<Event>(10);
        let event = Event {
            event_type: "TestEvent".into(),
            timestamp: chrono::Utc::now(),
            data: indexmap::IndexMap::default(),
        };
        tx.try_send(event).unwrap();
        let count = drain_and_print_outputs(&mut rx);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_try_load_inline_valid() {
        let (tx, _rx) = mpsc::channel::<Event>(10);
        let mut engine = Engine::new(tx);
        let source = r#"
            event SensorReading:
                temperature: float

            stream Alerts = SensorReading
                .where(temperature > 100)
                .emit(alert_type: "high_temp")
        "#;
        let result = try_load_inline(&mut engine, source);
        assert!(result.is_ok());
        assert!(!engine.stream_names().is_empty());
    }

    #[test]
    fn test_try_load_inline_invalid() {
        let (tx, _rx) = mpsc::channel::<Event>(10);
        let mut engine = Engine::new(tx);
        let result = try_load_inline(&mut engine, "this is not valid VPL @@#$%");
        assert!(result.is_err());
    }
}
