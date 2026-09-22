//! `varpulis` — the engine's command line.
//!
//! Three things, all offline: check a program, print its syntax tree, run it
//! over an `.evt` file and print what it emits. Running a program on a bus
//! is a host's job; the reference host is Vejas, where a program is a
//! `detect` unit.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use varpulis_engine::Program;
use varpulis_runtime::event_file::EventFileParser;

#[derive(Parser)]
#[command(
    name = "varpulis",
    version,
    about = "The Varpulis CEP engine: check, parse and simulate VPL programs"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check a VPL program: parse it, validate it, load it into the engine
    Check {
        /// Path to the .vpl file
        file: PathBuf,
    },
    /// Print a VPL program's syntax tree
    Parse {
        /// Path to the .vpl file
        file: PathBuf,
    },
    /// Run a VPL program over an event file and print what it emits, one
    /// JSON object per line
    Simulate {
        /// Path to the VPL program (.vpl)
        #[arg(short, long)]
        program: PathBuf,
        /// Path to the event file (.evt)
        #[arg(short, long)]
        events: PathBuf,
        /// Print every input event on stderr as it is fed
        #[arg(short, long)]
        verbose: bool,
        /// Accepted for older command lines; the engine runs on one thread
        #[arg(short = 'w', long, hide = true)]
        workers: Option<usize>,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn run() -> Result<(), String> {
    match Cli::parse().command {
        Command::Check { file } => {
            let src = read(&file)?;
            match Program::check_with_warnings(&src) {
                Ok(warnings) => {
                    for warning in warnings {
                        eprintln!("{warning}");
                    }
                    println!("ok");
                    Ok(())
                }
                Err(varpulis_engine::Error::Invalid(diagnostics)) => {
                    eprintln!("{diagnostics}");
                    Err(format!("{} does not check", file.display()))
                }
                Err(e) => Err(e.to_string()),
            }
        }
        Command::Parse { file } => {
            let src = read(&file)?;
            let ast = varpulis_parser::parse(&src).map_err(|e| e.to_string())?;
            println!("{ast:#?}");
            Ok(())
        }
        Command::Simulate {
            program,
            events,
            verbose,
            workers: _,
        } => {
            let src = read(&program)?;
            let mut program = Program::compile(&src).map_err(|e| e.to_string())?;
            let file = EventFileParser::parse_file(&events)?;
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            let (mut fed, mut emitted) = (0usize, 0usize);
            for timed in file.events {
                if verbose {
                    eprintln!(
                        "-> {}",
                        String::from_utf8_lossy(&timed.event.to_sink_payload())
                    );
                }
                fed += 1;
                for emit in program.feed(timed.event).map_err(|e| e.to_string())? {
                    emitted += 1;
                    writeln!(out, "{}", emit.to_json()).map_err(|e| e.to_string())?;
                }
            }
            for emit in program.end_of_input().map_err(|e| e.to_string())? {
                emitted += 1;
                writeln!(out, "{}", emit.to_json()).map_err(|e| e.to_string())?;
            }
            eprintln!("{fed} events in, {emitted} emitted");
            Ok(())
        }
    }
}
