//! CLI output helpers for consistent formatting, colors, and progress indicators.
//!
//! Respects the `NO_COLOR` environment variable (<https://no-color.org/>).

use std::env;

use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;

/// Whether color output is enabled (respects NO_COLOR env var).
pub fn color_enabled() -> bool {
    env::var("NO_COLOR").is_err()
}

/// Create a progress bar for known total (preload mode).
pub fn create_progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({per_sec}) [{eta}]",
        )
        .unwrap()
        .progress_chars("##-"),
    );
    pb
}

/// Create a spinner for unknown total (streaming mode).
pub fn create_spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} {msg} [{elapsed_precise}]")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(120));
    pb
}

/// Print a success message in green.
pub fn success(msg: &str) {
    if color_enabled() {
        println!("{}", msg.green().bold());
    } else {
        println!("{msg}");
    }
}

/// Print an error message in red.
pub fn error(msg: &str) {
    if color_enabled() {
        eprintln!("{}", msg.red().bold());
    } else {
        eprintln!("{msg}");
    }
}

/// Print a warning message in yellow.
pub fn warning(msg: &str) {
    if color_enabled() {
        eprintln!("{}", msg.yellow());
    } else {
        eprintln!("{msg}");
    }
}

/// Print a header/banner.
pub fn header(msg: &str) {
    if color_enabled() {
        println!("{}", msg.bold());
        println!("{}", "=".repeat(msg.len()).dimmed());
    } else {
        println!("{msg}");
        println!("{}", "=".repeat(msg.len()));
    }
}
