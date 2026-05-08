//! TUI transport for the interactive session.
//!
//! Renders a split-pane terminal UI using ratatui with four panes:
//! topology (top-left), event stream (top-right), input (bottom-left),
//! and metrics (bottom-right).
//!
//! This module is gated behind the `tui` feature.

use std::io;

use crossterm::event::{Event as CrosstermEvent, EventStream, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures_util::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use varpulis_runtime::engine::graph::{GraphEdge, GraphNode};
use varpulis_runtime::interactive::{InteractiveSession, SessionCommand, SessionResponse};

/// Number of panes in the UI.
const PANE_COUNT: usize = 4;

/// Pane indices.
const PANE_TOPOLOGY: usize = 0;
const PANE_EVENTS: usize = 1;
const PANE_INPUT: usize = 2;
const PANE_METRICS: usize = 3;

/// Maximum number of event log entries to keep.
const MAX_EVENT_LOG: usize = 5000;

/// Application state for the TUI.
struct TuiApp {
    session: InteractiveSession,
    // response_rx removed — polling via poll_connectors/poll_generator is sufficient
    /// Scrolling log of output events and trace entries.
    event_log: Vec<LogEntry>,
    /// Current scroll offset in the event log (0 = bottom/latest).
    event_log_offset: usize,
    /// Topology display text.
    topology_lines: Vec<String>,
    /// Metrics display text.
    metrics_lines: Vec<String>,
    /// Current input buffer.
    input_buffer: String,
    /// Cursor position within the input buffer.
    cursor_pos: usize,
    /// Currently active pane index.
    active_pane: usize,
    /// Whether the app is still running.
    running: bool,
    /// Status line (shown at the bottom of the input pane).
    status: String,
    /// Whether the generator is running.
    generator_running: bool,
    /// Whether tracing is enabled.
    trace_enabled: bool,
}

/// A single entry in the event log, with a color category.
struct LogEntry {
    text: String,
    color: Color,
}

impl TuiApp {
    fn new() -> Self {
        let session = InteractiveSession::new();
        Self {
            session,
            event_log: Vec::new(),
            event_log_offset: 0,
            topology_lines: vec!["(no program loaded)".into()],
            metrics_lines: vec![
                "Events:  0".into(),
                "Output:  0".into(),
                "Streams: 0".into(),
            ],
            input_buffer: String::new(),
            cursor_pos: 0,
            active_pane: PANE_INPUT,
            running: true,
            status: "Enter = newline, blank Enter = submit | Tab = switch pane | :help".into(),
            generator_running: false,
            trace_enabled: false,
        }
    }

    /// Process responses from a handle_command call.
    fn process_responses(&mut self, responses: Vec<SessionResponse>) {
        for resp in responses {
            self.handle_response(resp);
        }
    }

    /// Handle a single session response, updating UI state.
    fn handle_response(&mut self, resp: SessionResponse) {
        match resp {
            SessionResponse::Ready { version } => {
                self.status = format!("Varpulis v{version} ready.");
            }
            SessionResponse::Loaded {
                streams,
                added,
                removed,
                ..
            } => {
                self.push_log(
                    format!("Loaded {} stream(s): {}", streams.len(), streams.join(", ")),
                    Color::Green,
                );
                if !added.is_empty() {
                    self.push_log(format!("  added: {}", added.join(", ")), Color::Green);
                }
                if !removed.is_empty() {
                    self.push_log(format!("  removed: {}", removed.join(", ")), Color::Yellow);
                }
                // Refresh topology
                self.refresh_topology();
                // Refresh metrics
                self.refresh_metrics();
            }
            SessionResponse::Output {
                stream,
                event,
                timestamp,
            } => {
                let event_str = serde_json::to_string(&event).unwrap_or_default();
                self.push_log(format!("[{timestamp}] {stream}: {event_str}"), Color::Cyan);
            }
            SessionResponse::Trace { entries } => {
                for entry in entries {
                    let stream = entry.stream.as_deref().unwrap_or("?");
                    let detail = entry.detail.as_deref().unwrap_or("");
                    self.push_log(
                        format!("  TRACE [{stream}] {}: {detail}", entry.kind),
                        Color::DarkGray,
                    );
                }
            }
            SessionResponse::Metrics {
                events_processed,
                output_events,
                streams_count,
            } => {
                self.metrics_lines = vec![
                    format!("Events:  {events_processed}"),
                    format!("Output:  {output_events}"),
                    format!("Streams: {streams_count}"),
                ];
            }
            SessionResponse::Topology { nodes, edges } => {
                self.topology_lines = render_topology(&nodes, &edges);
            }
            SessionResponse::Streams { streams } => {
                self.push_log(format!("Streams ({}):", streams.len()), Color::White);
                for s in &streams {
                    self.push_log(
                        format!("  {} ({}, {} ops)", s.name, s.source, s.ops_count),
                        Color::White,
                    );
                }
            }
            SessionResponse::Error { message } => {
                self.push_log(format!("ERROR: {message}"), Color::Red);
                self.status = format!("Error: {message}");
            }
            SessionResponse::GeneratorStatus { running, generated } => {
                self.generator_running = running;
                let state = if running { "started" } else { "stopped" };
                self.push_log(
                    format!("Generator {state} (generated: {generated})"),
                    Color::Magenta,
                );
            }
            SessionResponse::Saved { path, lines } => {
                self.push_log(format!("Saved {lines} lines to {path}"), Color::Green);
            }
            SessionResponse::Source { vpl } => {
                for line in vpl.lines() {
                    self.push_log(line.to_string(), Color::White);
                }
            }
            SessionResponse::Bye => {
                self.running = false;
            }
        }
    }

    /// Add an entry to the event log.
    fn push_log(&mut self, text: String, color: Color) {
        self.event_log.push(LogEntry { text, color });
        if self.event_log.len() > MAX_EVENT_LOG {
            self.event_log
                .drain(0..self.event_log.len() - MAX_EVENT_LOG);
        }
        // Auto-scroll to bottom when new entries arrive (if already at bottom)
        if self.event_log_offset == 0 {
            // Already at bottom, stay there
        }
    }

    /// Refresh the topology display by querying the session.
    fn refresh_topology(&mut self) {
        let responses = self.session.handle_command(SessionCommand::GetTopology);
        for resp in responses {
            if let SessionResponse::Topology { nodes, edges } = resp {
                self.topology_lines = render_topology(&nodes, &edges);
            } else if let SessionResponse::Error { message } = resp {
                self.topology_lines = vec![format!("(error: {message})")];
            }
        }
    }

    /// Refresh the metrics display.
    fn refresh_metrics(&mut self) {
        let responses = self.session.handle_command(SessionCommand::GetMetrics);
        for resp in responses {
            if let SessionResponse::Metrics {
                events_processed,
                output_events,
                streams_count,
            } = resp
            {
                self.metrics_lines = vec![
                    format!("Events:  {events_processed}"),
                    format!("Output:  {output_events}"),
                    format!("Streams: {streams_count}"),
                ];
            }
        }
    }

    /// Handle a key event.
    fn handle_key(&mut self, key: KeyEvent) {
        match (key.modifiers, key.code) {
            // Ctrl+Q: quit
            (KeyModifiers::CONTROL, KeyCode::Char('q')) => {
                self.running = false;
            }
            // Ctrl+G: toggle generator
            (KeyModifiers::CONTROL, KeyCode::Char('g')) => {
                self.toggle_generator();
            }
            // Ctrl+T: toggle trace
            (KeyModifiers::CONTROL, KeyCode::Char('t')) => {
                self.trace_enabled = !self.trace_enabled;
                self.session.handle_command(SessionCommand::SetTrace {
                    enabled: self.trace_enabled,
                });
                let state = if self.trace_enabled { "ON" } else { "OFF" };
                self.status = format!("Trace: {state}");
            }
            // Tab: cycle panes
            (_, KeyCode::Tab) => {
                self.active_pane = (self.active_pane + 1) % PANE_COUNT;
            }
            // BackTab (Shift+Tab): cycle panes backward
            (_, KeyCode::BackTab) => {
                self.active_pane = (self.active_pane + PANE_COUNT - 1) % PANE_COUNT;
            }
            // Up/Down in events pane: scroll
            (_, KeyCode::Up)
                if self.active_pane == PANE_EVENTS
                    && self.event_log_offset < self.event_log.len().saturating_sub(1) =>
            {
                self.event_log_offset += 1;
            }
            (_, KeyCode::Down) if self.active_pane == PANE_EVENTS => {
                self.event_log_offset = self.event_log_offset.saturating_sub(1);
            }
            (_, KeyCode::PageUp) if self.active_pane == PANE_EVENTS => {
                self.event_log_offset = self
                    .event_log_offset
                    .saturating_add(20)
                    .min(self.event_log.len().saturating_sub(1));
            }
            (_, KeyCode::PageDown) if self.active_pane == PANE_EVENTS => {
                self.event_log_offset = self.event_log_offset.saturating_sub(20);
            }
            // Input pane text editing
            (_, KeyCode::Char(c)) if self.active_pane == PANE_INPUT => {
                self.input_buffer.insert(self.cursor_pos, c);
                self.cursor_pos += 1;
            }
            (_, KeyCode::Backspace) if self.active_pane == PANE_INPUT && self.cursor_pos > 0 => {
                self.cursor_pos -= 1;
                self.input_buffer.remove(self.cursor_pos);
            }
            (_, KeyCode::Delete)
                if self.active_pane == PANE_INPUT && self.cursor_pos < self.input_buffer.len() =>
            {
                self.input_buffer.remove(self.cursor_pos);
            }
            (_, KeyCode::Left) if self.active_pane == PANE_INPUT => {
                self.cursor_pos = self.cursor_pos.saturating_sub(1);
            }
            (_, KeyCode::Right)
                if self.active_pane == PANE_INPUT && self.cursor_pos < self.input_buffer.len() =>
            {
                self.cursor_pos += 1;
            }
            (_, KeyCode::Home) if self.active_pane == PANE_INPUT => {
                self.cursor_pos = 0;
            }
            (_, KeyCode::End) if self.active_pane == PANE_INPUT => {
                self.cursor_pos = self.input_buffer.len();
            }
            // Enter: newline in input buffer; blank Enter (double-Enter) submits
            (_, KeyCode::Enter) if self.active_pane == PANE_INPUT => {
                let last_line_empty =
                    self.input_buffer.ends_with('\n') || self.input_buffer.trim().is_empty();
                if last_line_empty && !self.input_buffer.trim().is_empty() {
                    self.submit_input();
                } else if self.input_buffer.trim().is_empty() {
                    // Don't add newlines to empty buffer
                } else {
                    self.input_buffer.push('\n');
                    self.cursor_pos = self.input_buffer.len();
                }
            }
            // Escape: clear input
            (_, KeyCode::Esc) if self.active_pane == PANE_INPUT => {
                self.input_buffer.clear();
                self.cursor_pos = 0;
            }
            _ => {}
        }
    }

    /// Submit the current input buffer as a command or VPL.
    fn submit_input(&mut self) {
        let input = self.input_buffer.trim().to_string();
        if input.is_empty() {
            return;
        }
        self.input_buffer.clear();
        self.cursor_pos = 0;

        // Check for colon-commands
        if let Some(cmd) = input.strip_prefix(':') {
            self.handle_colon_command(cmd);
            return;
        }

        // Check for JSON command (starts with '{')
        if input.starts_with('{') {
            match serde_json::from_str::<SessionCommand>(&input) {
                Ok(cmd) => {
                    let responses = self.session.handle_command(cmd);
                    self.process_responses(responses);
                }
                Err(e) => {
                    self.push_log(format!("Invalid JSON command: {e}"), Color::Red);
                }
            }
            return;
        }

        // Check if it looks like an event literal: `EventType { field: value }`
        if input.contains('{') && input.contains('}') && !input.starts_with("event ") {
            // Attempt to parse as event literal
            if let Some(brace_pos) = input.find('{') {
                let event_type = input[..brace_pos].trim();
                let fields_str = input[brace_pos..].trim();
                // Try parsing fields as JSON (convert evt syntax to JSON)
                let json_str = crate::commands::interactive::shell::evt_fields_to_json(fields_str);
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    self.push_log(format!("> {input}"), Color::DarkGray);
                    let responses = self.session.handle_command(SessionCommand::Inject {
                        event_type: event_type.to_string(),
                        data,
                    });
                    self.process_responses(responses);
                    return;
                }
            }
        }

        // Otherwise, treat as VPL source code (incremental append)
        self.push_log(format!("> {input}"), Color::White);
        let responses = self
            .session
            .handle_command(SessionCommand::AppendVpl { vpl: input.clone() });
        self.process_responses(responses);
    }

    /// Handle a `:command` entered in the input pane.
    fn handle_colon_command(&mut self, cmd: &str) {
        let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
        let name = parts[0];
        let arg = parts.get(1).copied().unwrap_or("");

        match name {
            "help" | "h" => {
                self.push_log("Commands:".into(), Color::Yellow);
                self.push_log(
                    "  :load <file>     Load VPL from file".into(),
                    Color::Yellow,
                );
                self.push_log(
                    "  :inject <file>   Inject events from file".into(),
                    Color::Yellow,
                );
                self.push_log(
                    "  :gen <schema>    Start generator (fraud, iot, trading)".into(),
                    Color::Yellow,
                );
                self.push_log("  :stop            Stop generator".into(), Color::Yellow);
                self.push_log(
                    "  :streams         List loaded streams".into(),
                    Color::Yellow,
                );
                self.push_log("  :metrics         Show metrics".into(), Color::Yellow);
                self.push_log("  :trace           Toggle tracing".into(), Color::Yellow);
                self.push_log("  :quit            Exit".into(), Color::Yellow);
                self.push_log("  (or type VPL source directly)".into(), Color::Yellow);
                self.push_log("Shortcuts:".into(), Color::Yellow);
                self.push_log("  Ctrl+Q  Quit".into(), Color::Yellow);
                self.push_log("  Ctrl+G  Toggle generator".into(), Color::Yellow);
                self.push_log("  Ctrl+T  Toggle trace".into(), Color::Yellow);
                self.push_log("  Tab     Cycle panes".into(), Color::Yellow);
            }
            "load" | "l" => {
                if arg.is_empty() {
                    self.push_log("Usage: :load <file.vpl>".into(), Color::Red);
                } else {
                    let responses = self.session.handle_command(SessionCommand::LoadFile {
                        path: arg.to_string(),
                    });
                    self.process_responses(responses);
                }
            }
            "inject" | "i" => {
                if arg.is_empty() {
                    self.push_log("Usage: :inject <file.evt>".into(), Color::Red);
                } else {
                    let responses = self.session.handle_command(SessionCommand::InjectFile {
                        path: arg.to_string(),
                    });
                    self.process_responses(responses);
                }
            }
            "gen" | "generate" => {
                if arg.is_empty() {
                    self.push_log(
                        "Usage: :gen <schema> (fraud, iot, trading)".into(),
                        Color::Red,
                    );
                } else {
                    let responses = self.session.handle_command(SessionCommand::Generate {
                        schema: arg.to_string(),
                        rate: 1000,
                        duration: None,
                    });
                    self.process_responses(responses);
                }
            }
            "stop" => {
                let responses = self.session.handle_command(SessionCommand::StopGenerate);
                self.process_responses(responses);
            }
            "streams" | "ls" => {
                let responses = self.session.handle_command(SessionCommand::GetStreams);
                self.process_responses(responses);
            }
            "metrics" | "m" => {
                self.refresh_metrics();
            }
            "trace" | "t" => {
                self.trace_enabled = !self.trace_enabled;
                self.session.handle_command(SessionCommand::SetTrace {
                    enabled: self.trace_enabled,
                });
                let state = if self.trace_enabled { "ON" } else { "OFF" };
                self.status = format!("Trace: {state}");
            }
            "save" | "s" => {
                if arg.is_empty() {
                    self.push_log("Usage: :save <file.vpl>".into(), Color::Red);
                } else {
                    let responses = self.session.handle_command(SessionCommand::Save {
                        path: arg.to_string(),
                    });
                    self.process_responses(responses);
                }
            }
            "source" | "src" => {
                let responses = self.session.handle_command(SessionCommand::GetSource);
                self.process_responses(responses);
            }
            "quit" | "q" => {
                self.running = false;
            }
            _ => {
                self.push_log(format!("Unknown command: :{name}"), Color::Red);
            }
        }
    }

    /// Toggle the event generator on/off.
    fn toggle_generator(&mut self) {
        if self.generator_running {
            let responses = self.session.handle_command(SessionCommand::StopGenerate);
            self.process_responses(responses);
        } else {
            // Default schema: "fraud", rate 1000
            let responses = self.session.handle_command(SessionCommand::Generate {
                schema: "fraud".into(),
                rate: 1000,
                duration: None,
            });
            self.process_responses(responses);
        }
    }

    /// Render the UI into a terminal frame.
    fn render(&self, f: &mut Frame) {
        let area = f.area();

        // Top/bottom split: 60% / 40%
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(area);

        // Top row: topology (35%) | events (65%)
        let top = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
            .split(vertical[0]);

        // Bottom row: input (50%) | metrics (50%)
        let bottom = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(vertical[1]);

        // Render each pane
        self.render_topology_pane(f, top[0]);
        self.render_events_pane(f, top[1]);
        self.render_input_pane(f, bottom[0]);
        self.render_metrics_pane(f, bottom[1]);
    }

    fn render_topology_pane(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let border_style = self.pane_border_style(PANE_TOPOLOGY);
        let block = Block::default()
            .title(" TOPOLOGY ")
            .borders(Borders::ALL)
            .border_style(border_style);

        let text: Vec<Line> = self
            .topology_lines
            .iter()
            .map(|line| {
                if line.starts_with('[') {
                    Line::from(Span::styled(line.clone(), Style::default().fg(Color::Cyan)))
                } else {
                    Line::from(Span::styled(
                        line.clone(),
                        Style::default().fg(Color::DarkGray),
                    ))
                }
            })
            .collect();

        let paragraph = Paragraph::new(text).block(block).wrap(Wrap { trim: false });
        f.render_widget(paragraph, area);
    }

    fn render_events_pane(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let border_style = self.pane_border_style(PANE_EVENTS);
        let title = if self.event_log_offset > 0 {
            format!(" EVENT STREAM (scroll: {}) ", self.event_log_offset)
        } else {
            " EVENT STREAM ".to_string()
        };
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(border_style);

        // Calculate visible window
        let inner_height = area.height.saturating_sub(2) as usize; // minus borders
        let total = self.event_log.len();
        let end = total.saturating_sub(self.event_log_offset);
        let start = end.saturating_sub(inner_height);

        let items: Vec<ListItem> = self.event_log[start..end]
            .iter()
            .map(|entry| {
                ListItem::new(Line::from(Span::styled(
                    entry.text.clone(),
                    Style::default().fg(entry.color),
                )))
            })
            .collect();

        let list = List::new(items).block(block);
        f.render_widget(list, area);
    }

    fn render_input_pane(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let border_style = self.pane_border_style(PANE_INPUT);
        let block = Block::default()
            .title(" INPUT ")
            .borders(Borders::ALL)
            .border_style(border_style);

        let inner = block.inner(area);
        f.render_widget(block, area);

        // Split inner into input line and status line
        if inner.height >= 2 {
            let input_area = ratatui::layout::Rect {
                x: inner.x,
                y: inner.y,
                width: inner.width,
                height: inner.height.saturating_sub(1),
            };
            let status_area = ratatui::layout::Rect {
                x: inner.x,
                y: inner.y + inner.height.saturating_sub(1),
                width: inner.width,
                height: 1,
            };

            // Render multi-line input with per-line prompts
            let lines_text: Vec<&str> = if self.input_buffer.is_empty() {
                vec![""]
            } else {
                self.input_buffer.split('\n').collect()
            };
            let display_lines: Vec<Line> = lines_text
                .iter()
                .enumerate()
                .map(|(i, line)| {
                    let prompt = if i == 0 { "> " } else { "... " };
                    Line::from(format!("{prompt}{line}"))
                })
                .collect();
            let input_paragraph = Paragraph::new(display_lines).wrap(Wrap { trim: false });
            f.render_widget(input_paragraph, input_area);

            // Show cursor position if this pane is active
            if self.active_pane == PANE_INPUT {
                // Find which line the cursor is on and the column within that line
                let before_cursor = &self.input_buffer[..self.cursor_pos];
                let cursor_line = before_cursor.matches('\n').count();
                let last_newline = before_cursor.rfind('\n').map(|p| p + 1).unwrap_or(0);
                let col_in_line = self.cursor_pos - last_newline;
                let prompt_len = if cursor_line == 0 { 2 } else { 4 }; // "> " or "... "
                let cursor_x = inner.x + (prompt_len + col_in_line) as u16;
                let cursor_y = inner.y + cursor_line as u16;
                if cursor_x < inner.x + inner.width && cursor_y < inner.y + input_area.height {
                    f.set_cursor_position((cursor_x, cursor_y));
                }
            }

            // Render status line
            let status = Paragraph::new(Line::from(Span::styled(
                self.status.clone(),
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            )));
            f.render_widget(status, status_area);
        } else {
            let display = format!("> {}", self.input_buffer);
            let input_line = Paragraph::new(display);
            f.render_widget(input_line, inner);
        }
    }

    fn render_metrics_pane(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let border_style = self.pane_border_style(PANE_METRICS);
        let mut title_parts = vec![" METRICS ".to_string()];
        if self.generator_running {
            title_parts.push("[GEN] ".into());
        }
        if self.trace_enabled {
            title_parts.push("[TRACE] ".into());
        }
        let title = title_parts.join("");

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(border_style);

        let text: Vec<Line> = self
            .metrics_lines
            .iter()
            .map(|line| {
                Line::from(Span::styled(
                    line.clone(),
                    Style::default().fg(Color::White),
                ))
            })
            .collect();

        let paragraph = Paragraph::new(text).block(block);
        f.render_widget(paragraph, area);
    }

    /// Get the border style for a pane (highlighted if active).
    fn pane_border_style(&self, pane: usize) -> Style {
        if self.active_pane == pane {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        }
    }
}

/// Convert a pipeline topology into simple ASCII lines.
fn render_topology(nodes: &[GraphNode], edges: &[GraphEdge]) -> Vec<String> {
    if nodes.is_empty() {
        return vec!["(empty pipeline)".into()];
    }

    // Build adjacency: for each node, find its children
    use std::collections::{HashMap, HashSet};
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut has_parent: HashSet<&str> = HashSet::new();

    for edge in edges {
        children
            .entry(edge.source.as_str())
            .or_default()
            .push(edge.target.as_str());
        has_parent.insert(edge.target.as_str());
    }

    // Find root nodes (no incoming edges)
    let roots: Vec<&str> = nodes
        .iter()
        .filter(|n| !has_parent.contains(n.id.as_str()))
        .map(|n| n.id.as_str())
        .collect();

    let node_map: HashMap<&str, &GraphNode> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let mut lines = Vec::new();

    for (i, root) in roots.iter().enumerate() {
        if i > 0 {
            lines.push(String::new());
        }
        render_node_recursive(root, &node_map, &children, &mut lines, 0);
    }

    if lines.is_empty() {
        // Fallback: just list all nodes
        for node in nodes {
            lines.push(format!("[{}: {}]", node.node_type, node.label));
        }
    }

    lines
}

/// Recursively render a node and its children as ASCII art.
fn render_node_recursive(
    node_id: &str,
    node_map: &std::collections::HashMap<&str, &GraphNode>,
    children: &std::collections::HashMap<&str, Vec<&str>>,
    lines: &mut Vec<String>,
    depth: usize,
) {
    let indent = "    ".repeat(depth);

    if let Some(node) = node_map.get(node_id) {
        let type_label = &node.node_type;
        let label = &node.label;
        lines.push(format!("{indent}[{type_label}: {label}]"));
    } else {
        lines.push(format!("{indent}[{node_id}]"));
    }

    if let Some(kids) = children.get(node_id) {
        if !kids.is_empty() {
            lines.push(format!("{indent}    |"));
            lines.push(format!("{indent}    v"));
        }
        for (i, kid) in kids.iter().enumerate() {
            if i > 0 {
                // For branches, add a separator
                lines.push(format!("{indent}    |"));
                lines.push(format!("{indent}    v"));
            }
            render_node_recursive(kid, node_map, children, lines, depth + 1);
        }
    }
}

/// Run the TUI interactive session.
///
/// Sets up the terminal, runs the main event loop, and restores the terminal
/// on exit (including panic recovery).
pub async fn run_tui_session(
    file: Option<&std::path::Path>,
    generate: Option<&str>,
    rate: u64,
    trace: bool,
) -> anyhow::Result<()> {
    // Install panic hook to restore terminal on panic
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Create app
    let mut app = TuiApp::new();

    // Auto-load file if provided
    if let Some(path) = file {
        let vpl = std::fs::read_to_string(path)?;
        let responses = app.session.handle_command(SessionCommand::LoadVpl { vpl });
        app.process_responses(responses);
    }

    // Auto-start generator if provided
    if let Some(schema) = generate {
        let responses = app.session.handle_command(SessionCommand::Generate {
            schema: schema.to_string(),
            rate,
            duration: None,
        });
        app.process_responses(responses);
    }

    // Enable trace if requested
    if trace {
        app.trace_enabled = true;
        app.session
            .handle_command(SessionCommand::SetTrace { enabled: true });
    }

    // Keyboard event stream
    let mut event_stream = EventStream::new();

    // Generator poll interval
    let mut generator_interval = tokio::time::interval(std::time::Duration::from_millis(50));
    generator_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Connector poll interval (same cadence as generator)
    let mut connector_interval = tokio::time::interval(std::time::Duration::from_millis(50));
    connector_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Metrics refresh interval (every 1s)
    let mut metrics_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    metrics_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Main loop
    let result: anyhow::Result<()> = async {
        while app.running {
            terminal.draw(|f| app.render(f))?;

            tokio::select! {
                event = event_stream.next() => {
                    match event {
                        Some(Ok(CrosstermEvent::Key(key))) => {
                            app.handle_key(key);
                        }
                        Some(Ok(CrosstermEvent::Resize(_, _))) => {
                            // Terminal will redraw on next iteration
                        }
                        Some(Err(_)) | None => {
                            app.running = false;
                        }
                        _ => {}
                    }
                }
                _ = generator_interval.tick() => {
                    let responses = app.session.poll_generator();
                    app.process_responses(responses);
                }
                _ = connector_interval.tick() => {
                    let responses = app.session.poll_connectors();
                    app.process_responses(responses);
                }
                _ = metrics_interval.tick() => {
                    if app.generator_running || app.session.has_source_bindings() {
                        app.refresh_metrics();
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}
