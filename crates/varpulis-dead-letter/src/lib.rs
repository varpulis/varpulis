//! Dead letter queue for events that fail sink delivery.
//!
//! When an event cannot be delivered to its target sink (after retries or
//! circuit breaker rejection), it is routed here instead of being silently
//! dropped. The DLQ appends one JSON line per failed event to a file,
//! including error metadata for later reprocessing.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use varpulis_core::Event;

/// Configuration for dead letter queue behavior.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Maximum number of lines before rotation (0 = unlimited)
    pub max_lines: u64,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self { max_lines: 100_000 }
    }
}

/// Raw source payloads are truncated to this many bytes before being written
/// to the DLQ, bounding the on-disk line size for pathological (e.g. oversized)
/// records. The full original length is always recorded separately in
/// [`DlqRawEntry::payload_len`].
const MAX_RAW_PAYLOAD_BYTES: usize = 65_536;

/// A dead letter queue entry with error metadata.
#[derive(serde::Serialize)]
struct DlqEntry<'a> {
    /// ISO-8601 timestamp when the event was dead-lettered.
    timestamp: String,
    /// Original sink connector name.
    connector: &'a str,
    /// Error message from the failed delivery attempt.
    error: &'a str,
    /// The event that failed to deliver.
    event: &'a Event,
}

/// A dead letter entry for a **raw** payload that could not be decoded into an
/// [`Event`] — e.g. a malformed-JSON or oversized record rejected at a *source*
/// before it ever became an `Event`. There is no `Event` to attach, so the
/// original bytes (best-effort UTF-8, truncated) are preserved alongside the
/// source coordinates so the record stays inspectable and replayable instead of
/// being silently dropped with its offset advanced past it.
#[derive(serde::Serialize)]
struct DlqRawEntry<'a> {
    /// ISO-8601 timestamp when the payload was dead-lettered.
    timestamp: String,
    /// Source connector name.
    connector: &'a str,
    /// Why the payload was rejected (decode error text or size-limit message).
    error: &'a str,
    /// Source coordinates, e.g. `"topic=t partition=0 offset=42"`.
    source: &'a str,
    /// Original payload rendered as best-effort UTF-8 and truncated to
    /// [`MAX_RAW_PAYLOAD_BYTES`]. Malformed-JSON text round-trips exactly;
    /// non-UTF-8 bytes are replaced with U+FFFD.
    payload: String,
    /// True byte length of the original payload, before truncation.
    payload_len: usize,
    /// Whether `payload` was truncated (`payload_len > MAX_RAW_PAYLOAD_BYTES`).
    truncated: bool,
}

/// Owned version of a DLQ entry for reading entries back.
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct DlqEntryOwned {
    /// ISO-8601 timestamp when the event was dead-lettered.
    pub timestamp: String,
    /// Original sink connector name.
    pub connector: String,
    /// Error message from the failed delivery attempt.
    pub error: String,
    /// The event payload as JSON.
    pub event: serde_json::Value,
}

/// File-backed dead letter queue.
///
/// Appends JSON-lines to a file. Thread-safe via internal mutex on the file handle.
#[derive(Debug)]
pub struct DeadLetterQueue {
    file: Mutex<File>,
    path: PathBuf,
    /// Total events written to this DLQ (lifetime counter, not reset on rotation).
    pub events_total: AtomicU64,
    /// Current number of lines in the file (tracks rotation).
    current_lines: AtomicU64,
    /// Configuration for rotation behavior.
    config: DlqConfig,
}

impl DeadLetterQueue {
    /// Open (or create) a DLQ file at the given path with default config.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with_config(path, DlqConfig::default())
    }

    /// Open (or create) a DLQ file at the given path with the specified config.
    pub fn open_with_config(path: impl AsRef<Path>, config: DlqConfig) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        // Count existing lines for correct initialization
        let existing_lines = Self::count_lines(&path).unwrap_or(0);

        Ok(Self {
            file: Mutex::new(file),
            path,
            events_total: AtomicU64::new(existing_lines),
            current_lines: AtomicU64::new(existing_lines),
            config,
        })
    }

    /// Count lines in a file.
    fn count_lines(path: &Path) -> std::io::Result<u64> {
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut count = 0u64;
        for _ in reader.lines() {
            count += 1;
        }
        Ok(count)
    }

    /// Write a failed event to the DLQ with error metadata.
    pub fn write(&self, connector: &str, error: &str, event: &Event) {
        let entry = DlqEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            connector,
            error,
            event,
        };

        if let Ok(line) = serde_json::to_string(&entry) {
            let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());
            // Best-effort: log but don't propagate DLQ write errors.
            if writeln!(file, "{line}").is_ok() {
                self.events_total.fetch_add(1, Ordering::Relaxed);
                self.current_lines.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.maybe_rotate();
    }

    /// Write a **raw**, undecodable source payload to the DLQ.
    ///
    /// Unlike [`write`](Self::write), this accepts bytes that never became an
    /// [`Event`] — a source-side decode failure or an oversized record. The
    /// bytes are preserved (best-effort UTF-8, truncated to a bounded size)
    /// together with the source coordinates so the record can be inspected or
    /// replayed. This is the Varpulis "never silently dropped" guarantee applied
    /// to the ingest path: a poison record is captured here instead of vanishing
    /// as the consumer offset advances past it.
    pub fn write_raw(&self, connector: &str, error: &str, source: &str, payload: &[u8]) {
        let capped = &payload[..payload.len().min(MAX_RAW_PAYLOAD_BYTES)];
        let entry = DlqRawEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            connector,
            error,
            source,
            payload: String::from_utf8_lossy(capped).into_owned(),
            payload_len: payload.len(),
            truncated: payload.len() > MAX_RAW_PAYLOAD_BYTES,
        };

        if let Ok(line) = serde_json::to_string(&entry) {
            let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());
            // Best-effort: log but don't propagate DLQ write errors.
            if writeln!(file, "{line}").is_ok() {
                self.events_total.fetch_add(1, Ordering::Relaxed);
                self.current_lines.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.maybe_rotate();
    }

    /// Write a batch of failed events to the DLQ.
    pub fn write_batch(&self, connector: &str, error: &str, events: &[std::sync::Arc<Event>]) {
        let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());
        let timestamp = chrono::Utc::now().to_rfc3339();

        for event in events {
            let entry = DlqEntry {
                timestamp: timestamp.clone(),
                connector,
                error,
                event,
            };
            if let Ok(line) = serde_json::to_string(&entry) {
                if writeln!(file, "{line}").is_ok() {
                    self.events_total.fetch_add(1, Ordering::Relaxed);
                    self.current_lines.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Release file lock before rotation
        drop(file);

        self.maybe_rotate();
    }

    /// Check if rotation is needed and perform it.
    fn maybe_rotate(&self) {
        let max = self.config.max_lines;
        if max == 0 {
            return;
        }
        let current = self.current_lines.load(Ordering::Relaxed);
        if current > max {
            if let Err(e) = self.rotate() {
                tracing::warn!("DLQ rotation failed: {}", e);
            }
        }
    }

    /// Rotate the DLQ file by keeping the newest `max_lines/2` lines.
    fn rotate(&self) -> std::io::Result<()> {
        let keep = (self.config.max_lines / 2) as usize;

        let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());

        // Read all lines from the file
        let content = std::fs::read_to_string(&self.path)?;
        let lines: Vec<&str> = content.lines().collect();

        if lines.len() <= keep {
            return Ok(());
        }

        // Keep the newest lines
        let to_keep = &lines[lines.len() - keep..];

        // Rewrite the file (truncate + write)
        let new_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut writer = std::io::BufWriter::new(new_file);
        for line in to_keep {
            writeln!(writer, "{line}")?;
        }
        writer.flush()?;

        // Re-open in append mode for future writes
        let append_file = OpenOptions::new().append(true).open(&self.path)?;
        *file = append_file;

        self.current_lines.store(keep as u64, Ordering::Relaxed);

        tracing::info!(
            "DLQ rotated: kept {} newest lines out of {}",
            keep,
            lines.len()
        );

        Ok(())
    }

    /// Read DLQ entries with pagination.
    ///
    /// Reads the file, skips `offset` lines, and returns up to `limit` entries.
    pub fn read_entries(&self, offset: usize, limit: usize) -> std::io::Result<Vec<DlqEntryOwned>> {
        let file = File::open(&self.path)?;
        let reader = std::io::BufReader::new(file);
        let mut entries = Vec::new();

        for line_result in reader.lines().skip(offset).take(limit) {
            let line = line_result?;
            if let Ok(entry) = serde_json::from_str::<DlqEntryOwned>(&line) {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Clear the DLQ file (truncate to zero) and reset the line counter.
    pub fn clear(&self) -> std::io::Result<()> {
        let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());

        // Truncate the file
        let new_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        // Re-open in append mode
        let append_file = OpenOptions::new().append(true).open(&self.path)?;
        drop(new_file);
        *file = append_file;

        self.current_lines.store(0, Ordering::Relaxed);

        Ok(())
    }

    /// Path to the DLQ file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of events written so far (lifetime total).
    pub fn count(&self) -> u64 {
        self.events_total.load(Ordering::Relaxed)
    }

    /// Current number of lines in the file.
    pub fn line_count(&self) -> u64 {
        self.current_lines.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_write_and_count() {
        let dir = std::env::temp_dir().join("varpulis_dlq_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();
        assert_eq!(dlq.count(), 0);

        let event = Event::new("TestEvent");
        dlq.write("kafka-sink", "connection refused", &event);
        assert_eq!(dlq.count(), 1);

        dlq.write("mqtt-sink", "timeout", &event);
        assert_eq!(dlq.count(), 2);

        // Verify file content is valid JSONL
        let content = std::fs::read_to_string(&dir).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        let entry: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry["connector"], "kafka-sink");
        assert_eq!(entry["error"], "connection refused");
        assert!(entry["timestamp"].is_string());
        assert!(entry["event"].is_object());

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_write_raw_preserves_bytes_and_metadata() {
        let dir = std::env::temp_dir().join("varpulis_dlq_raw_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();
        assert_eq!(dlq.count(), 0);

        let malformed = b"{ this is not json";
        dlq.write_raw(
            "kafka-source",
            "decode failed: expected value",
            "topic=events partition=0 offset=42",
            malformed,
        );
        assert_eq!(dlq.count(), 1);

        // The raw entry is a valid JSON line carrying the original bytes and
        // source coordinates — so the poison record is recoverable, not lost.
        let content = std::fs::read_to_string(&dir).unwrap();
        let entry: serde_json::Value =
            serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(entry["connector"], "kafka-source");
        assert_eq!(entry["source"], "topic=events partition=0 offset=42");
        assert_eq!(entry["payload"], "{ this is not json");
        assert_eq!(entry["payload_len"], malformed.len());
        assert_eq!(entry["truncated"], false);
        assert!(entry["timestamp"].is_string());

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_write_raw_truncates_oversized_payload() {
        let dir = std::env::temp_dir().join("varpulis_dlq_raw_trunc_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();

        let huge = vec![b'x'; MAX_RAW_PAYLOAD_BYTES + 4_096];
        dlq.write_raw(
            "kafka-source",
            "payload too large",
            "topic=t offset=1",
            &huge,
        );

        let content = std::fs::read_to_string(&dir).unwrap();
        let entry: serde_json::Value =
            serde_json::from_str(content.lines().next().unwrap()).unwrap();
        // Full length is recorded even though the stored bytes are capped.
        assert_eq!(entry["payload_len"], huge.len());
        assert_eq!(entry["truncated"], true);
        assert_eq!(
            entry["payload"].as_str().unwrap().len(),
            MAX_RAW_PAYLOAD_BYTES,
            "stored payload must be capped at MAX_RAW_PAYLOAD_BYTES"
        );

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_batch_write() {
        let dir = std::env::temp_dir().join("varpulis_dlq_batch_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();

        let events: Vec<std::sync::Arc<Event>> = (0..5)
            .map(|i| std::sync::Arc::new(Event::new(format!("Event{i}"))))
            .collect();

        dlq.write_batch("http-sink", "503 Service Unavailable", &events);
        assert_eq!(dlq.count(), 5);

        let content = std::fs::read_to_string(&dir).unwrap();
        assert_eq!(content.lines().count(), 5);

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_rotation() {
        let dir = std::env::temp_dir().join("varpulis_dlq_rotation_test");
        let _ = std::fs::remove_file(&dir);

        // Use a small max_lines to trigger rotation easily.
        // With max_lines=4, rotation keeps newest 2 lines.
        // Writing 10 events:
        //   1-4: lines=4 (not > 4)
        //   5:   lines=5 (> 4) → rotate → keep 2 (events 4,5), lines=2
        //   6:   lines=3
        //   7:   lines=4
        //   8:   lines=5 (> 4) → rotate → keep 2 (events 7,8), lines=2
        //   9:   lines=3
        //   10:  lines=4
        // Final: 4 lines in file
        let config = DlqConfig { max_lines: 4 };
        let dlq = DeadLetterQueue::open_with_config(&dir, config).unwrap();

        let event = Event::new("TestEvent");
        for _ in 0..10 {
            dlq.write("test-sink", "test error", &event);
        }

        let content = std::fs::read_to_string(&dir).unwrap();
        let line_count = content.lines().count();
        // After two rotations, file should have max_lines (4) or fewer lines
        assert!(
            line_count <= 4,
            "Expected at most 4 lines after rotation, got {line_count}"
        );
        assert_eq!(dlq.line_count(), line_count as u64);

        // events_total should still reflect all written events
        assert_eq!(dlq.count(), 10);

        // Verify file contents are valid JSON
        for line in content.lines() {
            assert!(
                serde_json::from_str::<serde_json::Value>(line).is_ok(),
                "Line is not valid JSON: {line}"
            );
        }

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_read_entries() {
        let dir = std::env::temp_dir().join("varpulis_dlq_read_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();

        // Write some entries
        for i in 0..10 {
            let event = Event::new(format!("Event{i}"));
            dlq.write("test-sink", &format!("error {i}"), &event);
        }

        // Read with offset and limit
        let entries = dlq.read_entries(2, 3).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].error, "error 2");
        assert_eq!(entries[1].error, "error 3");
        assert_eq!(entries[2].error, "error 4");

        // Read from beginning
        let entries = dlq.read_entries(0, 5).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].error, "error 0");

        // Read past end
        let entries = dlq.read_entries(8, 5).unwrap();
        assert_eq!(entries.len(), 2);

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_clear() {
        let dir = std::env::temp_dir().join("varpulis_dlq_clear_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();

        // Write some entries
        let event = Event::new("TestEvent");
        for _ in 0..5 {
            dlq.write("test-sink", "test error", &event);
        }
        assert_eq!(dlq.line_count(), 5);

        // Clear the DLQ
        dlq.clear().unwrap();
        assert_eq!(dlq.line_count(), 0);

        // Verify file is empty
        let content = std::fs::read_to_string(&dir).unwrap();
        assert!(content.is_empty());

        // Verify we can still write after clearing
        dlq.write("test-sink", "new error", &event);
        assert_eq!(dlq.line_count(), 1);
        let content = std::fs::read_to_string(&dir).unwrap();
        assert_eq!(content.lines().count(), 1);

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_dlq_open_existing_file_counts_lines() {
        let dir = std::env::temp_dir().join("varpulis_dlq_existing_test");
        let _ = std::fs::remove_file(&dir);

        // Write some lines first
        {
            let dlq = DeadLetterQueue::open(&dir).unwrap();
            let event = Event::new("TestEvent");
            for _ in 0..3 {
                dlq.write("test-sink", "test error", &event);
            }
        }

        // Re-open and check that existing lines are counted
        let dlq = DeadLetterQueue::open(&dir).unwrap();
        assert_eq!(dlq.line_count(), 3);
        assert_eq!(dlq.count(), 3);

        let _ = std::fs::remove_file(&dir);
    }
}
