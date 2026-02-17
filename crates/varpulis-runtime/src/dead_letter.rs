//! Dead letter queue for events that fail sink delivery.
//!
//! When an event cannot be delivered to its target sink (after retries or
//! circuit breaker rejection), it is routed here instead of being silently
//! dropped. The DLQ appends one JSON line per failed event to a file,
//! including error metadata for later reprocessing.

use crate::event::Event;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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

/// File-backed dead letter queue.
///
/// Appends JSON-lines to a file. Thread-safe via internal mutex on the file handle.
pub struct DeadLetterQueue {
    file: Mutex<File>,
    path: PathBuf,
    /// Total events written to this DLQ.
    pub events_total: AtomicU64,
}

impl DeadLetterQueue {
    /// Open (or create) a DLQ file at the given path.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            file: Mutex::new(file),
            path,
            events_total: AtomicU64::new(0),
        })
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
            if writeln!(file, "{}", line).is_ok() {
                self.events_total.fetch_add(1, Ordering::Relaxed);
            }
        }
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
                if writeln!(file, "{}", line).is_ok() {
                    self.events_total.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Path to the DLQ file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of events written so far.
    pub fn count(&self) -> u64 {
        self.events_total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

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
    fn test_dlq_batch_write() {
        let dir = std::env::temp_dir().join("varpulis_dlq_batch_test");
        let _ = std::fs::remove_file(&dir);

        let dlq = DeadLetterQueue::open(&dir).unwrap();

        let events: Vec<std::sync::Arc<Event>> = (0..5)
            .map(|i| std::sync::Arc::new(Event::new(format!("Event{}", i))))
            .collect();

        dlq.write_batch("http-sink", "503 Service Unavailable", &events);
        assert_eq!(dlq.count(), 5);

        let content = std::fs::read_to_string(&dir).unwrap();
        assert_eq!(content.lines().count(), 5);

        let _ = std::fs::remove_file(&dir);
    }
}
