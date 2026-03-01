//! Physical plan: the bridge between logical plans and runtime execution.
//!
//! A `PhysicalPlan` captures metadata about the materialized streams,
//! correlating each back to its logical plan node for introspection.

/// Physical plan produced by materializing a logical plan.
///
/// Stores metadata about each stream's materialization without owning
/// the actual runtime state (which lives in `Engine::streams`).
pub(crate) struct PhysicalPlan {
    pub streams: Vec<PhysicalStream>,
}

/// Metadata for a single materialized stream.
pub(crate) struct PhysicalStream {
    /// Stream name
    pub name: String,
    /// Number of runtime operations in the stream pipeline
    pub operation_count: usize,
    /// Human-readable summary of operations
    pub operation_summary: String,
    /// Correlation ID back to the logical plan stream
    pub logical_id: u32,
    /// Event types this stream is registered to receive
    pub registered_event_types: Vec<String>,
}

impl PhysicalPlan {
    /// Create a new empty physical plan.
    pub const fn new() -> Self {
        Self {
            streams: Vec::new(),
        }
    }

    /// Add a physical stream to the plan.
    pub fn add_stream(&mut self, stream: PhysicalStream) {
        self.streams.push(stream);
    }

    /// Get the number of streams in the plan.
    pub const fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Get a summary of the physical plan for debugging.
    pub fn summary(&self) -> String {
        let mut out = format!("PhysicalPlan ({} streams):\n", self.streams.len());
        for s in &self.streams {
            out.push_str(&format!(
                "  [logical={}] {} — {} ops ({}), events: [{}]\n",
                s.logical_id,
                s.name,
                s.operation_count,
                s.operation_summary,
                s.registered_event_types.join(", "),
            ));
        }
        out
    }
}

impl Default for PhysicalPlan {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_plan() {
        let plan = PhysicalPlan::new();
        assert_eq!(plan.stream_count(), 0);
        assert!(plan.summary().contains("0 streams"));
    }

    #[test]
    fn test_plan_with_streams() {
        let mut plan = PhysicalPlan::new();
        plan.add_stream(PhysicalStream {
            name: "Alerts".to_string(),
            operation_count: 3,
            operation_summary: "where → select → emit".to_string(),
            logical_id: 0,
            registered_event_types: vec!["SensorReading".to_string()],
        });
        assert_eq!(plan.stream_count(), 1);
        let summary = plan.summary();
        assert!(summary.contains("Alerts"));
        assert!(summary.contains("SensorReading"));
    }
}
