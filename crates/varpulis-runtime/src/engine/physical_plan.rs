//! Physical plan: the bridge between logical plans and runtime execution
//!
//! A `PhysicalPlan` wraps existing `StreamDefinition` + `Vec<RuntimeOp>` structures,
//! correlating each physical stream back to its logical plan node.

use super::types::StreamDefinition;

/// Physical plan produced by materializing a logical plan.
///
/// Each stream carries a `StreamDefinition` (which contains the actual
/// `Vec<RuntimeOp>`) plus metadata for correlation and debugging.
pub(crate) struct PhysicalPlan {
    pub streams: Vec<PhysicalStream>,
}

/// A single physical stream in the plan.
pub(crate) struct PhysicalStream {
    /// The runtime stream definition containing operations, SASE engine, etc.
    pub definition: StreamDefinition,
    /// Correlation ID back to the logical plan stream
    pub logical_id: u32,
    /// Event types this stream is registered to receive
    pub registered_event_types: Vec<String>,
}

impl PhysicalPlan {
    /// Create a new empty physical plan.
    pub fn new() -> Self {
        Self {
            streams: Vec::new(),
        }
    }

    /// Add a physical stream to the plan.
    pub fn add_stream(&mut self, stream: PhysicalStream) {
        self.streams.push(stream);
    }

    /// Get the number of streams in the plan.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Get a summary of the physical plan for debugging.
    pub fn summary(&self) -> String {
        let mut out = format!("PhysicalPlan ({} streams):\n", self.streams.len());
        for s in &self.streams {
            out.push_str(&format!(
                "  [logical={}] {} — {} ops, {} event types\n",
                s.logical_id,
                s.definition.name,
                s.definition.operations.len(),
                s.registered_event_types.len(),
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
}
