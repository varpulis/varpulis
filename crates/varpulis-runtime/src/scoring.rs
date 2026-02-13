//! ONNX model scoring for `.score()` operator
//!
//! When the `onnx` feature is enabled, provides `OnnxModel` which loads an ONNX model
//! and runs inference against event fields. Without the feature, a stub is provided
//! that returns an error at compile time (engine compilation, not Rust compilation).

#[cfg(feature = "onnx")]
mod inner {
    use crate::event::Event;
    use ort::{session::Session, value::Tensor};
    use std::sync::Arc;

    /// An ONNX model loaded into ONNX Runtime for per-event inference.
    pub struct OnnxModel {
        session: Arc<std::sync::Mutex<Session>>,
        input_name: String,
        pub input_fields: Vec<String>,
        pub output_fields: Vec<String>,
    }

    // SAFETY: Session is Send+Sync, wrapped in Arc<Mutex> for mutable run()
    unsafe impl Send for OnnxModel {}
    unsafe impl Sync for OnnxModel {}

    impl OnnxModel {
        /// Load an ONNX model from a file path.
        pub fn load(path: &str, inputs: Vec<String>, outputs: Vec<String>) -> Result<Self, String> {
            let session = Session::builder()
                .map_err(|e| format!("ONNX session builder error: {}", e))?
                .commit_from_file(path)
                .map_err(|e| format!("Failed to load ONNX model '{}': {}", path, e))?;

            // Read the first input tensor name from the model
            let input_name = session
                .inputs()
                .first()
                .map(|i| i.name().to_string())
                .unwrap_or_else(|| "input".to_string());

            Ok(OnnxModel {
                session: Arc::new(std::sync::Mutex::new(session)),
                input_name,
                input_fields: inputs,
                output_fields: outputs,
            })
        }

        /// Run inference on a single event, returning `(field_name, value)` pairs.
        pub fn infer(&self, event: &Event) -> Result<Vec<(String, f64)>, String> {
            let n = self.input_fields.len();
            let mut input_data = Vec::with_capacity(n);

            for field in &self.input_fields {
                let val = if let Some(f) = event.get_float(field) {
                    f as f32
                } else if let Some(i) = event.get_int(field) {
                    i as f32
                } else {
                    return Err(format!("Missing input field '{}' in event", field));
                };
                input_data.push(val);
            }

            // Use (shape, data) tuple form — compatible with ort's OwnedTensorArrayData
            let input_tensor = Tensor::from_array((vec![1_i64, n as i64], input_data))
                .map_err(|e| format!("Tensor creation error: {}", e))?;

            let mut session = self
                .session
                .lock()
                .map_err(|e| format!("Session lock error: {}", e))?;

            let outputs = session
                .run(ort::inputs![self.input_name.as_str() => input_tensor])
                .map_err(|e| format!("ONNX inference error: {}", e))?;

            // Index the first output tensor by position
            let output_value = &outputs[0];

            let (_, raw_data) = output_value
                .try_extract_tensor::<f32>()
                .map_err(|e| format!("Output tensor extract error: {}", e))?;

            let mut results = Vec::with_capacity(self.output_fields.len());
            for (i, field) in self.output_fields.iter().enumerate() {
                let val = raw_data.get(i).copied().unwrap_or(0.0f32);
                results.push((field.clone(), val as f64));
            }

            Ok(results)
        }
    }
}

#[cfg(not(feature = "onnx"))]
mod inner {
    use crate::event::Event;

    /// Stub `OnnxModel` when the `onnx` feature is not enabled.
    pub struct OnnxModel {
        pub input_fields: Vec<String>,
        pub output_fields: Vec<String>,
    }

    impl OnnxModel {
        pub fn load(
            _path: &str,
            _inputs: Vec<String>,
            _outputs: Vec<String>,
        ) -> Result<Self, String> {
            Err(
                ".score() requires the 'onnx' feature — rebuild with: cargo build --features onnx"
                    .to_string(),
            )
        }

        pub fn infer(&self, _event: &Event) -> Result<Vec<(String, f64)>, String> {
            Err(".score() requires the 'onnx' feature".to_string())
        }
    }
}

pub use inner::OnnxModel;
