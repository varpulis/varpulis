//! ONNX model scoring for `.score()` operator
//!
//! When the `onnx` feature is enabled, provides `OnnxModel` which loads an ONNX model
//! and runs inference against event fields. Without the feature, a stub is provided
//! that returns an error at compile time (engine compilation, not Rust compilation).
//!
//! GPU acceleration is available when both the `gpu` and `onnx` features are enabled.
//! The `gpu` feature implies `onnx`, so enabling `gpu` is sufficient.

/// GPU execution provider configuration
#[derive(Debug, Clone, Default)]
pub enum GpuProvider {
    /// CPU execution (default)
    #[default]
    Cpu,
    /// CUDA GPU execution
    Cuda { device_id: i32 },
    /// TensorRT GPU execution (optimized inference)
    TensorRT { device_id: i32 },
}

/// GPU and batching configuration for ONNX inference
#[derive(Debug, Clone)]
pub struct GpuConfig {
    pub provider: GpuProvider,
    pub batch_size: usize,
}

impl Default for GpuConfig {
    fn default() -> Self {
        Self {
            provider: GpuProvider::Cpu,
            batch_size: 1,
        }
    }
}

#[cfg(feature = "onnx")]
mod inner {
    use super::{GpuConfig, GpuProvider};
    use crate::event::Event;
    use ort::{session::Session, value::Tensor};
    use std::sync::Arc;

    /// An ONNX model loaded into ONNX Runtime for per-event or batch inference.
    pub struct OnnxModel {
        session: Arc<std::sync::Mutex<Session>>,
        input_name: String,
        pub input_fields: Vec<String>,
        pub output_fields: Vec<String>,
        pub batch_size: usize,
    }

    impl std::fmt::Debug for OnnxModel {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OnnxModel")
                .field("input_name", &self.input_name)
                .field("input_fields", &self.input_fields)
                .field("output_fields", &self.output_fields)
                .field("batch_size", &self.batch_size)
                .finish_non_exhaustive()
        }
    }

    impl OnnxModel {
        /// Load an ONNX model from a file path with optional GPU configuration.
        pub fn load(
            path: &str,
            inputs: Vec<String>,
            outputs: Vec<String>,
            gpu_config: Option<GpuConfig>,
        ) -> Result<Self, String> {
            let mut builder =
                Session::builder().map_err(|e| format!("ONNX session builder error: {}", e))?;

            // Configure execution provider based on GPU settings
            if let Some(ref config) = gpu_config {
                match &config.provider {
                    GpuProvider::Cpu => {
                        // Default CPU provider, no extra config needed
                    }
                    GpuProvider::Cuda { device_id } => {
                        builder = builder
                            .with_execution_providers([
                                ort::execution_providers::CUDAExecutionProvider::default()
                                    .with_device_id(*device_id)
                                    .build(),
                            ])
                            .map_err(|e| format!("CUDA provider error: {}", e))?;
                    }
                    GpuProvider::TensorRT { device_id } => {
                        builder = builder
                            .with_execution_providers([
                                ort::execution_providers::TensorRTExecutionProvider::default()
                                    .with_device_id(*device_id)
                                    .build(),
                            ])
                            .map_err(|e| format!("TensorRT provider error: {}", e))?;
                    }
                }
            }

            let session = builder
                .commit_from_file(path)
                .map_err(|e| format!("Failed to load ONNX model '{}': {}", path, e))?;

            // Read the first input tensor name from the model
            let input_name = session
                .inputs()
                .first()
                .map(|i| i.name().to_string())
                .unwrap_or_else(|| "input".to_string());

            let batch_size = gpu_config.as_ref().map(|c| c.batch_size).unwrap_or(1);

            Ok(OnnxModel {
                session: Arc::new(std::sync::Mutex::new(session)),
                input_name,
                input_fields: inputs,
                output_fields: outputs,
                batch_size,
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

        /// Run batch inference on multiple events, returning per-event results.
        ///
        /// Constructs a [batch_size, features] tensor and runs a single inference call.
        /// More efficient than calling `infer()` per event when GPU is enabled.
        pub fn infer_batch(&self, events: &[&Event]) -> Result<Vec<Vec<(String, f64)>>, String> {
            if events.is_empty() {
                return Ok(Vec::new());
            }

            let n_features = self.input_fields.len();
            let batch = events.len();
            let mut input_data = Vec::with_capacity(batch * n_features);

            for event in events {
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
            }

            let input_tensor =
                Tensor::from_array((vec![batch as i64, n_features as i64], input_data))
                    .map_err(|e| format!("Batch tensor creation error: {}", e))?;

            let mut session = self
                .session
                .lock()
                .map_err(|e| format!("Session lock error: {}", e))?;

            let outputs = session
                .run(ort::inputs![self.input_name.as_str() => input_tensor])
                .map_err(|e| format!("ONNX batch inference error: {}", e))?;

            let output_value = &outputs[0];
            let (_, raw_data) = output_value
                .try_extract_tensor::<f32>()
                .map_err(|e| format!("Batch output tensor extract error: {}", e))?;

            let n_outputs = self.output_fields.len();
            let mut batch_results = Vec::with_capacity(batch);

            for i in 0..batch {
                let mut results = Vec::with_capacity(n_outputs);
                for (j, field) in self.output_fields.iter().enumerate() {
                    let idx = i * n_outputs + j;
                    let val = raw_data.get(idx).copied().unwrap_or(0.0f32);
                    results.push((field.clone(), val as f64));
                }
                batch_results.push(results);
            }

            Ok(batch_results)
        }
    }
}

#[cfg(not(feature = "onnx"))]
mod inner {
    use super::GpuConfig;
    use crate::event::Event;

    /// Stub `OnnxModel` when the `onnx` feature is not enabled.
    #[derive(Debug)]
    pub struct OnnxModel {
        pub input_fields: Vec<String>,
        pub output_fields: Vec<String>,
        pub batch_size: usize,
    }

    impl OnnxModel {
        pub fn load(
            _path: &str,
            _inputs: Vec<String>,
            _outputs: Vec<String>,
            _gpu_config: Option<GpuConfig>,
        ) -> Result<Self, String> {
            Err(
                ".score() requires the 'onnx' feature — rebuild with: cargo build --features onnx"
                    .to_string(),
            )
        }

        pub fn infer(&self, _event: &Event) -> Result<Vec<(String, f64)>, String> {
            Err(".score() requires the 'onnx' feature".to_string())
        }

        pub fn infer_batch(&self, _events: &[&Event]) -> Result<Vec<Vec<(String, f64)>>, String> {
            Err(".score() requires the 'onnx' feature".to_string())
        }
    }
}

pub use inner::OnnxModel;
