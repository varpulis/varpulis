//! WASM-sandboxed User-Defined Functions.
//!
//! Wraps SmartModule instances as scalar and aggregate UDFs, integrating with
//! the existing [`UdfRegistry`](crate::udf::UdfRegistry) so WASM UDFs are
//! callable from VPL expressions identically to native Rust UDFs.
//!
//! ## Safety Guarantees
//!
//! - **CPU**: Fuel-based instruction counting (configurable per call)
//! - **Memory**: Maximum WASM linear memory limit
//! - **Isolation**: Each UDF runs in its own WASM sandbox
//!
//! ## ABI
//!
//! WASM scalar UDF modules must export:
//!
//! | Export | Signature | Description |
//! |--------|-----------|-------------|
//! | `memory` | `Memory` | Linear memory |
//! | `sm_alloc` | `(i32) -> i32` | Allocate bytes |
//! | `sm_dealloc` | `(i32, i32) -> ()` | Free bytes |
//! | `sm_scalar_udf` | `(i32, i32) -> i64` | Args JSON in, result JSON out |

use std::sync::{Arc, Mutex};

use varpulis_core::Value;
use varpulis_wasm::smartmodule::{
    SmartModuleConfig, SmartModuleEngine, SmartModuleInstance, SmartModuleKind,
};

use crate::udf::{ScalarUDF, Signature, TypeConstraint};

/// A scalar UDF backed by a WASM SmartModule.
///
/// Thread-safe via `Mutex` on the `SmartModuleInstance`. This is acceptable
/// because UDF calls are single-threaded per pipeline context.
pub struct WasmScalarUDF {
    name: String,
    signature: Signature,
    instance: Arc<Mutex<SmartModuleInstance>>,
}

impl std::fmt::Debug for WasmScalarUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmScalarUDF")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl WasmScalarUDF {
    /// Create a new WASM scalar UDF from WASM bytes.
    ///
    /// # Arguments
    /// * `name` — Function name (used in VPL expressions)
    /// * `wasm_bytes` — Compiled WASM module bytes (or WAT text)
    /// * `signature` — Type signature for validation
    /// * `max_fuel` — Maximum fuel (instruction count) per call
    pub fn new(
        name: &str,
        wasm_bytes: &[u8],
        signature: Signature,
        max_fuel: Option<u64>,
    ) -> Result<Self, String> {
        let engine = SmartModuleEngine::new().map_err(|e| e.to_string())?;
        let config = SmartModuleConfig {
            kind: SmartModuleKind::ScalarUdf,
            max_fuel,
            ..Default::default()
        };
        let instance = engine
            .instantiate(wasm_bytes, config)
            .map_err(|e| e.to_string())?;

        Ok(Self {
            name: name.to_string(),
            signature,
            instance: Arc::new(Mutex::new(instance)),
        })
    }

    /// Create from an already-instantiated SmartModule.
    pub fn from_instance(name: &str, signature: Signature, instance: SmartModuleInstance) -> Self {
        Self {
            name: name.to_string(),
            signature,
            instance: Arc::new(Mutex::new(instance)),
        }
    }
}

impl ScalarUDF for WasmScalarUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> Signature {
        self.signature.clone()
    }

    fn evaluate(&self, args: &[Value]) -> Option<Value> {
        let mut instance = self.instance.lock().ok()?;
        match instance.call_scalar(args) {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::warn!("WASM UDF '{}' failed: {e}", self.name);
                None
            }
        }
    }
}

/// Helper to build a [`Signature`] for a WASM UDF with the given parameter
/// count and return type. All parameters accept any type (the WASM module
/// handles the actual type logic).
pub fn any_signature(param_count: usize, return_type: varpulis_core::Type) -> Signature {
    Signature {
        input_types: vec![TypeConstraint::Any; param_count],
        return_type,
        variadic: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WAT module: scalar UDF that returns its first argument unchanged (identity).
    /// Reads the JSON array input, and returns the first element.
    /// For simplicity, this identity module just returns the whole input as-is.
    const SCALAR_IDENTITY: &str = r#"
        (module
            (memory (export "memory") 1)
            (global $offset (mut i32) (i32.const 1024))
            (func (export "sm_alloc") (param $size i32) (result i32)
                (local $ptr i32)
                (local.set $ptr (global.get $offset))
                (global.set $offset (i32.add (global.get $offset) (local.get $size)))
                (local.get $ptr)
            )
            (func (export "sm_dealloc") (param i32) (param i32))
            (func (export "sm_scalar_udf") (param $ptr i32) (param $len i32) (result i64)
                ;; Return the input unchanged (echo back the JSON args array)
                (i64.or
                    (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len))
                )
            )
        )
    "#;

    #[test]
    fn test_wasm_scalar_udf_creation() {
        let sig = any_signature(1, varpulis_core::Type::Any);
        let udf = WasmScalarUDF::new("echo", SCALAR_IDENTITY.as_bytes(), sig, Some(10_000_000));
        assert!(udf.is_ok());
        let udf = udf.unwrap();
        assert_eq!(udf.name(), "echo");
    }

    #[test]
    fn test_wasm_scalar_udf_evaluate() {
        let sig = any_signature(1, varpulis_core::Type::Any);
        let udf =
            WasmScalarUDF::new("echo", SCALAR_IDENTITY.as_bytes(), sig, Some(10_000_000)).unwrap();

        // The identity module returns the args array as-is, which is a JSON array
        let args = vec![Value::Float(42.0)];
        let result = udf.evaluate(&args);
        // The module echoes the JSON args array back, so we get an Array value
        assert!(result.is_some(), "UDF should return a value");
    }

    #[test]
    fn test_wasm_scalar_udf_fuel_exhaustion() {
        // WAT with infinite loop
        let infinite_loop = r#"
            (module
                (memory (export "memory") 1)
                (global $offset (mut i32) (i32.const 1024))
                (func (export "sm_alloc") (param $size i32) (result i32)
                    (local $ptr i32)
                    (local.set $ptr (global.get $offset))
                    (global.set $offset (i32.add (global.get $offset) (local.get $size)))
                    (local.get $ptr)
                )
                (func (export "sm_dealloc") (param i32) (param i32))
                (func (export "sm_scalar_udf") (param $ptr i32) (param $len i32) (result i64)
                    (loop $spin (br $spin))
                    (i64.const 0)
                )
            )
        "#;

        let sig = any_signature(1, varpulis_core::Type::Any);
        let udf = WasmScalarUDF::new("looper", infinite_loop.as_bytes(), sig, Some(1_000)).unwrap();
        let result = udf.evaluate(&[Value::Int(1)]);
        assert!(result.is_none(), "Fuel-exhausted UDF should return None");
    }

    #[test]
    fn test_wasm_scalar_udf_in_registry() {
        let sig = any_signature(1, varpulis_core::Type::Any);
        let udf =
            WasmScalarUDF::new("echo", SCALAR_IDENTITY.as_bytes(), sig, Some(10_000_000)).unwrap();

        let mut registry = crate::udf::UdfRegistry::new();
        registry.register_scalar(Arc::new(udf));
        assert!(registry.get_scalar("echo").is_some());
    }
}
