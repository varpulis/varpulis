//! SmartModule execution engine backed by wasmtime.

use varpulis_core::Event;
use wasmtime::*;

use super::error::SmartModuleError;
use super::SmartModuleKind;

/// Configuration for loading and running a SmartModule.
#[derive(Debug, Clone)]
pub struct SmartModuleConfig {
    /// Kind of processing.
    pub kind: SmartModuleKind,
    /// Maximum fuel (instruction count) per invocation. `None` = unlimited.
    pub max_fuel: Option<u64>,
    /// Maximum linear memory in bytes (default: 16 MiB).
    pub max_memory_bytes: usize,
}

impl Default for SmartModuleConfig {
    fn default() -> Self {
        Self {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(1_000_000),
            max_memory_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Engine for compiling and instantiating SmartModules.
///
/// A single engine can be shared across multiple module instances.
/// It holds the JIT compiler configuration and code cache.
pub struct SmartModuleEngine {
    engine: Engine,
}

impl std::fmt::Debug for SmartModuleEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmartModuleEngine").finish_non_exhaustive()
    }
}

impl SmartModuleEngine {
    /// Create a new SmartModule engine with fuel metering enabled.
    pub fn new() -> Result<Self, SmartModuleError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine =
            Engine::new(&config).map_err(|e| SmartModuleError::Compilation(e.to_string()))?;
        Ok(Self { engine })
    }

    /// Compile and instantiate a SmartModule from WASM bytes or WAT text.
    pub fn instantiate(
        &self,
        wasm_bytes: &[u8],
        config: SmartModuleConfig,
    ) -> Result<SmartModuleInstance, SmartModuleError> {
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| SmartModuleError::Compilation(e.to_string()))?;

        let mut store = Store::new(&self.engine, ());

        // Set initial fuel budget
        let fuel = config.max_fuel.unwrap_or(u64::MAX / 2);
        store
            .set_fuel(fuel)
            .map_err(|e: Error| SmartModuleError::Execution(e.to_string()))?;

        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|e| SmartModuleError::Execution(e.to_string()))?;

        // Get required exports
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| SmartModuleError::MissingExport("memory".into()))?;

        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "sm_alloc")
            .map_err(|e| SmartModuleError::InvalidSignature {
                name: "sm_alloc".into(),
                reason: e.to_string(),
            })?;

        let dealloc = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "sm_dealloc")
            .map_err(|e| SmartModuleError::InvalidSignature {
                name: "sm_dealloc".into(),
                reason: e.to_string(),
            })?;

        // Get the kind-specific function
        // Get scalar UDF function (optional)
        let scalar_udf_fn = if config.kind == SmartModuleKind::ScalarUdf {
            Some(
                instance
                    .get_typed_func::<(i32, i32), i64>(&mut store, "sm_scalar_udf")
                    .map_err(|e| SmartModuleError::InvalidSignature {
                        name: "sm_scalar_udf".into(),
                        reason: e.to_string(),
                    })?,
            )
        } else {
            // Try to get it optionally (module might export it even if not the primary kind)
            instance
                .get_typed_func::<(i32, i32), i64>(&mut store, "sm_scalar_udf")
                .ok()
        };

        let filter_fn = if config.kind == SmartModuleKind::Filter {
            Some(
                instance
                    .get_typed_func::<(i32, i32), i32>(&mut store, "sm_filter")
                    .map_err(|e| SmartModuleError::InvalidSignature {
                        name: "sm_filter".into(),
                        reason: e.to_string(),
                    })?,
            )
        } else {
            None
        };

        let map_fn = if config.kind == SmartModuleKind::Map {
            Some(
                instance
                    .get_typed_func::<(i32, i32), i64>(&mut store, "sm_map")
                    .map_err(|e| SmartModuleError::InvalidSignature {
                        name: "sm_map".into(),
                        reason: e.to_string(),
                    })?,
            )
        } else {
            None
        };

        Ok(SmartModuleInstance {
            store,
            kind: config.kind,
            max_fuel: config.max_fuel,
            max_memory_bytes: config.max_memory_bytes,
            alloc,
            dealloc,
            filter_fn,
            map_fn,
            scalar_udf_fn,
            memory,
        })
    }
}

/// A loaded and ready-to-execute SmartModule instance.
pub struct SmartModuleInstance {
    store: Store<()>,
    kind: SmartModuleKind,
    max_fuel: Option<u64>,
    max_memory_bytes: usize,
    alloc: TypedFunc<i32, i32>,
    dealloc: TypedFunc<(i32, i32), ()>,
    filter_fn: Option<TypedFunc<(i32, i32), i32>>,
    map_fn: Option<TypedFunc<(i32, i32), i64>>,
    scalar_udf_fn: Option<TypedFunc<(i32, i32), i64>>,
    memory: Memory,
}

impl std::fmt::Debug for SmartModuleInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmartModuleInstance")
            .field("kind", &self.kind)
            .field("max_fuel", &self.max_fuel)
            .field("max_memory_bytes", &self.max_memory_bytes)
            .finish_non_exhaustive()
    }
}

impl SmartModuleInstance {
    /// Filter an event. Returns `true` to keep, `false` to drop.
    pub fn filter(&mut self, event: &Event) -> Result<bool, SmartModuleError> {
        let filter_fn =
            self.filter_fn
                .as_ref()
                .ok_or_else(|| SmartModuleError::InvalidSignature {
                    name: "sm_filter".into(),
                    reason: "module is not a SmartFilter".into(),
                })?;
        // Clone to avoid borrow conflict with self.store
        let filter_fn = filter_fn.clone();

        let json = serde_json::to_vec(event)
            .map_err(|e| SmartModuleError::Serialization(e.to_string()))?;

        let (ptr, len) = self.write_to_guest(&json)?;
        self.prepare_fuel()?;

        let result = filter_fn.call(&mut self.store, (ptr, len)).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("fuel") {
                SmartModuleError::FuelExhausted {
                    limit: self.max_fuel.unwrap_or(0),
                }
            } else {
                SmartModuleError::Execution(msg)
            }
        })?;

        let _ = self.dealloc.call(&mut self.store, (ptr, len));
        Ok(result != 0)
    }

    /// Map/transform an event. Returns a new event.
    pub fn map(&mut self, event: &Event) -> Result<Event, SmartModuleError> {
        let map_fn = self
            .map_fn
            .as_ref()
            .ok_or_else(|| SmartModuleError::InvalidSignature {
                name: "sm_map".into(),
                reason: "module is not a SmartMap".into(),
            })?;
        // Clone to avoid borrow conflict with self.store
        let map_fn = map_fn.clone();

        let json = serde_json::to_vec(event)
            .map_err(|e| SmartModuleError::Serialization(e.to_string()))?;

        let (ptr, len) = self.write_to_guest(&json)?;
        self.prepare_fuel()?;

        let result = map_fn.call(&mut self.store, (ptr, len)).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("fuel") {
                SmartModuleError::FuelExhausted {
                    limit: self.max_fuel.unwrap_or(0),
                }
            } else {
                SmartModuleError::Execution(msg)
            }
        })?;

        // Deallocate input
        let _ = self.dealloc.call(&mut self.store, (ptr, len));

        // Extract output pointer and length from packed i64 result
        let out_ptr = (result >> 32) as i32;
        let out_len = (result & 0xFFFF_FFFF) as i32;

        let output = self.read_from_guest(out_ptr, out_len)?;
        let _ = self.dealloc.call(&mut self.store, (out_ptr, out_len));

        serde_json::from_slice(&output).map_err(|e| SmartModuleError::Serialization(e.to_string()))
    }

    /// Call a scalar UDF with the given arguments.
    ///
    /// Arguments are serialized as a JSON array `[arg1, arg2, ...]`.
    /// The WASM module returns a single JSON-encoded value.
    pub fn call_scalar(
        &mut self,
        args: &[varpulis_core::Value],
    ) -> Result<varpulis_core::Value, SmartModuleError> {
        let scalar_fn =
            self.scalar_udf_fn
                .as_ref()
                .ok_or_else(|| SmartModuleError::InvalidSignature {
                    name: "sm_scalar_udf".into(),
                    reason: "module does not export sm_scalar_udf".into(),
                })?;
        let scalar_fn = scalar_fn.clone();

        let json =
            serde_json::to_vec(args).map_err(|e| SmartModuleError::Serialization(e.to_string()))?;

        let (ptr, len) = self.write_to_guest(&json)?;
        self.prepare_fuel()?;

        let result = scalar_fn.call(&mut self.store, (ptr, len)).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("fuel") {
                SmartModuleError::FuelExhausted {
                    limit: self.max_fuel.unwrap_or(0),
                }
            } else {
                SmartModuleError::Execution(msg)
            }
        })?;

        let _ = self.dealloc.call(&mut self.store, (ptr, len));

        // Extract output pointer and length from packed i64 result
        let out_ptr = (result >> 32) as i32;
        let out_len = (result & 0xFFFF_FFFF) as i32;

        let output = self.read_from_guest(out_ptr, out_len)?;
        let _ = self.dealloc.call(&mut self.store, (out_ptr, out_len));

        serde_json::from_slice(&output).map_err(|e| SmartModuleError::Serialization(e.to_string()))
    }

    /// Get the kind of this SmartModule.
    pub fn kind(&self) -> SmartModuleKind {
        self.kind
    }

    /// Reset fuel budget before each call.
    fn prepare_fuel(&mut self) -> Result<(), SmartModuleError> {
        if let Some(limit) = self.max_fuel {
            self.store
                .set_fuel(limit)
                .map_err(|e: Error| SmartModuleError::Execution(e.to_string()))?;
        }
        Ok(())
    }

    /// Write bytes into guest linear memory via `sm_alloc`.
    fn write_to_guest(&mut self, data: &[u8]) -> Result<(i32, i32), SmartModuleError> {
        let len = data.len() as i32;

        let current_size = self.memory.data_size(&self.store);
        if current_size + data.len() > self.max_memory_bytes {
            return Err(SmartModuleError::MemoryLimitExceeded {
                requested: current_size + data.len(),
                limit: self.max_memory_bytes,
            });
        }

        let ptr = self
            .alloc
            .call(&mut self.store, len)
            .map_err(|e| SmartModuleError::Execution(format!("sm_alloc failed: {e}")))?;

        let mem_data = self.memory.data_mut(&mut self.store);
        let start = ptr as usize;
        let end = start + data.len();
        if end > mem_data.len() {
            return Err(SmartModuleError::Execution(
                "sm_alloc returned pointer beyond memory bounds".into(),
            ));
        }
        mem_data[start..end].copy_from_slice(data);

        Ok((ptr, len))
    }

    /// Read bytes from guest linear memory.
    fn read_from_guest(&self, ptr: i32, len: i32) -> Result<Vec<u8>, SmartModuleError> {
        let mem_data = self.memory.data(&self.store);
        let start = ptr as usize;
        let end = start + len as usize;
        if end > mem_data.len() {
            return Err(SmartModuleError::Execution(
                "output pointer beyond memory bounds".into(),
            ));
        }
        Ok(mem_data[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use varpulis_core::Event;

    use super::*;

    /// WAT module: filter that keeps all events.
    const FILTER_KEEP_ALL: &str = r#"
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
            (func (export "sm_filter") (param $ptr i32) (param $len i32) (result i32)
                (i32.const 1)
            )
        )
    "#;

    /// WAT module: filter that drops all events.
    const FILTER_DROP_ALL: &str = r#"
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
            (func (export "sm_filter") (param $ptr i32) (param $len i32) (result i32)
                (i32.const 0)
            )
        )
    "#;

    /// WAT module: map that returns input unchanged (identity).
    const MAP_IDENTITY: &str = r#"
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
            (func (export "sm_map") (param $ptr i32) (param $len i32) (result i64)
                ;; Return same (ptr, len) packed as i64
                (i64.or
                    (i64.shl
                        (i64.extend_i32_u (local.get $ptr))
                        (i64.const 32)
                    )
                    (i64.extend_i32_u (local.get $len))
                )
            )
        )
    "#;

    /// WAT module: filter with infinite loop (for fuel exhaustion testing).
    const FILTER_INFINITE_LOOP: &str = r#"
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
            (func (export "sm_filter") (param $ptr i32) (param $len i32) (result i32)
                (loop $spin (br $spin))
                (i32.const 1)
            )
        )
    "#;

    fn test_event() -> Event {
        let mut e = Event::new("SensorReading");
        e.data
            .insert("temperature".into(), varpulis_core::Value::Float(42.5));
        e.data
            .insert("sensor_id".into(), varpulis_core::Value::Str("S1".into()));
        e
    }

    #[test]
    fn filter_keep_all() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine
            .instantiate(FILTER_KEEP_ALL.as_bytes(), config)
            .unwrap();
        assert!(instance.filter(&test_event()).unwrap());
    }

    #[test]
    fn filter_drop_all() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine
            .instantiate(FILTER_DROP_ALL.as_bytes(), config)
            .unwrap();
        assert!(!instance.filter(&test_event()).unwrap());
    }

    #[test]
    fn map_identity() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Map,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine.instantiate(MAP_IDENTITY.as_bytes(), config).unwrap();
        let event = test_event();
        let mapped = instance.map(&event).unwrap();
        assert_eq!(mapped.event_type.as_ref(), "SensorReading");
        assert_eq!(
            mapped.data.get("temperature"),
            Some(&varpulis_core::Value::Float(42.5))
        );
    }

    #[test]
    fn fuel_exhaustion() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(1_000),
            ..Default::default()
        };
        let mut instance = engine
            .instantiate(FILTER_INFINITE_LOOP.as_bytes(), config)
            .unwrap();
        let result = instance.filter(&test_event());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SmartModuleError::FuelExhausted { .. })
                || matches!(err, SmartModuleError::Execution(_)),
            "expected fuel error, got: {err}"
        );
    }

    #[test]
    fn missing_export() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            ..Default::default()
        };
        // Module with memory but no sm_filter
        let wat = r#"
            (module
                (memory (export "memory") 1)
                (func (export "sm_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "sm_dealloc") (param i32) (param i32))
            )
        "#;
        let result = engine.instantiate(wat.as_bytes(), config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SmartModuleError::InvalidSignature { .. }),
            "expected InvalidSignature, got: {err}"
        );
    }

    #[test]
    fn missing_memory() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            ..Default::default()
        };
        let wat = r#"
            (module
                (func (export "sm_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "sm_dealloc") (param i32) (param i32))
                (func (export "sm_filter") (param i32) (param i32) (result i32) (i32.const 1))
            )
        "#;
        let result = engine.instantiate(wat.as_bytes(), config);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SmartModuleError::MissingExport(_)
        ));
    }

    #[test]
    fn filter_multiple_events() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine
            .instantiate(FILTER_KEEP_ALL.as_bytes(), config)
            .unwrap();

        for i in 0..100 {
            let mut e = Event::new("Tick");
            e.data.insert("seq".into(), varpulis_core::Value::Int(i));
            assert!(instance.filter(&e).unwrap());
        }
    }

    #[test]
    fn map_preserves_fields() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Map,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine.instantiate(MAP_IDENTITY.as_bytes(), config).unwrap();

        let mut event = Event::new("Trade");
        event
            .data
            .insert("price".into(), varpulis_core::Value::Float(100.50));
        event
            .data
            .insert("symbol".into(), varpulis_core::Value::Str("AAPL".into()));
        event
            .data
            .insert("quantity".into(), varpulis_core::Value::Int(500));

        let mapped = instance.map(&event).unwrap();
        assert_eq!(mapped.event_type.as_ref(), "Trade");
        assert_eq!(mapped.data.len(), 3);
        assert_eq!(
            mapped.data.get("price"),
            Some(&varpulis_core::Value::Float(100.50))
        );
        assert_eq!(
            mapped.data.get("symbol"),
            Some(&varpulis_core::Value::Str("AAPL".into()))
        );
    }

    #[test]
    fn wrong_kind_filter_on_map() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Map,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine.instantiate(MAP_IDENTITY.as_bytes(), config).unwrap();
        // Calling filter on a Map module should fail
        let result = instance.filter(&test_event());
        assert!(result.is_err());
    }

    #[test]
    fn wrong_kind_map_on_filter() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let mut instance = engine
            .instantiate(FILTER_KEEP_ALL.as_bytes(), config)
            .unwrap();
        // Calling map on a Filter module should fail
        let result = instance.map(&test_event());
        assert!(result.is_err());
    }

    #[test]
    fn engine_debug() {
        let engine = SmartModuleEngine::new().unwrap();
        let debug = format!("{engine:?}");
        assert!(debug.contains("SmartModuleEngine"));
    }

    #[test]
    fn instance_debug() {
        let engine = SmartModuleEngine::new().unwrap();
        let config = SmartModuleConfig {
            kind: SmartModuleKind::Filter,
            max_fuel: Some(10_000_000),
            ..Default::default()
        };
        let instance = engine
            .instantiate(FILTER_KEEP_ALL.as_bytes(), config)
            .unwrap();
        let debug = format!("{instance:?}");
        assert!(debug.contains("SmartModuleInstance"));
        assert!(debug.contains("Filter"));
    }

    #[test]
    fn config_default() {
        let config = SmartModuleConfig::default();
        assert_eq!(config.kind, SmartModuleKind::Filter);
        assert_eq!(config.max_fuel, Some(1_000_000));
        assert_eq!(config.max_memory_bytes, 16 * 1024 * 1024);
    }
}
