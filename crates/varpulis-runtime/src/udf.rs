//! User-defined functions with type signatures
//!
//! Provides traits for native Rust scalar and aggregate UDFs that bypass
//! the VPL interpreter for maximum performance.
//!
//! # Example
//!
//! ```rust,no_run
//! use varpulis_runtime::udf::{ScalarUDF, Signature, TypeConstraint};
//! use varpulis_core::{Type, Value};
//!
//! struct DoubleUdf;
//! impl ScalarUDF for DoubleUdf {
//!     fn name(&self) -> &str { "double" }
//!     fn signature(&self) -> Signature {
//!         Signature {
//!             input_types: vec![TypeConstraint::Numeric],
//!             return_type: Type::Float,
//!             variadic: None,
//!         }
//!     }
//!     fn evaluate(&self, args: &[Value]) -> Option<Value> {
//!         match &args[0] {
//!             Value::Int(i) => Some(Value::Float(*i as f64 * 2.0)),
//!             Value::Float(f) => Some(Value::Float(f * 2.0)),
//!             _ => None,
//!         }
//!     }
//! }
//! ```

use rustc_hash::FxHashMap;
use std::sync::Arc;
use varpulis_core::{Type, Value};

/// Type constraint for a function parameter.
#[derive(Debug, Clone)]
pub enum TypeConstraint {
    /// Must be exactly this type.
    Exact(Type),
    /// Must be a numeric type (Int or Float).
    Numeric,
    /// Any type is accepted.
    Any,
    /// Must be one of the listed types.
    OneOf(Vec<Type>),
}

impl TypeConstraint {
    /// Check whether a concrete type satisfies this constraint.
    pub fn matches(&self, ty: &Type) -> bool {
        match self {
            TypeConstraint::Exact(expected) => ty == expected,
            TypeConstraint::Numeric => ty.is_numeric(),
            TypeConstraint::Any => true,
            TypeConstraint::OneOf(types) => types.contains(ty),
        }
    }
}

/// Specification for variadic arguments.
#[derive(Debug, Clone)]
pub struct VariadicSpec {
    /// Minimum number of variadic arguments.
    pub min_args: usize,
    /// Type constraint for each variadic argument.
    pub arg_type: TypeConstraint,
}

/// Function signature describing inputs and output.
#[derive(Debug, Clone)]
pub struct Signature {
    /// Type constraints for each positional parameter.
    pub input_types: Vec<TypeConstraint>,
    /// Return type of the function.
    pub return_type: Type,
    /// Optional variadic specification (additional args beyond `input_types`).
    pub variadic: Option<VariadicSpec>,
}

/// Trait for native Rust scalar UDFs.
///
/// Scalar UDFs evaluate once per event, producing a single output value.
pub trait ScalarUDF: Send + Sync {
    /// Function name used in VPL source.
    fn name(&self) -> &str;
    /// Type signature for validation and optimization.
    fn signature(&self) -> Signature;
    /// Evaluate the function with the given arguments.
    fn evaluate(&self, args: &[Value]) -> Option<Value>;
}

/// Trait for native Rust aggregate UDFs.
///
/// Aggregate UDFs produce an [`Accumulator`] that collects values across
/// a window or group.
pub trait AggregateUDF: Send + Sync {
    /// Function name used in VPL source.
    fn name(&self) -> &str;
    /// Type signature for validation.
    fn signature(&self) -> Signature;
    /// Create a fresh accumulator instance.
    fn init(&self) -> Box<dyn Accumulator>;
}

/// Stateful accumulator for aggregate UDFs.
pub trait Accumulator: Send + Sync {
    /// Incorporate a new value.
    fn update(&mut self, value: &Value);
    /// Merge another accumulator's state into this one.
    fn merge(&mut self, other: &dyn Accumulator);
    /// Produce the final aggregate result.
    fn finish(&self) -> Value;
    /// Reset to the initial state.
    fn reset(&mut self);
    /// Clone this accumulator into a boxed trait object.
    fn clone_box(&self) -> Box<dyn Accumulator>;
}

/// Registry of native UDFs.
///
/// The engine checks this registry before falling through to VPL-interpreted
/// user functions, allowing native Rust implementations to take priority.
pub struct UdfRegistry {
    scalar_udfs: FxHashMap<String, Arc<dyn ScalarUDF>>,
    aggregate_udfs: FxHashMap<String, Arc<dyn AggregateUDF>>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self {
            scalar_udfs: FxHashMap::default(),
            aggregate_udfs: FxHashMap::default(),
        }
    }

    /// Register a scalar UDF.
    pub fn register_scalar(&mut self, udf: Arc<dyn ScalarUDF>) {
        self.scalar_udfs.insert(udf.name().to_string(), udf);
    }

    /// Register an aggregate UDF.
    pub fn register_aggregate(&mut self, udf: Arc<dyn AggregateUDF>) {
        self.aggregate_udfs.insert(udf.name().to_string(), udf);
    }

    /// Look up a scalar UDF by name.
    pub fn get_scalar(&self, name: &str) -> Option<&Arc<dyn ScalarUDF>> {
        self.scalar_udfs.get(name)
    }

    /// Look up an aggregate UDF by name.
    pub fn get_aggregate(&self, name: &str) -> Option<&Arc<dyn AggregateUDF>> {
        self.aggregate_udfs.get(name)
    }

    /// Validate a function call against registered UDF signatures.
    ///
    /// Returns `Ok(return_type)` if the call is valid, or `Err(reason)` otherwise.
    pub fn validate_call(&self, name: &str, arg_types: &[Type]) -> Result<Type, String> {
        if let Some(udf) = self.scalar_udfs.get(name) {
            let sig = udf.signature();
            validate_signature(&sig, arg_types)?;
            return Ok(sig.return_type);
        }
        if let Some(udf) = self.aggregate_udfs.get(name) {
            let sig = udf.signature();
            validate_signature(&sig, arg_types)?;
            return Ok(sig.return_type);
        }
        Err(format!("unknown UDF: {}", name))
    }

    /// Returns true if the registry has no registered UDFs.
    pub fn is_empty(&self) -> bool {
        self.scalar_udfs.is_empty() && self.aggregate_udfs.is_empty()
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_signature(sig: &Signature, arg_types: &[Type]) -> Result<(), String> {
    let required = sig.input_types.len();

    if let Some(variadic) = &sig.variadic {
        if arg_types.len() < required + variadic.min_args {
            return Err(format!(
                "expected at least {} arguments, got {}",
                required + variadic.min_args,
                arg_types.len()
            ));
        }
        // Validate required args
        for (i, constraint) in sig.input_types.iter().enumerate() {
            if !constraint.matches(&arg_types[i]) {
                return Err(format!(
                    "argument {} type mismatch: expected {:?}, got {:?}",
                    i, constraint, arg_types[i]
                ));
            }
        }
        // Validate variadic args
        for (i, ty) in arg_types[required..].iter().enumerate() {
            if !variadic.arg_type.matches(ty) {
                return Err(format!(
                    "variadic argument {} type mismatch: expected {:?}, got {:?}",
                    i, variadic.arg_type, ty
                ));
            }
        }
    } else {
        if arg_types.len() != required {
            return Err(format!(
                "expected {} arguments, got {}",
                required,
                arg_types.len()
            ));
        }
        for (i, constraint) in sig.input_types.iter().enumerate() {
            if !constraint.matches(&arg_types[i]) {
                return Err(format!(
                    "argument {} type mismatch: expected {:?}, got {:?}",
                    i, constraint, arg_types[i]
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DoubleUdf;
    impl ScalarUDF for DoubleUdf {
        fn name(&self) -> &str {
            "double"
        }
        fn signature(&self) -> Signature {
            Signature {
                input_types: vec![TypeConstraint::Numeric],
                return_type: Type::Float,
                variadic: None,
            }
        }
        fn evaluate(&self, args: &[Value]) -> Option<Value> {
            match &args[0] {
                Value::Int(i) => Some(Value::Float(*i as f64 * 2.0)),
                Value::Float(f) => Some(Value::Float(f * 2.0)),
                _ => None,
            }
        }
    }

    struct SumAccumulator {
        total: f64,
    }

    impl Accumulator for SumAccumulator {
        fn update(&mut self, value: &Value) {
            match value {
                Value::Int(i) => self.total += *i as f64,
                Value::Float(f) => self.total += f,
                _ => {}
            }
        }
        fn merge(&mut self, other: &dyn Accumulator) {
            // In practice we'd downcast; for testing just use finish()
            let val = other.finish();
            self.update(&val);
        }
        fn finish(&self) -> Value {
            Value::Float(self.total)
        }
        fn reset(&mut self) {
            self.total = 0.0;
        }
        fn clone_box(&self) -> Box<dyn Accumulator> {
            Box::new(SumAccumulator { total: self.total })
        }
    }

    struct CustomSumUdf;
    impl AggregateUDF for CustomSumUdf {
        fn name(&self) -> &str {
            "custom_sum"
        }
        fn signature(&self) -> Signature {
            Signature {
                input_types: vec![TypeConstraint::Numeric],
                return_type: Type::Float,
                variadic: None,
            }
        }
        fn init(&self) -> Box<dyn Accumulator> {
            Box::new(SumAccumulator { total: 0.0 })
        }
    }

    #[test]
    fn test_scalar_udf_evaluate() {
        let udf = DoubleUdf;
        assert_eq!(udf.evaluate(&[Value::Int(5)]), Some(Value::Float(10.0)));
        assert_eq!(udf.evaluate(&[Value::Float(2.5)]), Some(Value::Float(5.0)));
        assert_eq!(udf.evaluate(&[Value::Null]), None);
    }

    #[test]
    fn test_registry_lookup() {
        let mut registry = UdfRegistry::new();
        assert!(registry.is_empty());

        registry.register_scalar(Arc::new(DoubleUdf));
        registry.register_aggregate(Arc::new(CustomSumUdf));

        assert!(!registry.is_empty());
        assert!(registry.get_scalar("double").is_some());
        assert!(registry.get_aggregate("custom_sum").is_some());
        assert!(registry.get_scalar("unknown").is_none());
    }

    #[test]
    fn test_validate_call_valid() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(DoubleUdf));

        let result = registry.validate_call("double", &[Type::Int]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Float);
    }

    #[test]
    fn test_validate_call_wrong_type() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(DoubleUdf));

        let result = registry.validate_call("double", &[Type::Str]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_call_wrong_arity() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(DoubleUdf));

        let result = registry.validate_call("double", &[Type::Int, Type::Int]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_unknown_udf() {
        let registry = UdfRegistry::new();
        let result = registry.validate_call("nonexistent", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_accumulator_lifecycle() {
        let udf = CustomSumUdf;
        let mut acc = udf.init();

        acc.update(&Value::Int(10));
        acc.update(&Value::Float(5.5));
        assert_eq!(acc.finish(), Value::Float(15.5));

        let cloned = acc.clone_box();
        assert_eq!(cloned.finish(), Value::Float(15.5));

        acc.reset();
        assert_eq!(acc.finish(), Value::Float(0.0));
    }

    #[test]
    fn test_type_constraint_matches() {
        assert!(TypeConstraint::Any.matches(&Type::Int));
        assert!(TypeConstraint::Any.matches(&Type::Str));

        assert!(TypeConstraint::Numeric.matches(&Type::Int));
        assert!(TypeConstraint::Numeric.matches(&Type::Float));
        assert!(!TypeConstraint::Numeric.matches(&Type::Str));

        assert!(TypeConstraint::Exact(Type::Int).matches(&Type::Int));
        assert!(!TypeConstraint::Exact(Type::Int).matches(&Type::Float));

        assert!(TypeConstraint::OneOf(vec![Type::Int, Type::Str]).matches(&Type::Str));
        assert!(!TypeConstraint::OneOf(vec![Type::Int, Type::Str]).matches(&Type::Bool));
    }

    #[test]
    fn test_variadic_validation() {
        let mut registry = UdfRegistry::new();

        struct ConcatUdf;
        impl ScalarUDF for ConcatUdf {
            fn name(&self) -> &str {
                "concat"
            }
            fn signature(&self) -> Signature {
                Signature {
                    input_types: vec![],
                    return_type: Type::Str,
                    variadic: Some(VariadicSpec {
                        min_args: 1,
                        arg_type: TypeConstraint::Exact(Type::Str),
                    }),
                }
            }
            fn evaluate(&self, _args: &[Value]) -> Option<Value> {
                None
            }
        }

        registry.register_scalar(Arc::new(ConcatUdf));

        // Valid: 1 string arg
        assert!(registry.validate_call("concat", &[Type::Str]).is_ok());
        // Valid: 3 string args
        assert!(registry
            .validate_call("concat", &[Type::Str, Type::Str, Type::Str])
            .is_ok());
        // Invalid: 0 args
        assert!(registry.validate_call("concat", &[]).is_err());
        // Invalid: int arg
        assert!(registry.validate_call("concat", &[Type::Int]).is_err());
    }
}
