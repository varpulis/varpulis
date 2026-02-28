# ADR-010: Declarative Component Registration

**Status:** Accepted
**Date:** 2026-02-28
**Authors:** Varpulis Team

## Context

Adding a new connector to Varpulis requires editing three separate match-arm
dispatch tables:

1. `managed_registry.rs` — `create_managed()`: creates supervised connectors
2. `registry.rs` — `create_from_config()`: creates legacy sink connectors
3. `sink_factory.rs` — `create_sink_from_config()`: creates engine sinks

Each of these contains 3–14 match arms plus a wildcard fallback. Feature-gated
connectors require `#[cfg(feature = ...)]` on each arm in each file, tripling
the maintenance burden.

This pattern is error-prone: forgetting to add a match arm in one of the three
files creates a silent failure (the connector appears to not exist for that
code path). It also makes it impossible to enumerate available connectors at
runtime for a registry browser UI.

## Decision

Use the `inventory` crate for zero-boilerplate global static registration.

### Why `inventory`

- **Zero boilerplate**: A single `inventory::submit!` macro call per connector
- **Feature-flag compatible**: `#[cfg(feature = ...)]` on the module naturally
  gates the registration — no separate conditional logic needed
- **No serde dependency**: Unlike `typetag`, `inventory` doesn't require
  `Serialize`/`Deserialize` on the trait (which would conflict with our
  `SinkConnector` trait's `async` methods)
- **Mature**: Used by `tracing`, `linkme`, and other production crates
- **Tiny**: Adds ~200 lines of compiled code

### Architecture

```rust
// connector/component.rs
pub trait ConnectorFactory: Send + Sync {
    fn info(&self) -> &ConnectorComponentInfo;
    fn create_managed(...) -> Result<Box<dyn ManagedConnector>, ConnectorError>;
    fn create_sink_connector(...) -> Result<Box<dyn SinkConnector>, ConnectorError>;
    fn create_engine_sink(...) -> Result<Arc<dyn Sink>, ConnectorError>;
}

inventory::collect!(&'static dyn ConnectorFactory);

pub fn find_factory(connector_type: &str) -> Option<&'static dyn ConnectorFactory>;
pub fn list_components() -> Vec<&'static ConnectorComponentInfo>;
```

### Per-connector registration

Each connector module adds a static factory:

```rust
// connector/console.rs
static CONSOLE_INFO: ConnectorComponentInfo = ConnectorComponentInfo { ... };
struct ConsoleFactory;
impl ConnectorFactory for ConsoleFactory { ... }
inventory::submit! { &ConsoleFactory as &dyn ConnectorFactory }
```

### Migration strategy

Dispatch tables now try `find_factory()` first and fall back to the existing
match arms. This allows incremental migration: connectors can be registered
one at a time, and the match arms can be removed once all connectors are
registered.

## Alternatives Considered

1. **`typetag` crate**: Provides automatic serde-based dispatch. Rejected
   because it requires `Serialize`/`Deserialize` on the trait object, which
   conflicts with our `async_trait` sink/source traits and would require
   restructuring the connector trait hierarchy.

2. **Derive macro (`#[connector(...)]`)**: A custom proc macro could generate
   registration code. Rejected because the macro would need to understand
   feature flags, async constructors, and three different factory methods —
   resulting in a complex macro for limited benefit over `inventory::submit!`.

3. **Centralized registry with `LinkMe`**: Similar to inventory but uses
   linker sections. Rejected because `inventory` has broader platform support
   and a simpler API.

4. **Keep match arms, add a lint**: A clippy-like lint could check that all
   connectors appear in all three dispatch tables. Rejected because it doesn't
   solve the runtime enumeration problem (no `list_components()`).

## Consequences

### Positive

- New connectors require editing only their own module file
- Runtime enumeration enables a connector browser UI
- Feature flags are handled naturally by the module system
- `ConnectorComponentInfo` provides structured metadata for documentation

### Negative

- `inventory` uses `ctor` for static initialization, which may not work on all
  exotic targets (all supported platforms are fine)
- Factory trait has three methods with default "not supported" implementations;
  connectors must override the right ones
- Legacy match arms remain during migration period (code duplication)

## References

- `crates/varpulis-runtime/src/connector/component.rs` — Factory trait + inventory
- `inventory` crate: https://docs.rs/inventory
- `crates/varpulis-runtime/Cargo.toml` — `inventory = "0.3"` dependency
