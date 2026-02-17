# Contributing to Varpulis

Thank you for your interest in contributing to Varpulis. This guide covers the workflow and conventions you need to follow.

## Getting Started

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/<your-fork>/varpulis.git
   cd varpulis
   ```

2. Install [Rust 1.85+](https://rustup.rs/):
   ```bash
   rustup update stable
   ```

3. Build the workspace:
   ```bash
   cargo build --workspace
   ```

4. Verify everything works:
   ```bash
   cargo test --workspace
   ```

For the Web UI (optional):
```bash
cd web-ui && npm ci && npm run type-check
```

## Code Style

All code must pass formatting and linting checks before merge. CI enforces both.

- **Format**: `cargo fmt --all`
- **Lint**: `cargo clippy --workspace --all-targets -- -D warnings`

Key conventions:
- Use `!x.is_empty()` instead of `x.len() > 0`
- Use `n.is_multiple_of(d)` instead of `n % d == 0`
- Use `for v in map.values()` instead of `for (_, v) in &map`
- Always run `cargo fmt --all` after `cargo clippy --fix` (clippy can reformat code)

## Testing

```bash
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p varpulis-runtime

# Run a specific test
cargo test -p varpulis-runtime test_name
```

### Test Levels

| Level | Location | How to Run |
|-------|----------|------------|
| **Unit** | Inline `#[cfg(test)]` modules | `cargo test --workspace` |
| **Integration** | `crates/*/tests/` | `cargo test --workspace` |
| **E2E** | `tests/e2e-*/` | Require a running cluster; see test READMEs |
| **Chaos** | `crates/varpulis-cluster/tests/chaos/` | `cargo test -p varpulis-cluster --test chaos -- --ignored --test-threads=1` |

Chaos tests are inherently flaky and run with `continue-on-error` in CI. They require a release binary built beforehand (`cargo build --release -p varpulis-cli`).

## Commit Conventions

Use [Conventional Commits](https://www.conventionalcommits.org/) with imperative mood:

```
feat(engine): add sliding window support
fix(parser): handle escaped quotes in string literals
refactor(runtime): extract connector trait into separate module
docs(guides): add SASE+ pattern examples
test(cluster): add Raft leader election tests
ci: add cargo-deny advisory check
perf(sase): optimize NFA state transitions
```

Scopes are optional but encouraged. Common scopes: `engine`, `parser`, `runtime`, `cluster`, `cli`, `lsp`, `mcp`, `zdd`, `web-ui`, `security`, `docs`.

## Pull Request Process

1. Branch from `main`. Use descriptive branch names (e.g., `feat/sliding-windows`, `fix/mqtt-reconnect`).
2. Keep each PR to **one logical change**. Split large features into incremental PRs.
3. Include tests for new functionality. Bug fixes should include a regression test.
4. All CI checks must pass:
   - `cargo check --workspace --all-targets`
   - `cargo test --workspace`
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo audit`
   - `cargo deny check`
   - Feature flag matrix (see below)
   - Web UI type-check and unit tests
   - Coverage threshold
5. Update relevant documentation if your change affects user-facing behavior.
6. Respond to review feedback with fixup commits; the maintainer will squash on merge.

## Running Benchmarks

Criterion-based benchmarks live in each crate's `benches/` directory.

```bash
# Run all benchmarks for the runtime crate
cargo bench -p varpulis-runtime

# Run a specific benchmark group
cargo bench -p varpulis-runtime -- hamlet

# Run PST forecast benchmarks
cargo bench -p varpulis-runtime -- pst
```

Comparison benchmarks against Apama are in `benchmarks/apama-comparison/`. See the README in that directory for setup instructions.

**Important**: Always validate benchmark results manually. Check return codes and verify output counts match expectations. A benchmark that errors instantly will report misleading throughput numbers.

## Running Fuzz Tests

Fuzz testing requires nightly Rust and `cargo-fuzz`:

```bash
rustup install nightly
cargo install cargo-fuzz
```

Available fuzz targets:

```bash
# Parser fuzzing
cd crates/varpulis-parser/fuzz
cargo +nightly fuzz list
cargo +nightly fuzz run <target> -- -max_total_time=300

# Runtime fuzzing
cd crates/varpulis-runtime/fuzz
cargo +nightly fuzz list
cargo +nightly fuzz run <target> -- -max_total_time=300
```

If a fuzzer finds a crash, the reproducing input is saved to `fuzz/artifacts/`. Include it in your bug report or fix PR.

## Architecture Overview

The workspace contains 8 Rust crates and a web frontend:

```
crates/
  varpulis-core/       AST, types, values, semantic validation
  varpulis-parser/     Pest PEG parser for the VPL language
  varpulis-runtime/    Execution engine, SASE+ pattern matching, Hamlet
                       trend aggregation, PST forecasting, connectors
  varpulis-cluster/    Coordinator/worker architecture, Raft consensus
  varpulis-cli/        CLI binary and REST API server
  varpulis-lsp/        Language Server Protocol implementation
  varpulis-mcp/        Model Context Protocol server
  varpulis-zdd/        Zero-suppressed Decision Diagrams (research)
web-ui/                Vue 3 + Vuetify 3 control plane dashboard
```

Data flows: **VPL source** -> `varpulis-parser` -> **AST** (`varpulis-core`) -> `varpulis-runtime` compiles to **RuntimeOps** -> engine executes against event streams via connectors.

The cluster layer (`varpulis-cluster`) coordinates multiple workers, handles pipeline group assignment, and uses Raft for leader election.

## Feature Flags

Connectors and optional subsystems are gated behind feature flags. CI tests each flag independently.

| Flag | Description |
|------|-------------|
| `mqtt` | MQTT connector (enabled by default) |
| `kafka` | Apache Kafka connector |
| `database` | PostgreSQL, MySQL, SQLite sinks |
| `redis` | Redis sink |
| `s3` | AWS S3 source/sink |
| `kinesis` | AWS Kinesis source/sink |
| `elasticsearch` | Elasticsearch sink |
| `raft` | Raft consensus for cluster mode |
| `persistent` | RocksDB state persistence |
| `k8s` | Kubernetes integration |

Build with specific flags:
```bash
cargo build --release --features kafka,database
cargo build --release --features all-connectors
```

When adding a new import, verify it is not used only inside `#[cfg(feature = "...")]` blocks before removing it -- CI tests all feature flag combinations and will catch the error.

## Coverage

The project enforces a **70% project-wide** coverage threshold and **60% patch** coverage on new code.

Generate a local coverage report:
```bash
# Install cargo-llvm-cov (one-time)
cargo install cargo-llvm-cov

# Generate LCOV report
cargo llvm-cov --workspace --lcov --output-path lcov.info

# Generate HTML report for browsing
cargo llvm-cov --workspace --html --open
```

Coverage is uploaded to Codecov on every push and PR. If your PR drops coverage below the threshold, add tests before requesting review.

## License

By contributing, you agree that your contributions will be dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at the user's option, with no additional terms or conditions.
