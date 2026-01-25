# Actions Immédiates - Sprint 1
## Semaine du 27 Janvier 2026

---

## Objectif Sprint
> Atteindre 70% de couverture et avoir des sinks fonctionnels

---

## Tâches Prioritaires

### 1. Fix Warnings Rust [0.5 jour]
**Priorité**: HAUTE  
**Fichiers concernés**:
- `crates/varpulis-core/src/ast.rs` - unused import `Span`
- `crates/varpulis-core/src/span.rs` - unused `DeserializeOwned`
- `crates/varpulis-parser/src/parser.rs` - unused variables
- `crates/varpulis-runtime/src/attention.rs` - unused imports
- `crates/varpulis-runtime/src/connector.rs` - unused `Arc`

```bash
cargo fix --lib -p varpulis-core --allow-dirty
cargo fix --lib -p varpulis-parser --allow-dirty
cargo fix --lib -p varpulis-runtime --allow-dirty
cargo clippy --all-targets
```

### 2. Implémenter FileSink [2 jours]
**Priorité**: CRITIQUE  
**Fichier**: `crates/varpulis-runtime/src/sink.rs`

```rust
// Objectif: FileSink qui écrit des JSON Lines
pub struct FileSink {
    path: PathBuf,
    file: Option<BufWriter<File>>,
    buffer_size: usize,
}

impl Sink for FileSink {
    async fn send(&mut self, alert: &Alert) -> Result<()> {
        let line = serde_json::to_string(alert)?;
        writeln!(self.file, "{}", line)?;
        Ok(())
    }
}
```

**Tests à ajouter**:
- [ ] `test_file_sink_creates_file`
- [ ] `test_file_sink_writes_json_lines`
- [ ] `test_file_sink_handles_rotation`

### 3. Implémenter HttpWebhookSink [2 jours]
**Priorité**: HAUTE  
**Fichier**: `crates/varpulis-runtime/src/sink.rs`

```rust
pub struct HttpWebhookSink {
    client: reqwest::Client,
    url: String,
    headers: HashMap<String, String>,
    timeout: Duration,
    retry_config: RetryConfig,
}
```

**Dépendance à ajouter**:
```toml
reqwest = { version = "0.11", features = ["json"] }
```

### 4. Tests CLI [2 jours]
**Priorité**: HAUTE  
**Fichier**: `crates/varpulis-cli/src/main.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert_cmd::Command;
    
    #[test]
    fn test_run_simple_program() {
        let mut cmd = Command::cargo_bin("varpulis").unwrap();
        cmd.args(&["run", "-c", "stream X from Y"])
           .assert()
           .success();
    }
    
    #[test]
    fn test_check_valid_syntax() {
        // ...
    }
    
    #[test]
    fn test_parse_shows_ast() {
        // ...
    }
}
```

**Dépendance tests**:
```toml
[dev-dependencies]
assert_cmd = "2.0"
predicates = "3.0"
```

### 5. GitHub Actions [0.5 jour]
**Priorité**: MOYENNE  
**Fichier**: `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo build --all-targets
      - run: cargo test --all
      - run: cargo clippy -- -D warnings

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: llvm-tools-preview
      - run: cargo install grcov
      - run: RUSTFLAGS="-C instrument-coverage" cargo test
      - run: grcov . -s . --binary-path ./target/debug/ -t markdown -o coverage.md
```

---

## Checklist Fin de Sprint

### Code
- [ ] 0 warnings Rust
- [ ] FileSink implémenté et testé
- [ ] HttpWebhookSink implémenté et testé
- [ ] 5+ tests CLI ajoutés

### CI/CD
- [ ] GitHub Actions configuré
- [ ] Couverture automatique

### Documentation
- [ ] README mis à jour avec badges
- [ ] CHANGELOG créé

### Métriques
- [ ] Couverture ≥ 70%
- [ ] 0 tests échoués
- [ ] Build < 2 minutes

---

## Commandes Utiles

```bash
# Build
cargo build --release

# Tests avec couverture
RUSTFLAGS="-C instrument-coverage" cargo test
grcov . -s . --binary-path ./target/debug/ -t html -o coverage-html

# Linting
cargo clippy --all-targets -- -D warnings
cargo fmt --check

# Run demo
cargo run -- demo --duration 30 --anomalies

# Run scénario
cargo run -- simulate -p tests/scenarios/electrical_consumption.vpl \
                      -e tests/scenarios/electrical_consumption.evt \
                      --verbose
```

---

## Contacts & Ressources

- **Repo**: github.com/varpulis/varpulis
- **Docs**: `/home/cpo/cep/docs/`
- **Examples**: `/home/cpo/cep/examples/`

---

*Sprint Planning créé le 25/01/2026*
