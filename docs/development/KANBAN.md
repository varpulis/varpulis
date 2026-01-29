# Varpulis CEP - Kanban

> Derniere mise a jour: 2026-01-29

## Vue d'ensemble

| Categorie | A faire | En cours | Termine |
|-----------|---------|----------|----------|
| Parser Pest | 0 | 0 | **8** |
| SASE+ | 0 | 0 | **10** |
| Attention | 0 | 0 | **4** |
| Benchmarks | 0 | 0 | **2** |
| Test Infra | 0 | 0 | **4** |
| Engine Refactor | 0 | 0 | **3** |
| Security | 2 | 0 | **5** |
| CLI Refactor | 0 | 0 | **2** |
| Performance | 3 | 0 | 0 |
| Couverture | 2 | 0 | 0 |
| VS Code | 1 | 0 | 0 |
| **Total** | **8** | **0** | **38** |

---

## PRIORITE HAUTE - Security

### A faire

- [ ] **SEC-04**: Implementer authentification WebSocket
  - **Severite**: HIGH
  - **Action**: JWT ou API key authentication
  - **Fichier**: `crates/varpulis-cli/src/main.rs`
  - **Effort**: 1-2 jours

- [ ] **SEC-05**: Ajouter support TLS/WSS
  - **Severite**: HIGH
  - **Action**: Activer WSS avec certificats TLS
  - **Fichier**: `crates/varpulis-cli/src/main.rs`
  - **Effort**: 1 jour

### Termine

- [x] **SEC-01**: Corriger vulnerabilite path traversal
  - Ajout de `validate_path()` avec `canonicalize()`
  - Messages d'erreur generiques
- [x] **SEC-02**: Bind WebSocket sur localhost par defaut
  - Changement de `0.0.0.0` a `127.0.0.1`
  - Option `--bind` pour acces externe explicite
- [x] **SEC-03**: Limite de recursion pour imports
  - `MAX_IMPORT_DEPTH = 10`
  - Detection de cycles avec `HashSet<PathBuf>`

---

## TERMINE - CLI Refactoring (TDD)

> **Statut**: Modules security et websocket extraits avec 76 tests

### Termine

- [x] **CLI-01**: Creer module `security.rs` dedie
  - `SecurityError` enum avec types d'erreurs dedies
  - `validate_path()` pour prevenir path traversal
  - `validate_workdir()` pour valider le repertoire de travail
  - `is_suspicious_path()` pour detection rapide
  - `sanitize_filename()` pour nettoyage des noms de fichiers
  - `generate_request_id()` sans unwrap()
  - **28 tests unitaires**

- [x] **CLI-02**: Creer module `websocket.rs` dedie
  - `WsMessage` enum avec serialization Serde
  - `StreamInfo` struct pour info streams
  - `ServerState` struct pour etat serveur
  - `handle_message()` handler principal
  - `handle_connection()` pour connexions WS
  - `json_to_value()` et `value_to_json()` conversions
  - `spawn_alert_forwarder()` pour alerts
  - **48 tests unitaires**

### Principes appliques

- **TDD**: Tests ecrits avant implementation
- **Zero unwrap()**: Tous les `.unwrap()` remplaces par `.ok_or_else()`, `.unwrap_or()`, ou `?`
- **Modularite**: Code extrait dans modules dedies testables
- **Type safety**: `SecurityError` enum au lieu de String

---

## PRIORITE HAUTE - Performance

### A faire

- [ ] **PERF-01**: Reduire cloning dans les hot paths
  - **Severite**: HIGH
  - **Impact**: 427 occurrences de clone/into/collect
  - **Action**: Utiliser `Arc<Event>` pour evenements partages
  - **Fichier**: `crates/varpulis-runtime/src/engine/mod.rs`
  - **Effort**: 2-3 jours

- [ ] **PERF-02**: Optimiser window cleanup
  - **Action**: Remplacer O(n) iteration par deque rotation
  - **Fichier**: `crates/varpulis-runtime/src/window.rs`
  - **Effort**: 0.5 jour

- [ ] **PERF-03**: Optimiser CountDistinct hashing
  - **Action**: Implementer Hash directement sur Value
  - **Fichier**: `crates/varpulis-runtime/src/aggregation.rs`
  - **Effort**: 0.5 jour

---

## TERMINE - Parser Pest

- [x] **PEST-00**: Creer grammaire pest complete (`varpulis.pest`)
- [x] **PEST-00b**: Implementer `pest_parser.rs` avec conversion vers AST
- [x] **PEST-01**: Corriger `as alias` dans followed_by (aliased_source rule)
- [x] **PEST-02**: Corriger operateurs arithmetiques (+, -, *, /) - additive_op/multiplicative_op rules
- [x] **PEST-03**: Corriger match_all keyword (match_all_keyword rule)
- [x] **PEST-04**: Etendre pattern grammar (and/or/xor/not)
- [x] **PEST-05**: Preprocesseur d'indentation (`indent.rs`) - INDENT/DEDENT tokens
- [x] **PEST-06**: filter_expr pour followed_by (ne consomme plus `.emit()`)
- [x] **PEST-07**: pattern_body unifiant lambdas et sequences
- [x] **PEST-08**: **Remplacer tous les `.unwrap()` par `expect_next()`**
  - 114 `.unwrap()` -> 0 `.unwrap()`
  - Messages d'erreur descriptifs
  - 57 tests passants

**Parser Pest est maintenant le defaut** - L'ancien parser est deprecie.
**Parser securise** - Plus de panics sur input malforme.

---

## TERMINE - SASE+ Pattern Matching

> **Statut**: SASE+ est maintenant le **moteur principal** pour le pattern matching

### Termine

- [x] **SASE-01**: Analyser implementation actuelle (pattern.rs, sequence.rs)
- [x] **SASE-02**: Creer module sase.rs avec algo SASE+
- [x] **SASE-03**: Implementer NFA avec stack pour Kleene+
- [x] **SASE-04**: Ajouter partition par attribut (SASEXT)
- [x] **SASE-05**: Implementer negation efficace
- [x] **SASE-05b**: Integrer dans runtime engine (structure prete)
- [x] **SASE-06**: Syntaxe pattern supportee (lambdas + sequences `A -> B -> C`)
- [x] **SASE-07**: Benchmarks performance (`benches/pattern_benchmark.rs`)
- [x] **SASE-08**: Exemples SASE+ concrets (`examples/sase_patterns.vpl`)
- [x] **SASE-09**: **Integration complete dans engine.rs**
  - SASE+ utilise en priorite pour tous les patterns de sequence
  - References inter-evenements (CompareRef)
  - Kleene+ avec `CompleteAndBranch`
  - Negation globale via `global_negations`
  - Export `eval_filter_expr` pour evaluation des predicats
- [x] **SASE-10**: **Enrichissement des demos avec patterns SASE+**
  - HVAC: 4 nouveaux patterns (RapidTempSwing, CascadeFailure, etc.)
  - Financial: 4 nouveaux patterns (FlashCrashRecovery, MomentumUp, etc.)
  - Total: 11 patterns SASE+ dans les exemples

---

## TERMINE - Attention Engine (Performance)

> **Statut**: Toutes optimisations implementees - **Total ~30x speedup**

### Termine

- [x] **ATT-00**: Metriques performance (`AttentionStats`)
  - `avg_compute_time_us`, `max_compute_time_us`, `ops_per_sec`
  - `check_performance()` warnings, `estimated_throughput()`

- [x] **ATT-01**: ANN Indexing (HNSW)
  - `hnsw_rs` pour recherche top-k en O(log n)
  - `HnswIndex` avec ef_search=30, min_size=100
  - `new_without_hnsw()` pour comparaison

- [x] **ATT-02**: SIMD Projections **~3x speedup**
  - Loop unrolling 4x avec `get_unchecked` sur `project()`
  - SIMD dot product pour Q.K

- [x] **ATT-03**: Batch Processing
  - `compute_attention_batch()` avec `rayon`

- [x] **ATT-04**: Cache Q + Pre-calcul K **~8x speedup**
  - Q projection calcule 1x par head (au lieu de k fois)
  - K projections pre-calculees a l'insertion
  - Stockage: `history: Vec<(Event, Vec<f32>, Vec<Vec<f32>>)>`

### Performance finale (apres toutes optimisations)

| History | Avant | Apres | Speedup |
|---------|-------|-------|---------|
| 1000 | 41.2ms | **4.9ms** | **8.4x** |
| 2000 | 71.6ms | **12.7ms** | **5.6x** |

| History Size | Latence | Throughput | Verdict |
|--------------|---------|------------|----------|
| 500 | <2ms | **>500 evt/s** | Excellent |
| 1K | 5ms | **200 evt/s** | Production |
| 2K | 13ms | **77 evt/s** | OK |

---

## TERMINE - Benchmarks (criterion)

- [x] **BENCH-01**: Benchmarks SASE+ (`pattern_benchmark.rs`)
  - Simple sequence, Kleene+, predicates, long sequences
  - Complex patterns (negation, OR, nested)
  - Scalabilite 100K events
- [x] **BENCH-02**: Benchmarks Attention (`attention_benchmark.rs`)
  - Single event, batch processing
  - Comparaison sequentiel vs parallel
  - Cache embedding warm/cold

### Resultats SASE+ (10K events)

| Pattern | Temps | Throughput |
|---------|-------|------------|
| Simple seq (A->B) | 31ms | **320K evt/s** |
| Kleene+ (A->B+->C) | 25ms | **200K evt/s** |
| Long seq (10 events) | 377ms | 26K evt/s |

---

## TERMINE - Infrastructure de Test MQTT

- [x] **TEST-01**: Docker Compose Mosquitto (`tests/mqtt/docker-compose.yml`)
- [x] **TEST-02**: Simulateur Python (`tests/mqtt/simulator.py`)
  - Scenarios: fraud, trading, iot
  - Options: rate, duration, burst mode
- [x] **TEST-03**: Scenarios YAML (`tests/mqtt/scenarios/`)
  - `fraud_scenario.yaml`, `trading_scenario.yaml`, `iot_scenario.yaml`
- [x] **TEST-04**: Connecteur MQTT Rust (`connector.rs` avec feature `mqtt`)
  - `MqttSource`, `MqttSink` avec rumqttc

### Utilisation

```bash
# Demarrer Mosquitto
cd tests/mqtt && docker-compose up -d

# Simuler evenements
pip install -r requirements.txt
python simulator.py --scenario fraud --rate 100 --duration 60

# Lancer scenario complet
python run_scenario.py scenarios/fraud_scenario.yaml
```

---

## TERMINE - Refactoring Engine

> **Statut**: engine.rs decoupe en modules, code mort supprime, legacy tracker retire

### Termine

- [x] **ENG-01**: Modulariser engine.rs (3,716 -> ~1,700 lignes)
  - `mod.rs` - Point d'entree et Engine struct
  - `compiler.rs` - Compilation VPL -> Runtime
  - `evaluator.rs` - Evaluation expressions
  - `types.rs` - Types et structs
  - `tests.rs` - Tests unitaires
- [x] **ENG-02**: Supprimer code mort
  - `PartitionBy` variant inutilise
  - `aggregators` field inutilise
  - `#[allow(dead_code)]` sur `sase_engine`
- [x] **ENG-03**: Supprimer legacy SequenceTracker
  - SASE+ est maintenant le seul moteur de sequences
  - Supprime `sequence_tracker` field de `StreamDefinition`
  - Supprime `compile_sequence_filter()` inutilise
  - ~150 lignes de code legacy supprimees

---

## PRIORITE MOYENNE - Couverture de Tests

> **Couverture actuelle**: 62.92% (cible: 80%)

### A faire

- [ ] **COV-01**: Augmenter couverture engine/mod.rs
  - **Couverture actuelle**: ~55%
  - **Cible**: 80%+
  - **Action**: Ajouter tests unitaires pour fonctions non couvertes

- [ ] **COV-02**: Tests d'integration SASE+ avances
  - **Action**: Ajouter tests Kleene+, negation, partition
  - **Scenarios**: Out-of-order events, clock skew, concurrent patterns

---

## PRIORITE BASSE - Tooling VS Code

### A faire

- [ ] **VSCODE-01**: Integrer tree-sitter pour syntax highlighting
  - **Action**: Creer `tree-sitter-varpulis/grammar.js`
  - **Integration**: Remplacer TextMate grammar par tree-sitter
  - **Benefices**: Meilleur highlighting, code folding, semantic tokens
  - **Complexite**: Medium

---

## Ordre d'execution recommande

```mermaid
graph LR
    SEC04[SEC-04: Auth WS] --> SEC05[SEC-05: TLS]
    SEC05 --> PERF01[PERF-01: Arc Event]
    PERF01 --> COV01[COV-01: Tests engine]
    COV01 --> VSCODE01[VSCODE-01: Tree-sitter]
```

### Sprint 1 (Security - 3 jours)
1. SEC-04: Authentification WebSocket
2. SEC-05: TLS/WSS support

### Sprint 2 (Performance - 3 jours)
3. PERF-01: Reduire cloning
4. PERF-02: Optimiser window cleanup
5. PERF-03: Optimiser CountDistinct

### Sprint 3 (Quality - 2 jours)
6. COV-01: Tests engine
7. COV-02: Tests SASE+ avances

### Sprint 4 (Tooling - 2 jours)
8. VSCODE-01: Tree-sitter integration

---

## Commandes de validation

```bash
# Tests complets
cargo test --workspace

# Clippy sans warnings
cargo clippy --workspace

# Tests SASE uniquement
cargo test -p varpulis-runtime sase

# Tests parser pest
cargo test -p varpulis-parser pest

# Benchmarks
cargo bench --bench pattern_benchmark
cargo bench --bench attention_benchmark

# Coverage (necessite cargo-tarpaulin)
cargo tarpaulin --out Html
```

---

## Fichiers cles

| Fichier | Description |
|---------|-------------|
| `crates/varpulis-parser/src/varpulis.pest` | Grammaire PEG Pest |
| `crates/varpulis-parser/src/pest_parser.rs` | Parser Pest -> AST |
| `crates/varpulis-runtime/src/sase.rs` | Moteur SASE+ |
| `crates/varpulis-runtime/src/attention.rs` | Attention mechanism |
| `crates/varpulis-runtime/src/engine/mod.rs` | Runtime engine |
| `crates/varpulis-runtime/src/connector.rs` | Connecteurs MQTT/HTTP |
| `tests/mqtt/simulator.py` | Simulateur d'evenements Python |
| `vscode-varpulis/syntaxes/varpulis.tmLanguage.json` | TextMate grammar |

---

## Metriques actuelles

| Metrique | Valeur | Cible | Statut |
|----------|--------|-------|--------|
| **Tests totaux** | **157+** | 100+ | Excellent |
| **Tests CLI** | **76** | - | Excellent |
| **Couverture** | 62.92% | 80% | Needs work |
| **Clippy warnings** | 0 | 0 | Excellent |
| **Unsafe blocks** | 4 | <10 | Excellent |
| **Unwrap parser** | 0 | 0 | Excellent |
| **Unwrap CLI** | **0** | 0 | Excellent |
| **Unwrap runtime** | ~200 | <50 | Needs work |

---

## Architecture actuelle

```
~25,500 lignes de Rust
4 crates + 1 CLI
Version 0.1.0

+-- varpulis-core (1,420 lignes)
|   +-- ast.rs - AST definitions
|   +-- types.rs - Type system
|   +-- value.rs - Runtime values
|
+-- varpulis-parser (3,415 lignes)
|   +-- varpulis.pest - PEG grammar
|   +-- pest_parser.rs - Parser implementation
|   +-- indent.rs - Indentation preprocessor
|
+-- varpulis-runtime (9,171 lignes)
|   +-- sase.rs - SASE+ NFA engine
|   +-- attention.rs - Attention mechanism
|   +-- engine/ - Runtime engine (modularise)
|   +-- connector.rs - External connectors
|   +-- aggregation.rs - Aggregation functions
|   +-- window.rs - Time windows
|
+-- varpulis-cli (~1,100 lignes) [REFACTORED]
    +-- main.rs - CLI entry point
    +-- lib.rs - Library exports
    +-- security.rs - Security module (28 tests)
    +-- websocket.rs - WebSocket module (48 tests)
```

---

## Prochaines etapes recommandees

1. **Securite d'abord**: Implementer authentification et TLS avant deployment
2. **Performance**: Reduire cloning pour ameliorer throughput
3. **Qualite**: Augmenter couverture de tests a 80%
4. **Tooling**: Ameliorer experience developpeur avec tree-sitter

**Estimation pour production-ready**: ~10 jours de travail
