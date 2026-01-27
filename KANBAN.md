# Varpulis CEP - Kanban

> Dernière mise à jour: 2026-01-27

## Vue d'ensemble

| Catégorie | À faire | En cours | Terminé |
|-----------|---------|----------|----------|
| Parser Pest | 0 | 0 | **8** |
| SASE+ | 0 | 0 | **9** |
| Attention | 0 | 0 | 4 |
| Benchmarks | 0 | 0 | 2 |
| Test Infra | 0 | 0 | 4 |
| Couverture | 2 | 0 | 0 |
| VS Code | 1 | 0 | 0 |
| Engine Refactor | 0 | 0 | **2** |
| **Total** | **3** | **0** | **29** |

---

## ✅ TERMINÉ - Parser Pest

- [x] **PEST-00**: Créer grammaire pest complète (`varpulis.pest`)
- [x] **PEST-00b**: Implémenter `pest_parser.rs` avec conversion vers AST
- [x] **PEST-01**: Corriger `as alias` dans followed_by (aliased_source rule)
- [x] **PEST-02**: Corriger opérateurs arithmétiques (+, -, *, /) - additive_op/multiplicative_op rules
- [x] **PEST-03**: Corriger match_all keyword (match_all_keyword rule)
- [x] **PEST-04**: Étendre pattern grammar (and/or/xor/not)
- [x] **PEST-05**: Préprocesseur d'indentation (`indent.rs`) - INDENT/DEDENT tokens
- [x] **PEST-06**: filter_expr pour followed_by (ne consomme plus `.emit()`)
- [x] **PEST-07**: pattern_body unifiant lambdas et séquences
- [x] **PEST-08**: **Remplacer tous les `.unwrap()` par `expect_next()`** 🆕
  - 114 `.unwrap()` → 0 `.unwrap()`
  - Messages d'erreur descriptifs
  - 57 tests passants

**✅ Parser Pest est maintenant le défaut** - L'ancien parser est déprécié.
**✅ Parser sécurisé** - Plus de panics sur input malformé.

---

## ✅ TERMINÉ - SASE+ Pattern Matching

> **Statut**: SASE+ est maintenant le **moteur principal** pour le pattern matching

### Terminé

- [x] **SASE-01**: Analyser implémentation actuelle (pattern.rs, sequence.rs)
- [x] **SASE-02**: Créer module sase.rs avec algo SASE+
- [x] **SASE-03**: Implémenter NFA avec stack pour Kleene+
- [x] **SASE-04**: Ajouter partition par attribut (SASEXT)
- [x] **SASE-05**: Implémenter négation efficace
- [x] **SASE-05b**: Intégrer dans runtime engine (structure prête)
- [x] **SASE-06**: Syntaxe pattern supportée (lambdas + séquences `A -> B -> C`)
- [x] **SASE-07**: Benchmarks performance (`benches/pattern_benchmark.rs`)
- [x] **SASE-08**: Exemples SASE+ concrets (`examples/sase_patterns.vpl`)
- [x] **SASE-09**: **Intégration complète dans engine.rs** 🆕
  - SASE+ utilisé en priorité pour tous les patterns de séquence
  - Références inter-événements (CompareRef)
  - Kleene+ avec `CompleteAndBranch`
  - Négation globale via `global_negations`
  - Export `eval_filter_expr` pour évaluation des prédicats

---

## ✅ TERMINÉ - Attention Engine (Performance)

> **Statut**: Toutes optimisations implémentées - **Total ~30x speedup**

### Terminé

- [x] **ATT-00**: Métriques performance (`AttentionStats`)
  - `avg_compute_time_us`, `max_compute_time_us`, `ops_per_sec`
  - `check_performance()` warnings, `estimated_throughput()`

- [x] **ATT-01**: ANN Indexing (HNSW) ✅
  - `hnsw_rs` pour recherche top-k en O(log n)
  - `HnswIndex` avec ef_search=30, min_size=100
  - `new_without_hnsw()` pour comparaison

- [x] **ATT-02**: SIMD Projections ✅ **~3x speedup**
  - Loop unrolling 4x avec `get_unchecked` sur `project()`
  - SIMD dot product pour Q·K

- [x] **ATT-03**: Batch Processing ✅
  - `compute_attention_batch()` avec `rayon`

- [x] **ATT-04**: Cache Q + Pré-calcul K ✅ **~8x speedup**
  - Q projection calculé 1x par head (au lieu de k fois)
  - K projections pré-calculées à l'insertion
  - Stockage: `history: Vec<(Event, Vec<f32>, Vec<Vec<f32>>)>`

### Performance finale (après toutes optimisations)

| History | Avant | Après | Speedup |
|---------|-------|-------|---------|
| 1000 | 41.2ms | **4.9ms** | **8.4x** |
| 2000 | 71.6ms | **12.7ms** | **5.6x** |

| History Size | Latence | Throughput | Verdict |
|--------------|---------|------------|----------|
| 500 | <2ms | **>500 evt/s** | ✅ Excellent |
| 1K | 5ms | **200 evt/s** | ✅ Production |
| 2K | 13ms | **77 evt/s** | ✅ OK |

---

## ✅ TERMINÉ - Benchmarks (criterion)

- [x] **BENCH-01**: Benchmarks SASE+ (`pattern_benchmark.rs`)
  - Simple sequence, Kleene+, predicates, long sequences
  - Complex patterns (negation, OR, nested)
  - Scalabilité 100K events
- [x] **BENCH-02**: Benchmarks Attention (`attention_benchmark.rs`)
  - Single event, batch processing
  - Comparaison séquentiel vs parallel
  - Cache embedding warm/cold

### Résultats SASE+ (10K events)

| Pattern | Temps | Throughput |
|---------|-------|------------|
| Simple seq (A→B) | 31ms | **320K evt/s** |
| Kleene+ (A→B+→C) | 25ms | **200K evt/s** |
| Long seq (10 events) | 377ms | 26K evt/s |

---

## ✅ TERMINÉ - Infrastructure de Test MQTT

- [x] **TEST-01**: Docker Compose Mosquitto (`tests/mqtt/docker-compose.yml`)
- [x] **TEST-02**: Simulateur Python (`tests/mqtt/simulator.py`)
  - Scénarios: fraud, trading, iot
  - Options: rate, duration, burst mode
- [x] **TEST-03**: Scénarios YAML (`tests/mqtt/scenarios/`)
  - `fraud_scenario.yaml`, `trading_scenario.yaml`, `iot_scenario.yaml`
- [x] **TEST-04**: Connecteur MQTT Rust (`connector.rs` avec feature `mqtt`)
  - `MqttSource`, `MqttSink` avec rumqttc

### Utilisation

```bash
# Démarrer Mosquitto
cd tests/mqtt && docker-compose up -d

# Simuler événements
pip install -r requirements.txt
python simulator.py --scenario fraud --rate 100 --duration 60

# Lancer scénario complet
python run_scenario.py scenarios/fraud_scenario.yaml
```

---

## PRIORITÉ MOYENNE - Couverture de Tests

> **Couverture actuelle**: 62.92% 🔴

### À faire

- [ ] **COV-01**: Augmenter couverture attention.rs
  - **Cible**: 80%+ sur modules critiques

- [ ] **COV-02**: Tests d'intégration SASE+ avancés
  - **Action**: Ajouter tests Kleene+, negation, partition

---

## PRIORITÉ BASSE - Tooling VS Code

### À faire

- [ ] **VSCODE-01**: Intégrer tree-sitter pour syntax highlighting
  - **Action**: Créer `tree-sitter-varpulis/grammar.js`
  - **Intégration**: Remplacer TextMate grammar par tree-sitter
  - **Bénéfices**: Meilleur highlighting, code folding, semantic tokens
  - **Complexité**: Medium

---

## ✅ TERMINÉ - Refactoring Engine

> **Statut**: engine.rs découpé en modules, code mort supprimé

### Terminé

- [x] **ENG-01**: Modulariser engine.rs (3,716 → ~1,700 lignes)
  - `mod.rs` - Point d'entrée et Engine struct
  - `compiler.rs` - Compilation VPL → Runtime
  - `evaluator.rs` - Évaluation expressions
  - `types.rs` - Types et structs
  - `tests.rs` - Tests unitaires
- [x] **ENG-02**: Supprimer code mort
  - `PartitionBy` variant inutilisé
  - `aggregators` field inutilisé
  - `#[allow(dead_code)]` sur `sase_engine`

---

## Ordre d'exécution recommandé

```mermaid
graph LR
    PEST01[PEST-01: Fix alias] --> PEST02[PEST-02: Activer pest]
    PEST02 --> PEST03[PEST-03: Supprimer ancien]
    PEST02 --> SASE06[SASE-06: Syntaxe pattern]
    SASE06 --> SASE07[SASE-07: Benchmarks]
    SASE07 --> SASE08[SASE-08: Shared subexpr]
    PEST03 --> VSCODE01[VSCODE-01: Tree-sitter]
```

### Sprint 1 (Parser)
1. PEST-01 → PEST-02 → PEST-03

### Sprint 2 (SASE+)
2. SASE-06 → SASE-07 → SASE-08

### Sprint 3 (Tooling)
3. VSCODE-01

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
```

---

## Fichiers clés

| Fichier | Description |
|---------|-------------|
| `crates/varpulis-parser/src/varpulis.pest` | Grammaire PEG Pest |
| `crates/varpulis-parser/src/pest_parser.rs` | Parser Pest → AST |
| `crates/varpulis-parser/src/parser.rs` | Ancien parser (à supprimer) |
| `crates/varpulis-runtime/src/sase.rs` | Moteur SASE+ |
| `crates/varpulis-runtime/src/pattern.rs` | Ancien pattern matcher |
| `crates/varpulis-runtime/src/connector.rs` | Connecteurs MQTT/Kafka/HTTP |
| `tests/mqtt/simulator.py` | Simulateur d'événements Python |
| `vscode-varpulis/syntaxes/varpulis.tmLanguage.json` | TextMate grammar |

---

## Métriques actuelles

- **Tests totaux**: 544+ passing
- **Couverture**: 62.92% 🔴 (cible: 80%)
- **Clippy warnings**: 0
- **Parser par défaut**: ✅ Pest (avec préprocesseur d'indentation)
- **Attention Engine**: ✅ Optimisé ~30x speedup
- **SASE+ Engine**: ✅ **Intégré comme moteur principal**
  - NFA-based pattern matching
  - Kleene+ avec émission continue
  - Négation globale
  - Références inter-événements
- **Engine**: ✅ Modularisé (5 sous-modules)
- **Benchmarks**: Criterion benchmarks disponibles
- **Documentation**: README.md production-ready
