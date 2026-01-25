# Varpulis CEP - Kanban

> Dernière mise à jour: 2026-01-25

## Vue d'ensemble

| Catégorie | À faire | En cours | Terminé |
|-----------|---------|----------|----------|
| Parser Pest | 0 | 0 | 7 |
| SASE+ | 1 | 0 | 7 |
| Attention | 3 | 0 | 1 |
| Test Infra | 0 | 0 | 4 |
| Couverture | 2 | 0 | 0 |
| VS Code | 1 | 0 | 0 |
| **Total** | **7** | **0** | **19** |

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

**✅ Parser Pest est maintenant le défaut** - L'ancien parser est déprécié.

---

## PRIORITÉ HAUTE - SASE+ Pattern Matching

### À faire

- [ ] **SASE-07**: Benchmarks performance
  - **Action**: Créer `benches/pattern_bench.rs` avec criterion
  - **Comparer**: Ancien PatternEngine vs SaseEngine
  - **Métriques**: Latence, throughput, mémoire

### Terminé

- [x] **SASE-01**: Analyser implémentation actuelle (pattern.rs, sequence.rs)
- [x] **SASE-02**: Créer module sase.rs avec algo SASE+
- [x] **SASE-03**: Implémenter NFA avec stack pour Kleene+
- [x] **SASE-04**: Ajouter partition par attribut (SASEXT)
- [x] **SASE-05**: Implémenter négation efficace
- [x] **SASE-05b**: Intégrer dans runtime engine (structure prête)
- [x] **SASE-06**: Syntaxe pattern supportée (lambdas + séquences `A -> B -> C`)
- [x] **SASE-08**: Exemples SASE+ concrets (`examples/sase_patterns.vpl`)

---

## 🔴 PRIORITÉ CRITIQUE - Attention Engine (Performance)

> **Verdict**: Implémentation naïve O(n²) - NE SCALE PAS au-delà de 10K events

### À faire

- [ ] **ATT-01**: ANN Indexing (HNSW) - **100-1000x speedup**
  - **Problème**: Boucle sur TOUT l'historique O(n)
  - **Solution**: Utiliser `hnsw_rs` pour recherche top-k en O(log n)
  - **Gain**: 100K → 100 comparaisons
  - **Complexité**: High (2-3 semaines)

- [ ] **ATT-02**: SIMD Dot Products - **8x speedup**
  - **Problème**: `dot_product` scalaire
  - **Solution**: AVX2/AVX-512 vectorisation
  - **Complexité**: Medium (1 semaine)

- [ ] **ATT-03**: Batch Processing - **10x speedup**
  - **Problème**: Traitement event par event
  - **Solution**: Batch embedding + attention avec `rayon`
  - **Complexité**: Medium (2 semaines)

### Terminé

- [x] **ATT-00**: Métriques performance (`AttentionStats`)
  - `avg_compute_time_us`, `max_compute_time_us`, `ops_per_sec`
  - `check_performance()` warnings, `estimated_throughput()`

### Limites actuelles

| History Size | Max Events/sec | Latency | Verdict |
|--------------|---------------|---------|----------|
| 1K | 1K | 10ms | ✅ OK dev |
| 10K | 100 | 100ms | ⚠️ Limite |
| 100K | 1 | 10s | ❌ INUTILISABLE |

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

- **Tests totaux**: 544 passing (8 ignored)
- **Couverture**: 62.92% 🔴 (cible: 80%)
- **Clippy warnings**: 0
- **Parser par défaut**: ✅ Pest (avec préprocesseur d'indentation)
- **Attention Engine**: ⚠️ Naïve O(n²) - limite 10K events
- **SASE+ Tests**: 14 tests unitaires
- **Benchmarks**: Criterion benchmarks disponibles
- **Documentation**: README.md production-ready
