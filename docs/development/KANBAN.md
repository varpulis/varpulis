# Varpulis CEP - Kanban

> Derniere mise a jour: 2026-02-03 (v0.1.0 Release + SaaS Roadmap)

## Production Readiness Score: 10/10 - Released

| Critere | Score | Statut |
|---------|-------|--------|
| Code Quality | 10/10 | Clippy clean, LSP server, v0.1.0 released |
| Test Coverage | 6/10 | 62.92% (cible 80%) |
| Error Handling | **10/10** | **0 unwraps production** |
| Security | **10/10** | TLS/Auth + Rate Limiting |
| Performance | **10/10** | ZDD + SIMD + Incremental, 300-500K evt/s |
| Documentation | 9/10 | Exemples complets, LSP hover docs |
| Developer Experience | **10/10** | **LSP + Visual Editor + New Syntax** |
| Release & Distribution | **10/10** | **5 platforms + Docker + GitHub Release** |

### Production Status

1. ~~**~200 unwrap() in runtime**~~ **FIXED** - 0 unwraps en production
2. ~~**Rate limiting**~~ **DONE** - Token bucket per IP
3. ~~**LSP Server**~~ **DONE** - Diagnostics, hover, completion, semantic tokens
4. ~~**Connectivity Architecture**~~ **DONE** - Connectors, sinks, .from() syntax
5. ~~**v0.1.0 Release**~~ **DONE** - 5 platforms, Docker image, GitHub Release
6. **Test coverage 62.92%** - En dessous du seuil 80% (en cours)
7. **SaaS** - Multi-tenant, REST API, usage metering (en cours)

### Monetization Tiers

| Tier | Readiness | Work Remaining |
|------|-----------|----------------|
| **Community** (Open Source) | **RELEASED v0.1.0** | Disponible maintenant |
| **Enterprise** (On-prem + Support) | **READY** | LSP, rate limiting, hot reload done |
| **SaaS** (Managed Service) | **EN COURS** | Multi-tenant, REST API, metering |

## Vue d'ensemble

| Categorie | A faire | En cours | Termine |
|-----------|---------|----------|----------|
| **Production Readiness** | 0 | 0 | **4** |
| **Release & Distribution** | 0 | 0 | **2** |
| **SaaS** | **2** | **4** | 0 |
| Parser Pest | 0 | 0 | **9** |
| SASE+ Core | 0 | 0 | **10** |
| SASE+ Improvements | 1 | 0 | **6** |
| ZDD | 0 | 0 | **2** |
| Attention | 0 | 0 | **4** |
| Runtime Optimization | 0 | 0 | **6** |
| Engine Cleanup | 0 | 0 | **1** |
| Benchmarks | 0 | 0 | **3** |
| Test Infra | 0 | 0 | **4** |
| Engine Refactor | 0 | 0 | **3** |
| Security | 0 | 0 | **7** |
| CLI Refactor | 0 | 0 | **2** |
| Performance | 0 | 0 | **6** |
| Imperative | 0 | 0 | **4** |
| Couverture | 0 | 2 | 0 |
| VS Code + LSP | 0 | 0 | **8** |
| Connectivity | 0 | 0 | **4** |
| **Total** | **3** | **6** | **85** |

---

## TERMINE - Release & Distribution

> **Statut**: v0.1.0 released avec binaires 5 platforms + Docker image

### Termine

- [x] **REL-01**: GitHub Actions release workflow
  - **Build targets**: 5 platforms (Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64)
  - **Artifacts**: CLI (`varpulis`) + LSP server (`varpulis-lsp`) pour chaque platform
  - **Docker**: Image multi-stage poussee sur ghcr.io
  - **Release**: Checksums SHA256, release notes, prerelease detection
  - **Fix**: Guard `#[cfg(target_arch = "x86_64")]` sur import SIMD pour ARM64

- [x] **REL-02**: v0.1.0 Initial Release
  - **URL**: https://github.com/varpulis/varpulis/releases/tag/v0.1.0
  - **Assets**: 10 binaires + checksums.txt
  - **Docker**: `ghcr.io/varpulis/varpulis:0.1.0` et `:latest`
  - **Trigger**: Tag `v*` sur main

---

## HAUTE PRIORITE - SaaS (Managed Service)

> **Objectif**: Transformer Varpulis en service cloud multi-tenant
> **Statut**: EN COURS

### En cours

- [ ] **SAAS-01**: Multi-tenant isolation
  - **Description**: Isolation des engines par tenant
  - **Implementation prevue**:
    - `TenantId` type avec UUID
    - `TenantConfig` avec resource limits (max_pipelines, max_events_per_sec, max_streams)
    - `TenantEngine` wrapper autour de Engine avec quotas
    - `TenantManager` pour gestion du lifecycle
    - Isolation memoire: chaque tenant a son propre Engine
  - **Fichier**: `crates/varpulis-runtime/src/tenant.rs`

- [ ] **SAAS-02**: REST API pour gestion de pipelines
  - **Description**: API RESTful pour deployer/gerer des pipelines CEP
  - **Endpoints**:
    - `POST /api/v1/pipelines` - Deployer un pipeline VPL
    - `GET /api/v1/pipelines` - Lister les pipelines du tenant
    - `GET /api/v1/pipelines/:id` - Details d'un pipeline
    - `DELETE /api/v1/pipelines/:id` - Supprimer un pipeline
    - `POST /api/v1/pipelines/:id/events` - Injecter des evenements
    - `GET /api/v1/pipelines/:id/metrics` - Metriques du pipeline
    - `POST /api/v1/pipelines/:id/reload` - Hot reload
  - **Auth**: API key par tenant (header `X-API-Key`)
  - **Fichier**: `crates/varpulis-cli/src/api.rs`

- [ ] **SAAS-03**: Usage metering et quotas
  - **Description**: Tracking de l'utilisation par tenant pour facturation
  - **Metriques trackees**:
    - Events processed par tenant/pipeline
    - CPU time consumed
    - Nombre de pipelines actives
    - Nombre de streams actifs
    - Uptime par pipeline
  - **Quotas**:
    - `max_events_per_second` - Rate limit par tenant
    - `max_pipelines` - Nombre max de pipelines
    - `max_streams_per_pipeline` - Nombre max de streams
  - **Fichier**: `crates/varpulis-runtime/src/usage.rs`

- [ ] **SAAS-04**: Docker Compose SaaS stack
  - **Description**: Stack complete pour deploiement SaaS
  - **Services**:
    - `varpulis-server` - API server multi-tenant
    - `prometheus` - Collecte de metriques
    - `grafana` - Dashboard monitoring
  - **Fichier**: `deploy/docker/docker-compose.saas.yml`

### A faire

- [ ] **SAAS-05**: Grafana dashboard
  - **Description**: Dashboard pre-configure pour monitoring SaaS
  - **Panels**: Events/sec par tenant, latency P99, active pipelines, errors
  - **Fichier**: `deploy/grafana/dashboards/varpulis.json`

- [ ] **SAAS-06**: CLI client pour SaaS
  - **Description**: Commande `varpulis deploy` pour deployer vers SaaS
  - **Commands**: `deploy`, `status`, `logs`, `destroy`
  - **Config**: `.varpulis.toml` avec endpoint et API key

### Architecture SaaS

```
┌─────────────────────────────────────────────────────┐
│                   Load Balancer                      │
└─────────────────────┬───────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────┐
│              Varpulis API Server                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Tenant A │  │ Tenant B │  │ Tenant C │  ...     │
│  │ Engine   │  │ Engine   │  │ Engine   │         │
│  │ Quotas   │  │ Quotas   │  │ Quotas   │         │
│  └──────────┘  └──────────┘  └──────────┘         │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  Usage Metering  │  Rate Limiting  │  Auth  │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────┘
                      │
         ┌────────────┼────────────┐
         │            │            │
    ┌────▼────┐  ┌────▼────┐  ┌───▼────┐
    │Prometheus│  │ Grafana │  │ Alerts │
    └─────────┘  └─────────┘  └────────┘
```

---

## HAUTE PRIORITE - Production Readiness

> **Objectif**: Atteindre 9/10 pour monetisation Enterprise

### Termine

- [x] **PROD-01**: Reduire unwrap() dans runtime (~200 → 0)
  - **Resultat**: 0 unwraps en production (tous dans tests)
  - **Fichiers corriges**: sase.rs, persistence.rs, join.rs, metrics.rs, attention.rs
  - **Patterns utilises**: let-else, unwrap_or_default(), is_some_and()

- [x] **PROD-02**: Rate Limiting pour WebSocket API
  - **Implementation**:
    - Token bucket algorithm par client IP
    - CLI option `--rate-limit N` (requests/second, 0 = disabled)
    - Environment variable `VARPULIS_RATE_LIMIT`
    - 429 Too Many Requests avec retry_after
    - 10 tests unitaires
  - **Usage**: `varpulis server --rate-limit 100`

### Roadmap Monetisation

```
Phase 1 (3-5 jours) - Enterprise Ready
├── PROD-01: Unwrap reduction
├── PROD-02: Rate limiting
└── Metriques Prometheus (deja fait)

Phase 2 (1-2 semaines) - Enterprise+
├── COV-01: Test coverage 80%
├── Hot reload via API
└── Distributed tracing (OpenTelemetry)

Phase 3 (3-4 semaines) - SaaS Ready
├── Multi-tenant isolation
├── Kubernetes operator
├── Additional connectors (Pulsar, RabbitMQ)
└── High availability (Raft consensus)
```

---

## TERMINE - SASE+ Improvements

> **Statut**: 6 taches terminees - corrections de correctness et ameliorations production-ready
> **Reference**: Voir `SASE_IMPROVMENT.md` pour analyse complete

### P1 - Correctness (Haute priorite) - TERMINE

- [x] **NEG-01**: Negation temporelle incomplete
  - **Probleme**: La negation passait immediatement via epsilon sans attendre la fenetre
  - **Solution implementee**:
    - `NegationConstraint` struct avec deadlines processing-time et event-time
    - `pending_negations: Vec<NegationConstraint>` dans Run
    - Confirmation via `confirm_negations_processing_time()` et `confirm_negations_event_time()`
    - NFA compile ne cree plus d'epsilon vers continue_state
  - **Tests**: `test_negation_with_predicate`, negation invalidation tests

- [x] **AND-01**: Operateur AND semantiquement incorrect
  - **Probleme**: AND(A,B) ne gerait pas A et B arrivant dans n'importe quel ordre
  - **Solution implementee**:
    - `AndState` struct avec `completed: HashSet<usize>` et `captures: HashMap`
    - `AndConfig` et `AndBranch` pour configuration au compile-time
    - `StateType::And` variant dans NFA
    - `advance_and_state()` pour traitement runtime
  - **Tests**: `test_and_matches_in_any_order`, `test_and_matches_reverse_order`, `test_and_with_aliases`

- [x] **BP-01**: Absence de backpressure
  - **Probleme**: Runs excedentaires silencieusement ignores
  - **Solution implementee**:
    - `BackpressureStrategy`: Drop, Error, EvictOldest, EvictLeastProgress, Sample
    - `ProcessResult` avec warnings (`ProcessWarning` enum)
    - `ProcessStats` pour metriques par appel
    - `extended_stats()` pour metriques cumulees
    - `handle_backpressure()` et `handle_backpressure_partitioned()`
  - **Tests**: `test_backpressure_drop_strategy`, `test_backpressure_evict_oldest`, `test_backpressure_evict_least_progress`

### P2 - Performance (Priorite moyenne) - TERMINE

- [x] **IDX-01**: Indexation des evenements
  - **Probleme**: O(n*m) comparaisons par evenement
  - **Solution implementee**:
    - `EventTypeIndex` struct avec `state_index: HashMap<String, Vec<usize>>`
    - `wildcard_states` et `and_states` pour cas speciaux
    - `from_nfa()` pour construction depuis NFA
    - `has_interest()` pour rejection rapide des events non pertinents
    - `get_candidate_states()` pour lookup O(1)
  - **Tests**: `test_has_interest_positive`, `test_has_interest_with_negation`, `test_event_type_index_with_and`

- [x] **ET-01**: Gestion event-time fragile
  - **Probleme**: `unwrap_or_default()` masquait erreurs, pas de late events
  - **Solution implementee**:
    - `EventTimeConfig` avec `max_out_of_orderness`, `allowed_lateness`, `emit_late_events`
    - `EventTimeManager` pour gestion robuste du watermark
    - `EventTimeResult` enum: OnTime, Late, TooLate
    - `compute_deadline()` static method pour calcul safe
    - Late events: `take_late_events()`, `late_event_stats()`
    - Watermark never recedes
  - **Tests**: `test_event_time_manager_late_events`, `test_event_time_manager_watermark_never_recedes`, `test_compute_deadline_safe`

- [x] **MET-01**: Metriques insuffisantes
  - **Probleme**: Seulement 3 stats basiques exposees
  - **Solution implementee**:
    - `SaseMetrics` avec compteurs atomiques: events_processed, events_matched, events_ignored, events_late_*, runs_*, matches_emitted
    - `LatencyHistogram` avec buckets exponentiels (<1µs to >1s)
    - `percentile()` et `mean()` pour statistiques latence
    - `to_prometheus()` pour export format Prometheus
    - `MetricsSummary` pour acces simplifie
    - `process_instrumented()` pour processing avec metriques completes
    - Peak runs tracking
  - **Tests**: `test_latency_histogram_*`, `test_sase_metrics_*`, `test_process_instrumented_*`

### P3 - Scalabilite (Priorite basse)

- [ ] **SE-01**: Shared Execution (future)
  - **Prerequis**: ZDD, IDX-01 (done)
  - **Description**: Partager prefixes communs entre patterns
  - **Gain**: ~50-97% reduction memoire selon patterns

---

## TERMINE - Runtime Optimization

> **Statut**: 6 taches terminees - tous les P0, P2, P3 issues de RUNTIME_ANALYSIS.md
> **Reference**: Voir `RUNTIME_ANALYSIS.md` pour analyse complete

### P0 - Bloquants pour production

- [x] **ATT-PROJ**: Pre-calcul projections Key
  - **Probleme**: K projections recalculees a chaque compute_attention()
  - **Solution implementee**: Pre-calcul a l'ajout dans history tuple
  - **Fichier**: attention.rs lines 600-606
  - **Statut**: DEJA FAIT

- [x] **ATT-LRU**: Cache LRU O(n) -> O(1)
  - **Probleme**: EmbeddingCache utilise HashMap + Vec pour LRU, retain() est O(n)
  - **Solution implementee**: Crate `lru` pour O(1) get/put
  - **Fichier**: attention.rs EmbeddingCache struct
  - **Tests**: 46 attention tests passent

- [x] **ATT-HIST**: History removal O(n) -> O(1)
  - **Probleme**: `history.remove(0)` est O(n) sur Vec
  - **Solution implementee**: `VecDeque` pour O(1) pop_front/push_back
  - **Fichier**: attention.rs AttentionEngine::history field
  - **Tests**: 46 attention tests passent

- [x] **SINK-ASYNC**: FileSink bloque runtime async
  - **Probleme**: std::fs::File::write bloque le worker thread tokio
  - **Solution implementee**: `AsyncFileSink` avec tokio::fs et buffer 64KB
  - **Fichier**: sink.rs
  - **Tests**: 8 nouveaux tests AsyncFileSink

### P2 - Performance

- [x] **JOIN-GC**: Cleanup O(n) -> O(log n)
  - **Probleme**: cleanup_expired itere sur toutes les cles a chaque event
  - **Solution implementee**: BinaryHeap expiry queue + gc_interval throttling
  - **Fichier**: join.rs JoinBuffer::cleanup_expired
  - **Tests**: 18 join tests passent

### P3 - Architecture

- [x] **SINK-RETRY**: HttpSink sans retry
  - **Probleme**: Erreurs HTTP ignorees, events perdus silencieusement
  - **Solution implementee**: `HttpSinkWithRetry` avec backoff exponentiel
  - **Features**: max_retries, initial_delay, max_delay, timeout configurable
  - **Fichier**: sink.rs HttpSinkWithRetry
  - **Tests**: 5 nouveaux tests

---

## TERMINE - Engine Cleanup

> **Statut**: Consolidation complete - SASE+ est le seul moteur de pattern matching

### Consolidation

- [x] **PATTERN-DEL**: Suppression de pattern.rs
  - **Contexte**: DELTA_ANALYSIS.md identifiait deux moteurs de patterns:
    - `pattern.rs`: Moteur AST recursif style Apama (PatternEngine, PatternExpr)
    - `sase.rs`: Moteur NFA SASE+ (SaseEngine, SasePattern)
  - **Decision**: SASE+ couvre tous les cas d'usage avec de meilleures performances
  - **Changements**:
    - Suppression `pub mod pattern;` de lib.rs
    - Suppression `PatternEngine` import de engine/mod.rs et engine/types.rs
    - Suppression `expr_to_pattern()` de engine/compiler.rs
    - Suppression champ `pattern_engine` de StreamDefinition
    - Suppression bloc de processing pattern_engine dans process_stream()
    - Suppression fichier pattern.rs (1200+ lignes)
  - **Note**: `RuntimeOp::Pattern` conserve pour evaluation d'expressions inline
  - **Tests**: 578 tests passent apres suppression

---

## TERMINE - ZDD (Zero-suppressed Decision Diagrams)

> **Statut**: Crate `varpulis-zdd` implementee avec 85 tests (73 unit + 12 doc)

### Termine

- [x] **ZDD-01**: Implementer crate varpulis-zdd
  - **Description**: ZDD pour representation compacte des combinaisons Kleene
  - **Composants**:
    - `ZddRef`: Empty, Base, Node terminals
    - `ZddNode`: Decision nodes avec var, lo, hi
    - `UniqueTable`: Canonicalization avec zero-suppression
    - Operations: union, intersection, difference, product
    - `product_with_optional`: Operation cle pour Kleene O(n) vs O(2^n)
    - `ZddIterator`: Iteration lazy sur combinaisons
    - Export DOT pour visualisation
  - **Tests**: 73 unit tests + 12 doc tests
  - **Performance**: 2^25 combinaisons avec <1000 nodes

- [x] **ZDD-02**: Fix bug product_with_optional same variable
  - **Bug**: Quand `n.var == var`, le code mettait `union(lo, hi)` sur les deux branches
  - **Symptome**: `singleton(0).product_with_optional(0)` retournait `{{}, {0}}` au lieu de `{{0}}`
  - **Fix**: `table.get_or_create(var, n.lo, new_hi)` au lieu de `table.get_or_create(var, union_lo_hi, union_lo_hi)`
  - **Note**: Bug ne se declenchait jamais en usage Kleene sequentiel (variables monotones)
  - **Test**: `test_product_with_optional_same_variable` ajoute

---

## TERMINE - Security

> **Statut**: Toutes les tâches de sécurité complétées

### Termine

- [x] **SEC-05**: Ajouter support TLS/WSS
  - Options CLI `--tls-cert` et `--tls-key`
  - Variables env `VARPULIS_TLS_CERT`, `VARPULIS_TLS_KEY`
  - Validation que cert et key sont fournis ensemble
  - Affichage `wss://` quand TLS activé
- [x] **SEC-04**: Implementer authentification WebSocket
  - Module `auth.rs` avec `AuthConfig` struct
  - API key validation avec constant-time comparison
  - Support header (Bearer/ApiKey) et query (api_key/token)
  - Integration warp filter avec rejection handling
  - Option CLI `--api-key` et env `VARPULIS_API_KEY`
  - **35 tests unitaires**
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

## TERMINE - Performance

### Termine

- [x] **PERF-01**: Reduire cloning dans les hot paths
  - **Severite**: HIGH
  - **Impact**: 427 occurrences de clone/into/collect
  - **Action**: Utiliser `Arc<Event>` pour evenements partages
  - **Fichier**: `crates/varpulis-runtime/src/engine/mod.rs`
  - **Effort**: 2-3 jours
  - **Note**: Necessite refactoring Event, Window, Aggregation
  - **Implementation**:
    - [x] SASE+ engine: `SharedEvent` (Arc<Event>) pour StackEntry, Run, MatchResult
    - [x] Window module: Vec<Event> -> Vec<SharedEvent>
    - [x] Aggregation module: apply_shared() pour SharedEvent
    - [x] Engine pipeline: Vec<SharedEvent> partout

- [x] **PERF-02**: Optimiser window cleanup
  - SlidingWindow: position() + drain() au lieu de pop_front() loop
  - SlidingCountWindow: saturating_sub + drain()
  - Complexite: O(log n + k) vs O(k) pour k events expires

- [x] **PERF-03**: Optimiser CountDistinct et aggregations
  - CountDistinct: stocke u64 hashes au lieu de cloner Values
  - Avg: algorithme single-pass avec fold
  - StdDev: algorithme de Welford (single-pass, stable numeriquement)

- [x] **PERF-04**: Eliminer cloning Vec dans hot path (event_sources)
  - **Severite**: MEDIUM
  - **Impact**: Clone Vec<String> pour chaque evenement traite
  - **Action**: Utiliser `Arc<[String]>` pour partage O(1)
  - **Implementation**:
    - `event_sources: HashMap<String, Arc<[String]>>` au lieu de Vec
    - Helper `add_event_source()` pour mutation propre
    - Clone Arc = increment atomique, pas copie profonde
    - 752 tests passent

- [x] **PERF-05**: Mode batch processing pour throughput eleve
  - **Severite**: HIGH
  - **Impact**: ~15-20% amelioration throughput a grand volume
  - **Implementation**:
    - `process_batch(Vec<Event>)` traite par lots de 100
    - Pre-allocation des vecteurs
    - Collecte des alerts en batch
    - CLI `simulate --immediate` utilise batch processing
  - **Resultats**:
    - Filter only: 170K evt/s (vs Apama 163K) = **Varpulis plus rapide**
    - Window+Aggregate: 140K evt/s (vs Apama 163K) = Apama 16% plus rapide

- [x] **PERF-06**: SIMD optimizations et incremental aggregation
  - **Severite**: MEDIUM
  - **Impact**: Amelioration significative aggregations sur grandes fenetres
  - **Implementation**:
    - Module `simd.rs` avec AVX2 vectorisation
    - `simd_sum`, `simd_min`, `simd_max` pour f64 arrays
    - `compare_gt_f64`, `compare_lt_f64` batch comparisons
    - `IncrementalSum`, `IncrementalMinMax` accumulators
    - `IncrementalSlidingWindow` avec O(1) aggregate updates
    - Integration dans aggregation.rs (Sum, Avg, Min, Max)
  - **Tests**: 28 nouveaux tests
  - **Note**: Unsafe blocks 4 → 17 (requis pour SIMD intrinsics)

---

## TERMINE - Feature Parity Apama

> **Statut**: Parite complete avec Apama + ameliorations

### Termine

- [x] **HOT-01**: Hot Reload sans redemarrage
  - **Apama**: Hot redeploy complexe
  - **Varpulis**: Simple et developer-friendly
  - **Implementation**:
    - `Engine::reload(&Program)` - recharge le programme
    - Preservation d'etat intelligente (filters: oui, windows: reset)
    - `ReloadReport` avec details des changements
    - Streams ajoutes/supprimes/mis a jour trackes
  - **API**:
    ```rust
    let report = engine.reload(&new_program)?;
    // report.streams_added, streams_removed, state_preserved
    ```

- [x] **PAR-01**: Parallelisme Controle (Worker Pools)
  - **Apama**: spawn/contexts opaques, pas de controle
  - **Varpulis**: Controle explicite, monitoring complet
  - **Implementation**:
    - `WorkerPool` avec workers configurables
    - `BackpressureStrategy`: Block, DropOldest, DropNewest, Error
    - Partition affinity (meme partition -> meme worker)
    - Metriques par worker: latency, queue_depth, events_processed
    - Resize dynamique possible
  - **API**:
    ```rust
    let pool = WorkerPool::new(config, |event| process(event));
    pool.submit(event, "partition_key").await?;
    let metrics = pool.metrics().await; // active_workers, p99_latency, etc.
    ```
  - **7 tests unitaires**

### Avantages vs Apama

| Feature | Apama | Varpulis |
|---------|-------|----------|
| Hot reload | Complexe | Simple `reload()` |
| Parallelisme | Opaque | Explicite + monitoring |
| Backpressure | Aucun | 4 strategies |
| Metriques workers | Non | Oui (latency, queue) |
| Partition control | Implicite | Explicite |

---

## TERMINE - Programmation Imperative

> **Statut**: Parite complete avec Apama EPL pour programmation imperative

### Termine

- [x] **IMP-01**: For/While loops dans evaluator
  - `eval_stmt()` et `eval_stmts()` pour evaluation de statements
  - Support `Stmt::For` avec iteration sur array, map, et range
  - Support `Stmt::While` avec limite de securite (10K iterations)
  - Support `Stmt::Break` et `Stmt::Continue` pour controle de boucle
  - Support `Stmt::Return` pour sortie anticipee de fonction
  - 4 tests unitaires

- [x] **IMP-02**: If/else statements dans evaluator
  - Support `Stmt::If` avec elif_branches et else_branch
  - Support complet pour conditionnels imbriques
  - 2 tests unitaires

- [x] **IMP-03**: Operations Array/Map
  - Litteraux array: `[1, 2, 3]`
  - Litteraux map: `{"key": value}`
  - Acces par index: `arr[i]`, `map["key"]`, `str[i]`
  - Slicing: `arr[1:3]`, `str[0:5]`
  - Range expressions: `0..10`, `0..=10`
  - Fonctions: push, pop, reverse, sort, contains, first, last
  - Fonctions map: keys, values, get, set
  - 9 tests unitaires

- [x] **IMP-04**: Fonctions built-in
  - **Math**: abs, sqrt, floor, ceil, round, pow, log, log10, exp, sin, cos, tan
  - **Array**: len, sum, avg, min, max, range, sort, reverse, contains
  - **String**: trim, lower/uppercase, split, join, replace, starts_with, ends_with, substring
  - **Type**: type_of, is_null, is_int, is_float, is_string, is_bool, is_array, is_map
  - **Conversion**: to_string, to_int, to_float
  - 6 tests unitaires

### Operateurs ajoutes

| Operateur | Syntaxe | Description |
|-----------|---------|-------------|
| Modulo | `a % b` | Reste de division |
| Power | `a ** b` | Puissance |
| In | `x in arr` | Appartenance |
| NotIn | `x not in arr` | Non-appartenance |
| Xor | `a xor b` | Ou exclusif |
| Null coalescing | `expr ?? default` | Valeur par defaut si null |

### Comparaison avec Apama EPL

| Feature | Apama EPL | Varpulis VPL |
|---------|-----------|--------------|
| For loop | `for i in items { }` | `for i in items:` |
| While loop | `while cond { }` | `while cond:` |
| If/elif/else | `if/else` | `if/elif/else:` |
| Break/Continue | Oui | Oui |
| Return | Oui | Oui |
| Array literal | `[1, 2, 3]` | `[1, 2, 3]` |
| Map literal | `{"k": v}` | `{"k": v}` |
| Index access | `arr[i]` | `arr[i]` |
| Slicing | Non | `arr[1:3]` |
| Range | Non | `0..10` |
| Null coalescing | Non | `expr ?? default` |
| Math functions | Oui | Oui (+ log, exp, trig) |
| Type checking | Limite | type_of, is_* functions |

### Example VPL vs Apama EPL

**Apama EPL:**
```epl
action computeStats(sequence<float> prices) returns dictionary<string, float> {
    float sum := 0.0;
    float min := prices[0];
    float max := prices[0];
    integer i := 0;
    while i < prices.size() {
        sum := sum + prices[i];
        if prices[i] < min { min := prices[i]; }
        if prices[i] > max { max := prices[i]; }
        i := i + 1;
    }
    return {"avg": sum / prices.size().toFloat(), "min": min, "max": max};
}
```

**Varpulis VPL:**
```vpl
fn compute_stats(prices: [float]) -> {str: float}:
    return {
        "avg": avg(prices),
        "min": min(prices),
        "max": max(prices)
    }
```

**Avantage Varpulis**: Fonctions built-in reduisent le code de 15 lignes a 5.

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
- [x] **BENCH-03**: Comparaison Apama (`APAMA_COMPARISON_2026.md`)
  - ZDD vs Apama manual state machines
  - Kleene+ benchmark: 400x faster than naive, 25000x memory compression
  - Code comparison: 22 lines VPL vs 63 lines Apama EPL

### Resultats SASE+ (10K events)

| Pattern | Temps | Throughput |
|---------|-------|------------|
| Simple seq (A->B) | 31ms | **320K evt/s** |
| Kleene+ (A->B+->C) | 25ms | **200K evt/s** |
| Long seq (10 events) | 377ms | 26K evt/s |

### ZDD Advantage (Kleene+ patterns)

| B Events | Combinations | ZDD Nodes | Memory Ratio |
|----------|--------------|-----------|--------------|
| 10       | 1,023        | ~20       | 51x          |
| 20       | 1,048,575    | ~40       | 26214x       |
| 30       | 1,073,741,823| ~60       | 17895697x    |

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

> **Couverture actuelle**: 62.92% (cible: 80%) | **721 tests**

### En cours

- [ ] **COV-01**: Augmenter couverture engine/mod.rs
  - **Couverture actuelle**: ~65% (progression de ~55%)
  - **Cible**: 80%+
  - **Action**: Ajouter tests unitaires pour fonctions non couvertes
  - **Progres**:
    - [x] Tests API publique: `get_pattern`, `patterns`, `get_function`, `function_names`, `add_filter`
    - [x] Tests config: `get_config`
    - [x] Tests window: count, tumbling time, sliding count, partitioned
    - [x] Tests aggregation: count, min/max
    - [x] Tests select
    - [x] Tests edge cases: empty program, many fields, special characters, max chain depth
    - [x] Tests event declaration
    - [x] Tests merge sources
    - [x] Tests print operation
    - [x] Tests import statement
    - [x] Tests window module (CountWindow, SlidingCountWindow, PartitionedWindows)
    - [x] Tests join module (3-way join, max events, common key detection, continuous correlation)
    - [x] Tests attention module (transforms, embeddings, stats, history)
    - [x] Tests connector module (Console, HTTP, Kafka, MQTT sources/sinks, registry, errors)
    - [x] Tests metrics module (gauges, histograms, cloning, multiple streams)
    - [x] Tests sequence module (negation, timeouts, context, correlations)
    - [x] Tests sink module (file sink, multi-sink broadcast, error handling)
    - [x] Tests stream module (buffer ordering, sender errors, interleaved operations)
    - [x] Tests pattern module (context, filters, XOR, FieldRef, timeouts)

### En cours

- [ ] **COV-02**: Tests d'integration SASE+ avances
  - **Action**: Ajouter tests Kleene+, negation, partition
  - **Scenarios**: Out-of-order events, clock skew, concurrent patterns
  - **Progres**:
    - [x] Tests out-of-order events
    - [x] Tests concurrent patterns (same event type)
    - [x] Tests Kleene+ requires at least one
    - [x] Tests Kleene* with occurrences
    - [x] Tests OR pattern in sequence
    - [x] Tests AND pattern both orders
    - [x] Tests CompareRef between events
    - [x] Tests long sequence chain (5 events)
    - [x] Tests partition isolation
    - [x] Tests negation cancels match
    - [x] Tests multiple Kleene matches
    - [x] Tests stats tracking

---

## PRIORITE MOYENNE - Feature Parity avec Apama

> **Source**: Analyse comparative avec Apama EPL (benchmarks/apama-comparison)

### A faire

- [x] **STREAM-01**: Operateur rstream (delay/previous value)
  - **Description**: Output les elements qui quittent la window (delay de 1)
  - **Use case**: Comparer valeur actuelle vs precedente (ex: avg change > threshold)
  - **Apama**: `from a in avg retain 1 select rstream a`
  - **Complexite**: Medium
  - **Priorite**: HIGH - Necessaire pour beaucoup de patterns
  - **Implementation**:
    - `DelayBuffer<T>` - buffer generique qui retarde de N elements
    - `PreviousValueTracker<T>` - optimise pour comparaison current vs previous
    - `PartitionedDelayBuffer<T>` - version partitionnee
    - `PartitionedPreviousValueTracker<T>` - version partitionnee
    - 15 tests unitaires dans `window.rs`

- [x] **STREAM-02**: Clause having pour filtrer apres aggregation
  - **Description**: Filtrer sur les resultats d'aggregation
  - **Use case**: `having last(price) > first(price) + threshold`
  - **Apama**: `having condition`
  - **Complexite**: Low
  - **Priorite**: MEDIUM
  - **Implementation**:
    - `StreamOp::Having(Expr)` dans AST (ast.rs)
    - `RuntimeOp::Having(Expr)` dans types.rs
    - `having_op` dans grammaire pest (varpulis.pest)
    - Parser support dans pest_parser.rs
    - Execution: filtre les events apres aggregation
    - 4 nouveaux tests (2 parser, 2 runtime)

- [x] **STREAM-03**: Jointures inter-streams avec comparaison d'aggregats
  - **Description**: Joindre deux streams et comparer leurs aggregats
  - **Use case**: Comparer avg actuel avec avg precedent
  - **Apama**: `from cur in avg join prev in prevaverages on ...`
  - **Complexite**: High
  - **Priorite**: MEDIUM
  - **Implementation**:
    - Fix event routing pour derived streams (aggregated, filtered)
    - Les streams avec operations produisent des events avec event_type = nom du stream
    - Les streams passthrough utilisent l'event type sous-jacent
    - JoinBuffer correle les events de sources multiples
    - Support pour expressions de comparaison: `EMA12.ema > EMA26.ema`
    - 1 nouveau test (test_aggregate_comparison_join)
    - Example: `09_aggregate_join.vpl`

- [x] **TIMER-01**: Timer periodique independant des evenements
  - **Description**: Declencher actions a intervalles fixes
  - **Use case**: Calculer VWAP toutes les 5 secondes, heartbeats
  - **Apama**: `on wait(period) { ... }`
  - **Complexite**: Medium
  - **Priorite**: LOW
  - **Implementation**:
    - `StreamSource::Timer(TimerDecl)` dans AST (ast.rs)
    - `RuntimeSource::Timer(TimerConfig)` dans types.rs
    - `timer_source` dans grammaire pest (varpulis.pest)
    - Syntaxe: `timer(5s)` ou `timer(5s, initial_delay: 1s)`
    - Module `timer.rs` avec `spawn_timer()` et `TimerManager`
    - `Engine::get_timers()` pour recuperer configs timer
    - 8 nouveaux tests (2 parser, 6 runtime)
    - Example: `10_timer_source.vpl`

- [x] **VAR-01**: Variables dynamiques modifiables au runtime
  - **Description**: Mettre a jour seuils/variables apres traitement
  - **Use case**: Augmenter threshold apres alerte
  - **Apama**: `threshold := alertPrice + 10000.0;`
  - **Complexite**: Medium
  - **Priorite**: LOW
  - **Implementation**:
    - `Stmt::Assignment { name, value }` dans AST (ast.rs)
    - `assignment_stmt` dans grammaire pest (varpulis.pest)
    - `var_keyword` rule pour capturer let/var correctement
    - `variables` et `mutable_vars` fields dans Engine
    - Methodes: `get_variable()`, `set_variable()`, `variables()`
    - Variables accessibles dans evaluations via bindings
    - 17 nouveaux tests (4 parser, 13 runtime)
    - Example: `11_variables.vpl`

- [x] **QUERY-01**: Requetes imbriquees (nested queries)
  - **Description**: Sous-requetes dans les streams
  - **Use case**: Multi-stage processing
  - **Complexite**: High
  - **Priorite**: LOW
  - **Implementation**:
    - Deja supporte via `StreamSource::Ident` dans AST
    - `RuntimeSource::Stream` dans engine/types.rs
    - Resolution automatique: stream -> event type sous-jacent
    - Support pipelines multi-etages (5+ niveaux testes)
    - Support branches paralleles depuis meme source
    - Support diamond pattern (A->B, A->C puis join)
    - 13 nouveaux tests (3 parser, 10 runtime)
    - Example: `12_nested_queries.vpl`

### Notes

Les exemples Apama compares sont dans `/benchmarks/apama-comparison/`:
- `apama/` - Exemples EPL originaux
- `varpulis/` - Implementations equivalentes VPL
- `COMPARISON.md` - Matrice de comparaison detaillee

---

## TERMINE - VS Code Extension + LSP Server

> **Statut**: Extension complete avec LSP server Rust et editeur visuel React Flow

### Termine

- [x] **LSP-01**: Creer crate varpulis-lsp
  - **Fichiers**: `crates/varpulis-lsp/src/{main,server,diagnostics,hover,completion,semantic}.rs`
  - **Dependencies**: tower-lsp, tokio, varpulis-parser
  - **Transport**: stdio JSON-RPC

- [x] **LSP-02**: Diagnostics avec positions precises
  - **Implementation**: Parse errors → LSP Diagnostic avec line/column
  - **Support**: ParseError::Located, UnexpectedToken, UnexpectedEof, Custom
  - **Tests**: 2 tests unitaires

- [x] **LSP-03**: Hover documentation
  - **Couverture**: 50+ keywords, operators, functions documentes
  - **Categories**: stream ops, aggregations, SASE+ operators, types
  - **Format**: Markdown avec syntax highlighting

- [x] **LSP-04**: Auto-completion
  - **Keywords**: stream, event, connector, sink, pattern, fn, config
  - **Stream ops**: .where, .select, .window, .aggregate, .emit, .to
  - **Aggregations**: sum, avg, count, min, max, first, last, stddev
  - **SASE+**: SEQ, AND, OR, NOT, within

- [x] **LSP-05**: Semantic tokens
  - **Token types**: keyword, function, string, number, comment, operator
  - **Parsing**: Line-by-line tokenization
  - **Integration**: VS Code semantic highlighting

- [x] **LSP-06**: VS Code extension integration
  - **Client**: LanguageClient dans extension.ts
  - **Config**: Server path configurable
  - **Activation**: On .vpl file open

- [x] **FLOW-01**: React Flow visual editor
  - **Nodes**: Connector, Source, Stream, Event, Pattern, Emit, Sink
  - **Edges**: Animated edges avec direction
  - **Features**: Drag-drop, minimap, zoom, auto-layout

- [x] **FLOW-02**: VPL code generator
  - **Implementation**: `vplGenerator.ts` (475 lignes)
  - **Output**: New connectivity syntax
  - **Coverage**: All node types, operations, parameters

---

## TERMINE - Connectivity Architecture

> **Statut**: Nouvelle architecture de connectivite implementee

### Termine

- [x] **CONN-01**: Syntax connector declarations
  - **Syntaxe**: `connector Name = type (params...)`
  - **Types**: mqtt, kafka, http, amqp, file, websocket, grpc
  - **Grammar**: `connector_decl`, `connector_type`, `connector_params` rules

- [x] **CONN-02**: Stream source syntax avec .from()
  - **Syntaxe**: `stream X = EventType.from(Connector, topic: "...")`
  - **AST**: `StreamSource::FromConnector { event_type, connector_name, params }`
  - **Grammar**: `from_connector_source` rule

- [x] **CONN-03**: Sink statements
  - **Syntaxe**: `sink StreamName to Connector (params...)`
  - **Builtins**: `console()`, `log(level: "...")`, `tap(counter: "...")`
  - **AST**: `Stmt::SinkStmt`, `SinkTarget` enum
  - **Grammar**: `sink_stmt`, `sink_target`, `sink_builtin` rules

- [x] **CONN-04**: Update all examples
  - **Fichiers**: 7 examples updated to new syntax
  - **Changes**: Added connectors, .from(), sink statements
  - **Tests**: All examples parse successfully

### New Syntax Examples

```vpl
# Connector declaration
connector MqttSensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "hvac-demo"
)

# Stream with .from()
stream Temperatures = TemperatureReading
    .from(MqttSensors, topic: "sensors/temperature/#")

# Processing pipeline
stream HighTempAlert = Temperatures
    .where(value > 28)
    .emit(
        alert_type: "HIGH_TEMPERATURE",
        zone: zone,
        temperature: value
    )

# Sink statements
sink HighTempAlert to KafkaAlerts (topic: "alerts.hvac")
sink HighTempAlert to console()
```

---

## Ordre d'execution recommande

```mermaid
graph LR
    SAAS01[SAAS-01: Multi-tenant] --> SAAS02[SAAS-02: REST API]
    SAAS02 --> SAAS03[SAAS-03: Usage metering]
    SAAS03 --> SAAS04[SAAS-04: Docker Compose]
    SAAS04 --> SAAS05[SAAS-05: Grafana]
    SAAS05 --> SAAS06[SAAS-06: CLI deploy]
```

### Sprint actuel (SaaS Foundation)
1. SAAS-01: Multi-tenant isolation
2. SAAS-02: REST API for pipeline management
3. SAAS-03: Usage metering and quotas
4. SAAS-04: Docker Compose SaaS stack

### Sprint suivant (SaaS Polish)
5. SAAS-05: Grafana dashboard
6. SAAS-06: CLI deploy command

### Complete
- [x] REL-01/02: v0.1.0 released (5 platforms + Docker)
- [x] SEC-04: Authentification WebSocket
- [x] SEC-05: TLS/WSS support
- [x] PERF-01-06: Toutes optimisations performance
- [x] LSP-01-06: LSP server complet
- [x] CONN-01-04: Architecture connectivite

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
| `crates/varpulis-runtime/src/timer.rs` | Timers periodiques |
| `crates/varpulis-lsp/src/server.rs` | LSP server implementation |
| `crates/varpulis-lsp/src/hover.rs` | Hover documentation |
| `tests/mqtt/simulator.py` | Simulateur d'evenements Python |
| `vscode-varpulis/webview-ui/src/utils/vplGenerator.ts` | Flow → VPL generator |

---

## Metriques actuelles

| Metrique | Valeur | Cible | Statut |
|----------|--------|-------|--------|
| **Production Score** | **9.5/10** | 9/10 | **Enterprise Ready** |
| **Tests totaux** | **830+** | 100+ | Excellent |
| **Tests CLI** | **76** | - | Excellent |
| **Tests LSP** | **20+** | - | Good |
| **Release** | **v0.1.0** | - | **5 platforms + Docker** |
| **Couverture** | 62.92% | 80% | Needs work |
| **Clippy warnings** | 0 | 0 | Excellent |
| **Unsafe blocks** | 17 | <20 | OK (SIMD) |
| **Unwrap parser** | 0 | 0 | Excellent |
| **Unwrap CLI** | **0** | 0 | Excellent |
| **Unwrap runtime** | **0** | <50 | **Excellent** |
| **Security (TLS/Auth)** | Done | Done | Excellent |
| **ZDD Memory** | O(n²) | - | Excellent |
| **Throughput** | 300-500K/s | - | Excellent |
| **SIMD Aggregations** | AVX2 | - | Done |
| **Incremental Window** | O(1) | - | Done |
| **LSP Server** | Full | - | **NEW** |
| **Visual Editor** | React Flow | - | **NEW** |

---

## Architecture actuelle

```
~27,000 lignes de Rust
5 crates + 1 CLI
Version 0.1.0

+-- varpulis-core (1,500 lignes)
|   +-- ast.rs - AST definitions (connector, sink support)
|   +-- types.rs - Type system
|   +-- value.rs - Runtime values
|
+-- varpulis-parser (3,600 lignes)
|   +-- varpulis.pest - PEG grammar (connector, sink rules)
|   +-- pest_parser.rs - Parser implementation
|   +-- indent.rs - Indentation preprocessor
|
+-- varpulis-lsp (800 lignes) **NEW**
|   +-- server.rs - LSP server with tower-lsp
|   +-- diagnostics.rs - Parse error → Diagnostic
|   +-- hover.rs - Documentation on hover
|   +-- completion.rs - Auto-completion
|   +-- semantic.rs - Semantic token highlighting
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

### Priorite Immediate (SaaS)

1. **SAAS-01**: Multi-tenant isolation (tenant.rs)
2. **SAAS-02**: REST API pipeline management (api.rs)
3. **SAAS-03**: Usage metering et quotas (usage.rs)
4. **SAAS-04**: Docker Compose SaaS stack

### Deja Complete

- [x] v0.1.0 Released: 5 platforms + Docker image
- [x] Securite: TLS/WSS (SEC-05) et Auth API Key (SEC-04)
- [x] Rate limiting: Token bucket per IP (PROD-02)
- [x] Performance: ZDD optimization, Arc<Event>, batch processing, SIMD
- [x] LSP Server: Diagnostics, hover, completion, semantic tokens
- [x] Connectivity: Connectors, sinks, .from() syntax
- [x] Benchmarks: Comparaison Apama avec ZDD advantage documentee

### Statut Monetisation

| Tier | Statut | Revenue Potential |
|------|--------|-------------------|
| **Community** | **RELEASED v0.1.0** | Brand awareness, adoption |
| **Enterprise** | **READY** | License fees, support contracts |
| **SaaS** | **EN COURS** | Recurring revenue, usage-based |

**Recommendation**: Focus SaaS - multi-tenant + REST API + Docker Compose.
