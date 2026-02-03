# Varpulis Runtime — Analyse Architecturale Complète

**Date :** Février 2026  
**Auteur :** Analyse technique pour Cyril  
**Scope :** `crates/varpulis-runtime/src/*`

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Analyse par module](#2-analyse-par-module)
   - [event.rs](#21-eventrs)
   - [stream.rs](#22-streamrs)
   - [window.rs](#23-windowrs)
   - [aggregation.rs](#24-aggregationrs)
   - [pattern.rs](#25-patternrs)
   - [sase.rs](#26-sasers)
   - [attention.rs](#27-attentionrs)
   - [join.rs](#28-joinrs)
   - [sink.rs](#29-sinkrs)
   - [timer.rs](#210-timerrs)
3. [Problème architectural majeur : Duplication des moteurs de pattern](#3-problème-architectural-majeur)
4. [Matrice de qualité](#4-matrice-de-qualité)
5. [Problèmes par priorité](#5-problèmes-par-priorité)
6. [Recommandations stratégiques](#6-recommandations-stratégiques)
7. [Plan d'action consolidé](#7-plan-daction-consolidé)

---

## 1. Vue d'ensemble

### Architecture actuelle

```
varpulis-runtime/
├── event.rs          # Types Event, SharedEvent (Arc<Event>)
├── stream.rs         # Abstraction tokio::mpsc pour streams
├── window.rs         # Tumbling, Sliding, Count, Partitioned windows
├── aggregation.rs    # Count, Sum, Avg, Min, Max, StdDev, EMA, etc.
├── pattern.rs        # Moteur de patterns AST-based (EPL-like)
├── sase.rs           # Moteur SASE+ NFA-based avec Kleene
├── attention.rs      # Mécanisme d'attention pour corrélation
├── join.rs           # Join temporel multi-streams
├── sink.rs           # Console, File, HTTP, Multi sinks
└── timer.rs          # Timers périodiques
```

### Points forts globaux

| Aspect | Évaluation | Détails |
|--------|------------|---------|
| **Zero-copy events** | ✅ Excellent | `SharedEvent = Arc<Event>` partout |
| **Algorithmes numériques** | ✅ Excellent | Welford pour stddev, EMA single-pass |
| **Structure de données** | ✅ Bon | VecDeque pour FIFO, IndexMap pour ordre |
| **Async runtime** | ✅ Bon | Tokio bien intégré |
| **Tests** | ✅ Bon | Couverture correcte |

### Points faibles globaux

| Aspect | Évaluation | Détails |
|--------|------------|---------|
| **Duplication pattern matching** | ⚠️ Problème | pattern.rs + sase.rs font la même chose |
| **Attention engine** | ⚠️ Problème | O(n²) et cache LRU en O(n) |
| **I/O async** | ⚠️ Problème | FileSink bloque le runtime |
| **Observabilité** | ⚠️ Faible | Métriques insuffisantes |

---

## 2. Analyse par module

### 2.1 event.rs

**Rôle :** Définition du type `Event` et types de domaine (HVAC).

```rust
pub type SharedEvent = Arc<Event>;

pub struct Event {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub data: IndexMap<String, Value>,
}
```

**Évaluation : ✅ Bon**

| Aspect | Statut | Notes |
|--------|--------|-------|
| Zero-copy via Arc | ✅ | Évite les clones dans les pipelines |
| IndexMap | ✅ | Préserve l'ordre d'insertion |
| Helpers get_* | ✅ | API ergonomique |
| Types domaine | ✅ | TemperatureReading, HVACStatus, etc. |

**Problème potentiel :**

```rust
pub data: IndexMap<String, Value>
```

`IndexMap` est ~30% plus lent que `HashMap` pour les lookups. Acceptable si < 50 champs/event.

**Recommandation :** Garder tel quel. Le trade-off ordre/performance est raisonnable.

---

### 2.2 stream.rs

**Rôle :** Abstraction simple autour de `tokio::mpsc`.

```rust
pub struct Stream {
    pub name: String,
    receiver: mpsc::Receiver<Event>,
    buffer: VecDeque<Event>,
}
```

**Évaluation : ✅ Correct**

Module minimaliste, fait son travail. Le buffer interne permet le push_back pour replay.

**Pas de problème.**

---

### 2.3 window.rs

**Rôle :** Fenêtres temporelles et par count pour l'agrégation.

**Types disponibles :**
- `TumblingWindow` — fenêtre fixe sans overlap
- `SlidingWindow` — fenêtre avec overlap
- `CountWindow` — N événements
- `SlidingCountWindow` — N événements avec slide
- `PartitionedTumblingWindow` / `PartitionedSlidingWindow`
- `DelayBuffer` — équivalent Apama `rstream`
- `PreviousValueTracker` — comparaison current/previous

**Évaluation : ✅ Très bon**

| Aspect | Statut | Notes |
|--------|--------|-------|
| VecDeque pour sliding | ✅ | O(1) push/pop |
| SharedEvent | ✅ | Évite les clones |
| Expiry efficace | ✅ | `partition_point` pour binary search |
| DelayBuffer | ✅ | Design élégant pour rstream |

**Code exemplaire — PreviousValueTracker :**

```rust
impl<T: Clone> PreviousValueTracker<T> {
    pub fn update(&mut self, value: T) {
        self.previous = self.current.take();
        self.current = Some(value);
    }
    
    pub fn get_pair(&self) -> Option<(&T, &T)> {
        match (&self.current, &self.previous) {
            (Some(curr), Some(prev)) => Some((curr, prev)),
            _ => None,
        }
    }
}
```

Permet le pattern Apama classique :
```
from a in averages 
from p in (from a in averages retain 1 select rstream a)
where a > p + threshold
```

**Problème mineur :**

```rust
let key = event
    .get(&self.partition_key)
    .map(|v| format!("{}", v))  // Allocation à chaque event !
    .unwrap_or_else(|| "default".to_string());
```

**Fix suggéré :**
```rust
// Option 1: String interning
let key = event
    .get(&self.partition_key)
    .map(|v| intern_string(&format!("{}", v)))
    .unwrap_or(INTERNED_DEFAULT);

// Option 2: Hash direct
let key_hash = event
    .get(&self.partition_key)
    .map(|v| {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    })
    .unwrap_or(0);
```

**Impact :** Faible. À optimiser si profilage montre un hotspot.

---

### 2.4 aggregation.rs

**Rôle :** Fonctions d'agrégation sur fenêtres d'événements.

**Agrégations disponibles :**
- `Count`, `Sum`, `Avg`, `Min`, `Max`
- `StdDev` (Welford's algorithm)
- `First`, `Last`
- `CountDistinct` (hash-based)
- `Ema` (Exponential Moving Average)
- `ExprAggregate` (composition d'agrégations)

**Évaluation : ✅ Excellent**

| Aspect | Statut | Notes |
|--------|--------|-------|
| Welford's algorithm | ✅ | Numériquement stable, single-pass |
| NaN handling | ✅ | Filter explicite pour min/max |
| EMA single-pass | ✅ | Efficace |
| CountDistinct | ✅ | Hash-only, évite clones de Value |
| Trait AggregateFunc | ✅ | Extensible |

**Code exemplaire — Welford's algorithm :**

```rust
impl AggregateFunc for StdDev {
    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        
        // Welford's online algorithm
        let mut count = 0usize;
        let mut mean = 0.0;
        let mut m2 = 0.0;

        for event in events {
            if let Some(x) = event.get_float(field) {
                count += 1;
                let delta = x - mean;
                mean += delta / count as f64;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
        }

        if count < 2 { return Value::Null; }
        
        let variance = m2 / (count - 1) as f64;
        Value::Float(variance.sqrt())
    }
}
```

**Pas de problème.** Module de référence.

---

### 2.5 pattern.rs

**Rôle :** Moteur de pattern matching EPL-like.

**Opérateurs supportés :**
- `Event { type, filter, alias }` — template d'événement
- `FollowedBy(A, B)` — A puis B
- `And(A, B)` — A et B (any order)
- `Or(A, B)` — A ou B
- `Xor(A, B)` — A xor B
- `Not(A)` — négation
- `All(A)` — tous les A
- `Within(A, duration)` — contrainte temporelle

**Évaluation : ⚠️ Problématique**

| Aspect | Statut | Notes |
|--------|--------|-------|
| AST propre | ✅ | PatternExpr bien structuré |
| Builder DSL | ✅ | API ergonomique |
| Filter evaluation | ✅ | Correct |
| **AND operator** | ❌ | Gestion any-order incomplète |
| **Pas de Kleene** | ❌ | Manque A+, A* |
| **Duplication avec sase.rs** | ⚠️ | Deux moteurs pour la même chose |

**Problème AND :**

```rust
fn eval_and(...) -> EvalResult {
    match state {
        EvalState::Initial => {
            let left_result = evaluate_pattern(left, ...);
            let right_result = evaluate_pattern(right, ...);
            
            match (&left_result, &right_result) {
                (EvalResult::Match(l_ctx), EvalResult::Match(r_ctx)) => {
                    // OK si les deux matchent sur LE MÊME événement
                    let mut combined = l_ctx.clone();
                    combined.merge(r_ctx);
                    EvalResult::Match(combined)
                }
                // ...
            }
        }
        // ...
    }
}
```

Le problème : `AND(A, B)` ne gère pas correctement le cas où A et B arrivent sur des événements différents dans n'importe quel ordre. C'est le même bug que dans sase.rs.

---

### 2.6 sase.rs

**Analysé en détail dans `varpulis-sase-improvements.md`.**

**Résumé des problèmes :**

| Problème | Sévérité | Document de référence |
|----------|----------|----------------------|
| Kleene explosion O(2ⁿ) | Critique | varpulis-zdd-specification.md |
| Négation incomplète | P1 | varpulis-sase-improvements.md §1 |
| AND incorrect | P1 | varpulis-sase-improvements.md §2 |
| Pas d'indexation | P2 | varpulis-sase-improvements.md §3 |
| Pas de backpressure | P1 | varpulis-sase-improvements.md §5 |

---

### 2.7 attention.rs

**Analysé en détail dans `varpulis-sase-improvements.md` §10.**

**Résumé des problèmes :**

| Problème | Sévérité | Fix |
|----------|----------|-----|
| Cache LRU O(n) | P1 | crate `lru` |
| History O(n) removal | P1 | VecDeque |
| Projections recalculées | P1 | Pré-calcul Key/Value |
| Pas de SIMD | P2 | AVX2 vectorization |
| Pas d'ANN | P3 | HNSW index |

---

### 2.8 join.rs

**Rôle :** Join temporel entre streams sur une clé commune.

```rust
pub struct JoinBuffer {
    buffers: HashMap<String, KeyedEventBuffer>,  // source → (key_value → events)
    sources: Vec<String>,
    join_keys: HashMap<String, String>,          // source → field_name
    window_duration: Duration,
    max_events_per_key: usize,
}
```

**Évaluation : ⚠️ Correct mais pas scalable**

| Aspect | Statut | Notes |
|--------|--------|-------|
| Design | ✅ | Correct pour temporal join |
| Multi-way join | ✅ | Supporte N sources |
| Auto-detection clé | ✅ | Fallback sur "symbol", "key", etc. |
| **Scalabilité** | ⚠️ | O(n) cleanup, pas d'index temporel |

**Problème — cleanup_expired :**

```rust
fn cleanup_expired(&mut self, current_time: DateTime<Utc>) {
    let cutoff = current_time - self.window_duration;

    for source_buffer in self.buffers.values_mut() {
        for key_events in source_buffer.values_mut() {
            // Binary search OK
            let cutoff_idx = key_events.partition_point(|(ts, _)| *ts < cutoff);
            if cutoff_idx > 0 {
                key_events.drain(..cutoff_idx);
            }
        }
        // Mais ici on itère sur TOUTES les clés
        source_buffer.retain(|_, events| !events.is_empty());
    }
}
```

Appelé à **chaque événement**. Avec 10000 clés distinctes, c'est 10000 itérations par event.

**Fix suggéré :**

```rust
pub struct JoinBuffer {
    // ... existing fields
    
    // Index temporel pour GC efficace
    expiry_queue: BinaryHeap<Reverse<(DateTime<Utc>, String, String)>>,  // (expiry_time, source, key)
    last_gc: DateTime<Utc>,
    gc_interval: Duration,
}

fn cleanup_expired(&mut self, current_time: DateTime<Utc>) {
    // GC périodique au lieu de systématique
    if current_time - self.last_gc < self.gc_interval {
        return;
    }
    self.last_gc = current_time;
    
    let cutoff = current_time - self.window_duration;
    
    // Drain uniquement les entrées expirées de la queue
    while let Some(Reverse((expiry, source, key))) = self.expiry_queue.peek() {
        if *expiry > cutoff {
            break;
        }
        
        let (_, source, key) = self.expiry_queue.pop().unwrap().0;
        
        if let Some(source_buffer) = self.buffers.get_mut(&source) {
            if let Some(key_events) = source_buffer.get_mut(&key) {
                let cutoff_idx = key_events.partition_point(|(ts, _)| *ts < cutoff);
                if cutoff_idx > 0 {
                    key_events.drain(..cutoff_idx);
                }
                if key_events.is_empty() {
                    source_buffer.remove(&key);
                }
            }
        }
    }
}
```

**Impact :** Moyen. À optimiser si > 1000 clés distinctes.

---

### 2.9 sink.rs

**Rôle :** Output des événements vers console, fichiers, HTTP.

**Types disponibles :**
- `ConsoleSink` — stdout avec pretty-print
- `FileSink` — JSON lines vers fichier
- `HttpSink` — webhook POST
- `MultiSink` — broadcast vers plusieurs sinks

**Évaluation : ⚠️ Problèmes async**

| Aspect | Statut | Notes |
|--------|--------|-------|
| Trait Sink | ✅ | API propre |
| ConsoleSink | ✅ | Correct |
| **FileSink** | ❌ | Bloque le runtime async |
| **HttpSink** | ⚠️ | Pas de retry, erreurs silencieuses |
| MultiSink | ✅ | Correct |

**Problème FileSink :**

```rust
use std::fs::{File, OpenOptions};  // BLOCKING I/O !
use std::io::Write;

impl Sink for FileSink {
    async fn send(&self, event: &Event) -> Result<()> {
        let json = serde_json::to_string(event)?;
        let mut file = self.file.lock().await;
        writeln!(file, "{}", json)?;  // BLOCKING WRITE dans un contexte async !
        Ok(())
    }
}
```

`std::fs::File::write` bloque le thread. Dans un runtime tokio, ça bloque le worker thread entier.

**Fix — AsyncFileSink :**

```rust
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub struct AsyncFileSink {
    name: String,
    path: PathBuf,
    file: Arc<Mutex<tokio::fs::File>>,
    buffer: Arc<Mutex<Vec<u8>>>,  // Buffer pour batching
    buffer_size: usize,
}

impl AsyncFileSink {
    pub async fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            name: name.into(),
            path,
            file: Arc::new(Mutex::new(file)),
            buffer: Arc::new(Mutex::new(Vec::with_capacity(64 * 1024))),
            buffer_size: 64 * 1024,  // 64KB buffer
        })
    }
}

#[async_trait]
impl Sink for AsyncFileSink {
    async fn send(&self, event: &Event) -> Result<()> {
        let json = serde_json::to_string(event)?;
        
        let should_flush = {
            let mut buffer = self.buffer.lock().await;
            buffer.extend_from_slice(json.as_bytes());
            buffer.push(b'\n');
            buffer.len() >= self.buffer_size
        };
        
        if should_flush {
            self.flush().await?;
        }
        
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let data = {
            let mut buffer = self.buffer.lock().await;
            std::mem::take(&mut *buffer)
        };
        
        if !data.is_empty() {
            let mut file = self.file.lock().await;
            file.write_all(&data).await?;
            file.flush().await?;
        }
        
        Ok(())
    }
}
```

**Problème HttpSink :**

```rust
match req.send().await {
    Ok(resp) => {
        if !resp.status().is_success() {
            error!("HTTP sink {} got status {}", self.name, resp.status());
            // Pas de retry, événement perdu !
        }
    }
    Err(e) => {
        error!("HTTP sink {} error: {}", self.name, e);
        // Pas de retry, événement perdu !
    }
}
Ok(())  // Retourne Ok même en cas d'erreur !
```

**Fix — HttpSink avec retry :**

```rust
pub struct HttpSinkConfig {
    pub url: String,
    pub headers: IndexMap<String, String>,
    pub max_retries: usize,
    pub retry_delay: Duration,
    pub timeout: Duration,
}

impl HttpSink {
    async fn send_with_retry(&self, event: &Event) -> Result<()> {
        let json = serde_json::to_vec(event)?;
        
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_delay * attempt as u32).await;
            }
            
            let mut req = self.client
                .post(&self.config.url)
                .timeout(self.config.timeout);
                
            for (k, v) in &self.config.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            
            match req.body(json.clone()).send().await {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(());
                }
                Ok(resp) if resp.status().is_server_error() => {
                    // 5xx: retry
                    warn!("HTTP sink {} got {}, retrying", self.name, resp.status());
                    continue;
                }
                Ok(resp) => {
                    // 4xx: don't retry
                    return Err(anyhow!("HTTP sink {} got {}", self.name, resp.status()));
                }
                Err(e) if e.is_timeout() || e.is_connect() => {
                    // Network error: retry
                    warn!("HTTP sink {} error: {}, retrying", self.name, e);
                    continue;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        
        Err(anyhow!("HTTP sink {} failed after {} retries", self.name, self.config.max_retries))
    }
}
```

---

### 2.10 timer.rs

**Rôle :** Génération périodique d'événements timer.

```rust
pub fn spawn_timer(
    interval_ns: u64,
    initial_delay_ns: Option<u64>,
    timer_event_type: String,
    event_tx: mpsc::Sender<Event>,
) -> JoinHandle<()>
```

**Évaluation : ✅ Correct mais basique**

| Aspect | Statut | Notes |
|--------|--------|-------|
| tokio::interval | ✅ | Correct |
| Initial delay | ✅ | Supporté |
| Graceful shutdown | ✅ | Via channel close |

**Manques :**
- Pas de timer cron-like (`0 9 * * 1-5` pour 9h du lundi au vendredi)
- Pas de timer event-time (watermark-based)
- Pas de timer one-shot

**Impact :** Faible pour un CEP engine. À ajouter si besoin métier.

---

## 3. Problème architectural majeur

### Duplication des moteurs de pattern matching

**Situation actuelle :**

```
┌─────────────────────────────────────────────────────────────┐
│                    Varpulis Runtime                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   pattern.rs                      sase.rs                   │
│   ┌─────────────────┐             ┌─────────────────┐       │
│   │ PatternExpr     │             │ SasePattern     │       │
│   │ - Event         │             │ - Event         │       │
│   │ - FollowedBy    │             │ - Seq           │       │
│   │ - And           │             │ - And           │       │
│   │ - Or            │             │ - Or            │       │
│   │ - Not           │             │ - Not           │       │
│   │ - Within        │             │ - Kleene+       │       │
│   │ - All           │             │ - Within        │       │
│   └─────────────────┘             └─────────────────┘       │
│          │                               │                  │
│          ▼                               ▼                  │
│   PatternEngine                   SaseEngine                │
│   (AST evaluation)                (NFA-based)               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Problèmes :**

1. **Duplication fonctionnelle** — Les deux font du pattern matching sur streams
2. **Divergence des bugs** — AND est cassé dans les deux, mais différemment
3. **Confusion pour l'utilisateur** — Lequel utiliser ?
4. **Maintenance** — Deux codebases à maintenir
5. **Incohérence** — `pattern.rs` n'a pas Kleene, `sase.rs` n'a pas XOR

**Comparaison des fonctionnalités :**

| Feature | pattern.rs | sase.rs |
|---------|------------|---------|
| Event template | ✅ | ✅ |
| Sequence (A → B) | ✅ FollowedBy | ✅ Seq |
| AND (any order) | ⚠️ Buggy | ⚠️ Buggy |
| OR | ✅ | ✅ |
| XOR | ✅ | ❌ |
| NOT | ⚠️ Partiel | ⚠️ Partiel |
| Kleene+ | ❌ | ✅ (explosif) |
| Kleene* | ❌ | ❌ |
| Within | ✅ | ✅ |
| All | ✅ | ❌ |
| Partitioning | ❌ | ✅ (SASEXT) |
| NFA compilation | ❌ | ✅ |

### Recommandation : Consolidation vers SASE+

**Stratégie proposée :**

```
Phase 1: Enrichir SASE+
├── Ajouter XOR
├── Ajouter All (match all occurrences)
├── Fixer AND (any order)
├── Fixer NOT (confirmation watermark)
└── Implémenter ZDD pour Kleene

Phase 2: API de façade
├── Créer PatternDSL qui compile vers SASE NFA
├── Conserver la syntaxe de PatternBuilder
└── Supprimer PatternEngine

Phase 3: Déprécier pattern.rs
├── Marquer #[deprecated]
├── Documenter la migration
└── Supprimer dans une version majeure
```

**Résultat cible :**

```rust
// API utilisateur (inchangée)
let pattern = PatternBuilder::followed_by(
    PatternBuilder::event_as("NewsItem", "news"),
    PatternBuilder::event_as("StockTick", "tick"),
);

// Sous le capot: compilation vers SASE NFA
let nfa = SaseCompiler::compile(&pattern)?;
let engine = SaseEngine::new(nfa);
```

---

## 4. Matrice de qualité

| Module | Correctness | Performance | API Design | Tests | Note |
|--------|-------------|-------------|------------|-------|------|
| event.rs | ✅ | ✅ | ✅ | ✅ | A |
| stream.rs | ✅ | ✅ | ✅ | ✅ | A |
| window.rs | ✅ | ✅ | ✅ | ✅ | A |
| aggregation.rs | ✅ | ✅ | ✅ | ✅ | A+ |
| pattern.rs | ⚠️ | ✅ | ✅ | ✅ | B- |
| sase.rs | ⚠️ | ⚠️ | ✅ | ✅ | C+ |
| attention.rs | ✅ | ❌ | ✅ | ✅ | C |
| join.rs | ✅ | ⚠️ | ✅ | ✅ | B |
| sink.rs | ⚠️ | ⚠️ | ✅ | ✅ | B- |
| timer.rs | ✅ | ✅ | ✅ | ⚠️ | B+ |

---

## 5. Problèmes par priorité

### P0 — Bloquants pour production

| Module | Problème | Impact | Effort |
|--------|----------|--------|--------|
| attention.rs | Cache LRU O(n) | CPU 100x overhead | Trivial |
| attention.rs | History O(n) | CPU 1000x overhead | Trivial |
| sink.rs | FileSink bloque async | Thread starvation | Moyen |

### P1 — Correctness

| Module | Problème | Impact | Effort |
|--------|----------|--------|--------|
| sase.rs | Négation incomplète | Faux négatifs | Moyen |
| sase.rs | AND incorrect | Faux négatifs | Moyen |
| pattern.rs | AND incorrect | Faux négatifs | Moyen |
| sase.rs | Backpressure silencieuse | Perte de données | Moyen |

### P2 — Performance

| Module | Problème | Impact | Effort |
|--------|----------|--------|--------|
| sase.rs | Kleene O(2ⁿ) | Explosion mémoire | Élevé (ZDD) |
| attention.rs | Projections recalculées | CPU 2000x | Moyen |
| sase.rs | Pas d'indexation | CPU O(n×m) | Moyen |
| join.rs | Cleanup O(n) | CPU sur gros volumes | Moyen |

### P3 — Architecture

| Module | Problème | Impact | Effort |
|--------|----------|--------|--------|
| pattern.rs + sase.rs | Duplication | Maintenance | Élevé |
| sink.rs | HttpSink pas de retry | Perte données | Moyen |
| timer.rs | Pas de cron | Fonctionnalité | Faible |

---

## 6. Recommandations stratégiques

### 6.1 Court terme (1 semaine)

**Quick wins à impact maximal :**

```rust
// 1. Attention cache (5 minutes)
// Cargo.toml
[dependencies]
lru = "0.12"

// attention.rs
use lru::LruCache;
pub struct EmbeddingCache {
    cache: LruCache<u64, CacheEntry>,
    // ...
}

// 2. Attention history (5 minutes)
use std::collections::VecDeque;
pub struct AttentionEngine {
    history: VecDeque<HistoryEntry>,  // Était Vec
    // ...
}

// 3. Pré-calcul projections (2 heures)
struct HistoryEntry {
    event: Event,
    embedding: Vec<f32>,
    key_projections: Vec<Vec<f32>>,    // Pré-calculées
    value_projections: Vec<Vec<f32>>,  // Pré-calculées
}
```

**Gain estimé :** ×500 throughput sur attention engine.

### 6.2 Moyen terme (1-2 mois)

**Corrections P1 et architecture :**

1. **SASE correctness** (2-3 semaines)
   - Négation temporelle
   - AND any-order
   - Backpressure explicite

2. **ZDD pour Kleene** (3-4 semaines)
   - Voir `varpulis-zdd-specification.md`

3. **AsyncFileSink** (1 jour)
   - Remplacer std::fs par tokio::fs
   - Ajouter batching

4. **Décision pattern.rs** (1 jour)
   - Choisir : garder, fusionner, ou supprimer

### 6.3 Long terme (3+ mois)

**Optimisations avancées :**

1. **Shared Execution** pour patterns avec préfixes communs
2. **HNSW index** pour attention engine sur grands historiques
3. **SIMD vectorization** pour calculs d'attention
4. **Join index** avec B-tree temporel

---

## 7. Plan d'action consolidé

```
┌─────────────────────────────────────────────────────────────┐
│                    SEMAINE 0 (immédiat)                      │
├─────────────────────────────────────────────────────────────┤
│ □ Attention quick wins (2h)                                 │
│   □ lru crate                                               │
│   □ VecDeque                                                │
│   □ Pré-calcul Key/Value                                    │
│                                                             │
│ □ Décision architecturale pattern.rs vs sase.rs             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    SEMAINES 1-3                              │
├─────────────────────────────────────────────────────────────┤
│ En parallèle:                                               │
│                                                             │
│ ┌─────────────────┐  ┌─────────────────┐                    │
│ │ ZDD (Claude     │  │ SASE P1 fixes   │                    │
│ │ Code)           │  │ - Négation      │                    │
│ │                 │  │ - AND           │                    │
│ │ 3-4 semaines    │  │ - Backpressure  │                    │
│ └─────────────────┘  └─────────────────┘                    │
│                                                             │
│ □ AsyncFileSink (1 jour)                                    │
│ □ HttpSink retry (0.5 jour)                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    SEMAINES 4-5                              │
├─────────────────────────────────────────────────────────────┤
│ □ Intégration ZDD dans sase.rs                              │
│ □ Indexation événements                                     │
│ □ Métriques & observabilité                                 │
│ □ Consolidation pattern.rs → sase.rs (si décidé)            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    SEMAINES 6+                               │
├─────────────────────────────────────────────────────────────┤
│ □ Shared Execution                                          │
│ □ SIMD attention                                            │
│ □ HNSW index (si besoin)                                    │
│ □ Join optimization (si besoin)                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Annexe A : Commandes Claude Code

### A.1 Quick wins Attention

```markdown
# Objectif
Optimiser les performances critiques de l'attention engine dans Varpulis.

# Fichier cible
crates/varpulis-runtime/src/attention.rs

# Tâches

## 1. Remplacer le cache LRU
Le cache actuel utilise `Vec::retain()` qui est O(n).

Ajouter au Cargo.toml:
```toml
lru = "0.12"
```

Remplacer EmbeddingCache par une version utilisant `lru::LruCache`.

## 2. VecDeque pour history
Remplacer `Vec<(Event, Vec<f32>)>` par `VecDeque` pour O(1) sur push_back/pop_front.

## 3. Pré-calcul des projections Key/Value
Modifier la structure pour stocker les projections Key et Value calculées à l'ajout
plutôt que de les recalculer à chaque compute_attention().

# Tests
Tous les tests existants doivent passer.
Ajouter un benchmark pour mesurer l'amélioration.
```

### A.2 AsyncFileSink

```markdown
# Objectif
Remplacer FileSink par une version non-bloquante.

# Fichier cible
crates/varpulis-runtime/src/sink.rs

# Tâches

## 1. Créer AsyncFileSink
- Utiliser tokio::fs::File au lieu de std::fs::File
- Implémenter un buffer interne pour batching (64KB)
- Flush automatique quand le buffer est plein

## 2. Garder FileSink pour compatibilité
- Marquer #[deprecated(note = "Use AsyncFileSink")]
- Documenter la migration

# Tests
- Tester l'écriture asynchrone
- Tester le batching
- Tester le flush manuel et automatique
```

---

*Document généré pour le projet Varpulis — Cyril, Février 2026*