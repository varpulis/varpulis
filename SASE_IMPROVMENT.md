# Varpulis SASE+ : Analyse des problèmes et améliorations

> Document d'analyse technique des limitations de l'implémentation SASE+ actuelle et propositions d'améliorations.

---

## Table des matières

1. [Négation temporelle incomplète](#1-négation-temporelle-incomplète)
2. [Opérateur AND sémantiquement incorrect](#2-opérateur-and-sémantiquement-incorrect)
3. [Absence d'indexation des événements](#3-absence-dindexation-des-événements)
4. [Absence de Shared Execution](#4-absence-de-shared-execution)
5. [Absence de backpressure](#5-absence-de-backpressure)
6. [Métriques insuffisantes](#6-métriques-insuffisantes)
7. [Gestion event-time fragile](#7-gestion-event-time-fragile)
8. [Tableau récapitulatif](#8-tableau-récapitulatif)
9. [Plan d'implémentation global](#9-plan-dimplémentation-global)
10. [Attention Engine : Analyse et optimisations](#10-attention-engine--analyse-et-optimisations)

---

## 1. Négation temporelle incomplète

### 1.1 Le problème

La négation dans SASE+ signifie "cet événement ne doit PAS arriver pendant la fenêtre temporelle". C'est une contrainte **temporelle**, pas instantanée.

Pattern exemple :
```
PATTERN SEQ(Order a, NOT(Cancel), Shipment b)
WHERE a.order_id = b.order_id
WITHIN 1 hour
```

Sémantique attendue : une commande suivie d'une expédition, **sans annulation entre les deux**.

### 1.2 Code actuel

```rust
// Dans NfaCompiler::compile_pattern
SasePattern::Not(inner) => {
    let neg_state = self.nfa.add_state(State::new(0, StateType::Negation));
    self.nfa.add_epsilon(prev, neg_state);
    
    // Compile inner pattern from negation state
    let (_, _inner_end) = self.compile_pattern(inner, neg_state);
    // Si inner matches, the run is invalidated
    
    // Continue state (reached if negation doesn't match)
    let continue_state = self.nfa.add_state(State::new(0, StateType::Normal));
    self.nfa.add_epsilon(neg_state, continue_state);
    
    (neg_state, continue_state)
}
```

Et dans `advance_run_shared` :
```rust
if current_state.state_type == StateType::Negation
    && event_matches_state(nfa, &event, current_state, &run.captured)
{
    return RunAdvanceResult::Invalidate;
}
```

### 1.3 Ce qui ne va pas

1. **Invalidation immédiate** : Si Cancel arrive, le run est invalidé ✓ (correct)
2. **Confirmation d'absence** : Si Cancel n'arrive PAS, le run devrait pouvoir progresser vers Shipment... mais **quand** ?

Le code actuel permet de passer immédiatement via epsilon transition, sans attendre la fin de la fenêtre. Cela signifie que la négation n'est pas vraiment vérifiée — elle est juste "espérée".

### 1.4 Exemple de bug

```
t=0   Order(id=1)         → Run créé, état: attend NOT(Cancel)
t=1   Shipment(id=1)      → Run complété ! ✓ (mais on n'a pas vérifié Cancel)
t=2   Cancel(id=1)        → Trop tard, le match a déjà été émis
```

Le pattern a matché alors qu'il n'aurait pas dû.

### 1.5 Solution : Négation avec fenêtre d'attente

#### 1.5.1 Nouvelle structure pour les états de négation

```rust
#[derive(Debug, Clone)]
pub struct NegationConstraint {
    /// Pattern qui ne doit PAS matcher
    pub forbidden_type: String,
    /// Prédicat optionnel
    pub predicate: Option<Predicate>,
    /// Deadline après laquelle la négation est confirmée
    pub deadline: Option<Instant>,
    /// Deadline en event-time
    pub event_time_deadline: Option<DateTime<Utc>>,
    /// État cible une fois la négation confirmée
    pub next_state: usize,
}

pub struct Run {
    // ... champs existants ...
    
    /// Contraintes de négation actives (peuvent être multiples)
    pub pending_negations: Vec<NegationConstraint>,
}
```

#### 1.5.2 Logique de traitement

```rust
impl SaseEngine {
    fn process_negations(&mut self, event: &Event) {
        for run in &mut self.runs {
            // Vérifier si l'événement viole une négation active
            for neg in &run.pending_negations {
                if event.event_type == neg.forbidden_type {
                    if neg.predicate.as_ref().map_or(true, |p| eval_predicate(p, event, &run.captured)) {
                        run.invalidated = true;
                        break;
                    }
                }
            }
            
            // Nettoyer les négations expirées (confirmées)
            run.pending_negations.retain(|neg| {
                match self.time_semantics {
                    TimeSemantics::ProcessingTime => {
                        neg.deadline.map_or(true, |d| Instant::now() <= d)
                    }
                    TimeSemantics::EventTime => {
                        if let (Some(deadline), Some(watermark)) = (neg.event_time_deadline, self.watermark) {
                            watermark <= deadline
                        } else {
                            true
                        }
                    }
                }
            });
        }
    }
    
    fn check_negation_completions(&mut self) -> Vec<usize> {
        let mut completable_runs = Vec::new();
        
        for (idx, run) in self.runs.iter().enumerate() {
            // Si toutes les négations sont confirmées (expirées sans violation)
            if run.pending_negations.is_empty() && run.waiting_for_negation {
                completable_runs.push(idx);
            }
        }
        
        completable_runs
    }
}
```

#### 1.5.3 Compilation NFA modifiée

```rust
SasePattern::Not(inner) => {
    // On ne crée plus d'epsilon vers continue_state
    // Le passage se fait uniquement via confirmation temporelle
    
    let neg_state = self.nfa.add_state(State::new(0, StateType::Negation));
    self.nfa.add_transition(prev, neg_state);
    
    // Extraire le type d'événement interdit
    let forbidden = extract_event_type(inner);
    
    // Le continue_state sera atteint uniquement après confirmation
    let continue_state = self.nfa.add_state(State::new(0, StateType::Normal));
    
    // Stocker la relation pour le runtime
    neg_state.negation_target = Some(NegationTarget {
        forbidden,
        on_confirmed: continue_state,
    });
    
    (neg_state, continue_state)
}
```

#### 1.5.4 Traitement event-time avec watermarks

Le watermark est crucial pour la négation :

```rust
fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) {
    self.watermark = Some(new_watermark);
    
    // Confirmer les négations dont la deadline est passée
    for run in &mut self.runs {
        let mut confirmed_negations = Vec::new();
        
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if let Some(deadline) = neg.event_time_deadline {
                if new_watermark > deadline {
                    // Négation confirmée : l'événement interdit n'est pas arrivé à temps
                    confirmed_negations.push((idx, neg.next_state));
                }
            }
        }
        
        // Faire progresser le run pour chaque négation confirmée
        for (idx, next_state) in confirmed_negations.into_iter().rev() {
            run.pending_negations.remove(idx);
            run.current_state = next_state;
        }
    }
    
    self.cleanup_by_watermark();
}
```

### 1.6 Tests de validation

```rust
#[test]
fn test_negation_blocks_until_window_expires() {
    use chrono::{TimeZone, Utc};
    
    // SEQ(A, NOT(Cancel), B) WITHIN 5s
    let pattern = PatternBuilder::within(
        PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::not(PatternBuilder::event("Cancel")),
            PatternBuilder::event("B"),
        ]),
        Duration::from_secs(5),
    );
    
    let mut engine = SaseEngine::new(pattern).with_event_time();
    
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t6 = t0 + chrono::Duration::seconds(6);
    
    // A à t0
    engine.process(&Event::new("A").with_timestamp(t0));
    
    // B à t1 — ne devrait PAS compléter (négation non confirmée)
    let results = engine.process(&Event::new("B").with_timestamp(t1));
    assert!(results.is_empty(), "B should not complete while negation pending");
    
    // Avancer le watermark au-delà de la fenêtre
    engine.advance_watermark(t6);
    
    // Maintenant la négation est confirmée, le match devrait être émis
    // (ou déclenché par le prochain événement selon l'implémentation)
}

#[test]
fn test_negation_invalidates_on_forbidden_event() {
    // SEQ(A, NOT(Cancel), B)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::not(PatternBuilder::event("Cancel")),
        PatternBuilder::event("B"),
    ]);
    
    let mut engine = SaseEngine::new(pattern);
    engine.add_negation("Cancel".to_string(), None);
    
    engine.process(&make_event("A", vec![]));
    assert!(engine.stats().active_runs > 0);
    
    // Cancel arrive — devrait invalider
    engine.process(&make_event("Cancel", vec![]));
    assert_eq!(engine.stats().active_runs, 0, "Run should be invalidated");
    
    // B ne devrait rien matcher
    let results = engine.process(&make_event("B", vec![]));
    assert!(results.is_empty());
}
```

---

## 2. Opérateur AND sémantiquement incorrect

### 2.1 Le problème

`AND(A, B)` devrait matcher quand A et B arrivent **tous les deux**, dans **n'importe quel ordre**, dans la fenêtre temporelle.

Exemples valides :
- A puis B ✓
- B puis A ✓
- A, C, B ✓ (avec skip-till-any-match)

### 2.2 Code actuel

```rust
SasePattern::And(left, right) => {
    // For AND, we need to track both patterns independently
    // Create a fork state
    let fork = self.nfa.add_state(State::new(0, StateType::Normal));
    self.nfa.add_epsilon(prev, fork);

    let (_, left_end) = self.compile_pattern(left, fork);
    let (_, right_end) = self.compile_pattern(right, fork);

    // Join state - both must complete
    let join = self.nfa.add_state(State::new(0, StateType::Normal));
    self.nfa.add_epsilon(left_end, join);
    self.nfa.add_epsilon(right_end, join);

    (fork, join)
}
```

### 2.3 Ce qui ne va pas

Cette implémentation crée un NFA où :
1. On fork en deux branches parallèles
2. Chaque branche attend son événement
3. On join quand les deux sont terminées

Le problème : avec un NFA classique et des epsilon transitions, **un seul chemin est suivi à la fois**. On ne peut pas être "dans les deux branches simultanément".

Résultat : `AND(A, B)` se comporte comme `OR(SEQ(A,B), SEQ(B,A))` dans le meilleur cas, ou ne fonctionne pas du tout.

### 2.4 Exemple de bug

```
Pattern: AND(A, B)
Flux: A, B

Exécution attendue:
- A arrive → branche gauche avance, branche droite attend
- B arrive → branche droite avance, JOIN atteint → MATCH

Exécution réelle (NFA avec epsilon):
- A arrive → on suit epsilon vers fork, puis transition vers left_end
- B arrive → ??? on est déjà à left_end, pas de transition vers B
```

### 2.5 Solution : État composite pour AND

#### 2.5.1 Nouvelle représentation

```rust
/// État pour le pattern AND : track quels sous-patterns sont complétés
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AndState {
    /// Sous-patterns requis (identifiés par index)
    pub required: Vec<usize>,
    /// Sous-patterns déjà matchés
    pub completed: HashSet<usize>,
}

impl AndState {
    pub fn new(required_count: usize) -> Self {
        Self {
            required: (0..required_count).collect(),
            completed: HashSet::new(),
        }
    }
    
    pub fn complete(&mut self, pattern_idx: usize) {
        self.completed.insert(pattern_idx);
    }
    
    pub fn is_fully_completed(&self) -> bool {
        self.required.iter().all(|idx| self.completed.contains(idx))
    }
}
```

#### 2.5.2 Modification de Run

```rust
pub struct Run {
    // ... champs existants ...
    
    /// État AND actif (si dans un pattern AND)
    pub and_state: Option<AndState>,
    
    /// Événements capturés par branche AND
    pub and_captures: HashMap<usize, SharedEvent>,
}
```

#### 2.5.3 Compilation AND

```rust
SasePattern::And(left, right) => {
    // Créer un état spécial AND
    let and_state = self.nfa.add_state(State::new(0, StateType::And));
    self.nfa.add_transition(prev, and_state);
    
    // Compiler les sous-patterns pour extraction des event_types
    let left_type = extract_event_type(left);
    let right_type = extract_event_type(right);
    
    // Stocker les infos pour le runtime
    and_state.and_config = Some(AndConfig {
        branches: vec![
            AndBranch { event_type: left_type, predicate: extract_predicate(left) },
            AndBranch { event_type: right_type, predicate: extract_predicate(right) },
        ],
    });
    
    // État de sortie
    let join = self.nfa.add_state(State::new(0, StateType::Normal));
    and_state.and_join = Some(join);
    
    (and_state, join)
}
```

#### 2.5.4 Traitement runtime

```rust
fn advance_and_state(
    run: &mut Run,
    state: &State,
    event: &SharedEvent,
) -> RunAdvanceResult {
    let config = state.and_config.as_ref().unwrap();
    let and_state = run.and_state.get_or_insert_with(|| AndState::new(config.branches.len()));
    
    // Vérifier si l'événement matche une branche non-complétée
    for (idx, branch) in config.branches.iter().enumerate() {
        if and_state.completed.contains(&idx) {
            continue; // Déjà matché
        }
        
        if event.event_type == branch.event_type {
            let predicate_ok = branch.predicate.as_ref()
                .map_or(true, |p| eval_predicate(p, event, &run.captured));
            
            if predicate_ok {
                and_state.complete(idx);
                run.and_captures.insert(idx, Arc::clone(event));
                
                if and_state.is_fully_completed() {
                    // Tous les sous-patterns matchés !
                    run.current_state = state.and_join.unwrap();
                    return RunAdvanceResult::Continue;
                }
                
                return RunAdvanceResult::Continue;
            }
        }
    }
    
    // Aucune branche matchée
    RunAdvanceResult::NoMatch
}
```

### 2.6 AND avec sous-patterns complexes

Pour `AND(SEQ(A,B), SEQ(C,D))`, c'est plus complexe. Chaque branche devient un mini-NFA avec son propre état.

```rust
pub struct AndBranchState {
    /// NFA de la branche
    pub nfa: Nfa,
    /// Run actif pour cette branche
    pub run: Option<Run>,
    /// Complété ?
    pub completed: bool,
}

pub struct ComplexAndState {
    pub branches: Vec<AndBranchState>,
}
```

Le traitement devient :
```rust
fn advance_complex_and(
    complex_and: &mut ComplexAndState,
    event: &SharedEvent,
) -> bool {
    let mut any_progress = false;
    
    for branch in &mut complex_and.branches {
        if branch.completed {
            continue;
        }
        
        // Tenter d'avancer le run de cette branche
        if let Some(ref mut run) = branch.run {
            match advance_run_shared(&branch.nfa, strategy, run, Arc::clone(event)) {
                RunAdvanceResult::Complete(_) => {
                    branch.completed = true;
                    any_progress = true;
                }
                RunAdvanceResult::Continue => {
                    any_progress = true;
                }
                _ => {}
            }
        } else {
            // Tenter de démarrer un run
            if let Some(new_run) = try_start_run(&branch.nfa, event) {
                branch.run = Some(new_run);
                any_progress = true;
            }
        }
    }
    
    any_progress
}
```

### 2.7 Tests de validation

```rust
#[test]
fn test_and_matches_in_any_order() {
    let pattern = PatternBuilder::and(
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
    );
    
    let mut engine = SaseEngine::new(pattern);
    
    // A puis B
    engine.process(&make_event("A", vec![]));
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_and_matches_reverse_order() {
    let pattern = PatternBuilder::and(
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
    );
    
    let mut engine = SaseEngine::new(pattern);
    
    // B puis A
    engine.process(&make_event("B", vec![]));
    let results = engine.process(&make_event("A", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_and_with_noise_between() {
    let pattern = PatternBuilder::and(
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
    );
    
    let mut engine = SaseEngine::new(pattern)
        .with_strategy(SelectionStrategy::SkipTillAnyMatch);
    
    // A, Noise, Noise, B
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("Noise", vec![]));
    engine.process(&make_event("Noise", vec![]));
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_and_does_not_match_partial() {
    let pattern = PatternBuilder::and(
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
    );
    
    let mut engine = SaseEngine::new(pattern);
    
    // Seulement A
    engine.process(&make_event("A", vec![]));
    // Pas de match attendu
    assert_eq!(engine.stats().active_runs, 1);
}
```

---

## 3. Absence d'indexation des événements

### 3.1 Le problème

Chaque événement est comparé à chaque état de chaque run :

```rust
fn event_matches_state(
    _nfa: &Nfa,
    event: &Event,
    state: &State,
    captured: &HashMap<String, SharedEvent>,
) -> bool {
    if let Some(ref expected_type) = state.event_type {
        if event.event_type != *expected_type {  // Comparaison string à chaque fois
            return false;
        }
    }
    // ...
}
```

Avec :
- 1000 événements/seconde
- 100 types d'événements différents
- 500 runs actifs
- 10 états par pattern

→ 500 × 10 = 5000 comparaisons de strings par événement, soit 5 millions/seconde.

### 3.2 Solution : Index par type d'événement

#### 3.2.1 Structure d'index

```rust
use std::collections::{HashMap, HashSet};

/// Index des états par type d'événement attendu
pub struct EventTypeIndex {
    /// event_type → états qui attendent ce type
    state_index: HashMap<String, Vec<usize>>,
    
    /// États qui n'ont pas de type spécifique (wildcard)
    wildcard_states: Vec<usize>,
}

impl EventTypeIndex {
    pub fn new() -> Self {
        Self {
            state_index: HashMap::new(),
            wildcard_states: Vec::new(),
        }
    }
    
    pub fn index_state(&mut self, state_id: usize, event_type: Option<&str>) {
        match event_type {
            Some(et) => {
                self.state_index
                    .entry(et.to_string())
                    .or_default()
                    .push(state_id);
            }
            None => {
                self.wildcard_states.push(state_id);
            }
        }
    }
    
    /// Retourne les états potentiellement intéressés par cet événement
    pub fn get_candidate_states(&self, event_type: &str) -> impl Iterator<Item = usize> + '_ {
        let typed = self.state_index
            .get(event_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        
        typed.iter().copied().chain(self.wildcard_states.iter().copied())
    }
}
```

#### 3.2.2 Index des runs par état courant

```rust
/// Index des runs par leur état courant
pub struct RunIndex {
    /// state_id → indices des runs dans cet état
    by_state: HashMap<usize, Vec<usize>>,
}

impl RunIndex {
    pub fn new() -> Self {
        Self {
            by_state: HashMap::new(),
        }
    }
    
    pub fn add_run(&mut self, run_idx: usize, state_id: usize) {
        self.by_state.entry(state_id).or_default().push(run_idx);
    }
    
    pub fn remove_run(&mut self, run_idx: usize, state_id: usize) {
        if let Some(runs) = self.by_state.get_mut(&state_id) {
            runs.retain(|&idx| idx != run_idx);
        }
    }
    
    pub fn move_run(&mut self, run_idx: usize, old_state: usize, new_state: usize) {
        self.remove_run(run_idx, old_state);
        self.add_run(run_idx, new_state);
    }
    
    pub fn get_runs_in_state(&self, state_id: usize) -> &[usize] {
        self.by_state.get(&state_id).map(|v| v.as_slice()).unwrap_or(&[])
    }
}
```

#### 3.2.3 Traitement optimisé

```rust
impl SaseEngine {
    pub fn process_indexed(&mut self, event: &Event) -> Vec<MatchResult> {
        let shared_event = Arc::new(event.clone());
        let mut completed = Vec::new();
        
        // 1. Trouver les états intéressés par ce type d'événement
        let candidate_states: Vec<usize> = self.event_type_index
            .get_candidate_states(&event.event_type)
            .collect();
        
        // 2. Pour chaque état candidat, trouver les runs
        for state_id in candidate_states {
            let run_indices: Vec<usize> = self.run_index
                .get_runs_in_state(state_id)
                .to_vec();
            
            for run_idx in run_indices {
                // Traiter ce run spécifiquement
                let result = self.advance_run_indexed(run_idx, Arc::clone(&shared_event));
                
                match result {
                    RunAdvanceResult::Complete(match_result) => {
                        completed.push(match_result);
                        // Mettre à jour les index
                        self.run_index.remove_run(run_idx, state_id);
                    }
                    RunAdvanceResult::Continue => {
                        // Mettre à jour l'index si l'état a changé
                        let new_state = self.runs[run_idx].current_state;
                        if new_state != state_id {
                            self.run_index.move_run(run_idx, state_id, new_state);
                        }
                    }
                    // ... autres cas
                }
            }
        }
        
        // 3. Tenter de démarrer de nouveaux runs (état initial)
        // ...
        
        completed
    }
}
```

### 3.3 Optimisation supplémentaire : Bloom filter

Pour les systèmes avec des milliers de types d'événements :

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Bloom filter pour pré-filtrage rapide
pub struct EventTypeBloomFilter {
    bits: Vec<bool>,
    hash_count: usize,
}

impl EventTypeBloomFilter {
    pub fn new(expected_items: usize) -> Self {
        // Taille optimale : ~10 bits par item pour 1% false positive
        let bits_count = expected_items * 10;
        Self {
            bits: vec![false; bits_count],
            hash_count: 7, // Optimal pour 10 bits/item
        }
    }
    
    pub fn insert(&mut self, event_type: &str) {
        for i in 0..self.hash_count {
            let idx = self.hash(event_type, i);
            self.bits[idx] = true;
        }
    }
    
    /// Retourne false si CERTAINEMENT pas dans l'ensemble
    /// Retourne true si PEUT-ÊTRE dans l'ensemble
    pub fn might_contain(&self, event_type: &str) -> bool {
        for i in 0..self.hash_count {
            let idx = self.hash(event_type, i);
            if !self.bits[idx] {
                return false;
            }
        }
        true
    }
    
    fn hash(&self, event_type: &str, seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        event_type.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize) % self.bits.len()
    }
}
```

Usage :
```rust
impl SaseEngine {
    pub fn process_with_bloom(&mut self, event: &Event) -> Vec<MatchResult> {
        // Pré-filtrage ultra-rapide
        if !self.bloom_filter.might_contain(&event.event_type) {
            // Aucun pattern n'attend ce type d'événement
            return Vec::new();
        }
        
        // Continuer avec le traitement normal
        self.process_indexed(event)
    }
}
```

### 3.4 Benchmark attendu

| Scénario | Sans index | Avec index | Amélioration |
|----------|-----------|------------|--------------|
| 100 types, 500 runs | 5000 cmp/evt | ~50 cmp/evt | 100x |
| 1000 types, 1000 runs | 50000 cmp/evt | ~100 cmp/evt | 500x |
| Avec Bloom filter | - | Souvent 0 cmp | ∞ (skip total) |

---

## 4. Absence de Shared Execution

### 4.1 Le problème

Si tu as 100 patterns qui commencent tous par `Transaction where amount > 1000`, chacun maintient ses propres runs indépendamment :

```
Pattern 1: SEQ(Transaction[amount>1000], Transfer, ...)
Pattern 2: SEQ(Transaction[amount>1000], Withdrawal, ...)
Pattern 3: SEQ(Transaction[amount>1000], Deposit, ...)
...
Pattern 100: SEQ(Transaction[amount>1000], ...)
```

Chaque Transaction crée 100 runs distincts, même s'ils partagent le même préfixe.

### 4.2 Solution : Prefix Tree (Trie) de patterns

#### 4.2.1 Structure de partage

```rust
/// Nœud dans le trie de patterns
#[derive(Debug)]
pub struct PatternTrieNode {
    /// Condition sur ce nœud (type + prédicat)
    pub condition: Option<EventCondition>,
    
    /// Enfants (suite des patterns)
    pub children: Vec<PatternTrieNode>,
    
    /// Patterns qui se terminent ici
    pub terminal_patterns: Vec<PatternId>,
    
    /// ID unique pour ce nœud
    pub node_id: usize,
}

#[derive(Debug, Clone)]
pub struct EventCondition {
    pub event_type: String,
    pub predicate: Option<Predicate>,
    pub alias: Option<String>,
}

/// Identifiant de pattern
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PatternId(pub usize);
```

#### 4.2.2 Construction du trie

```rust
impl PatternTrie {
    pub fn new() -> Self {
        Self {
            root: PatternTrieNode {
                condition: None,
                children: Vec::new(),
                terminal_patterns: Vec::new(),
                node_id: 0,
            },
            next_node_id: 1,
        }
    }
    
    pub fn add_pattern(&mut self, pattern_id: PatternId, conditions: Vec<EventCondition>) {
        let mut current = &mut self.root;
        
        for condition in conditions {
            // Chercher un enfant existant avec la même condition
            let child_idx = current.children.iter().position(|child| {
                child.condition.as_ref() == Some(&condition)
            });
            
            match child_idx {
                Some(idx) => {
                    current = &mut current.children[idx];
                }
                None => {
                    // Créer un nouveau nœud
                    let new_node = PatternTrieNode {
                        condition: Some(condition),
                        children: Vec::new(),
                        terminal_patterns: Vec::new(),
                        node_id: self.next_node_id,
                    };
                    self.next_node_id += 1;
                    current.children.push(new_node);
                    current = current.children.last_mut().unwrap();
                }
            }
        }
        
        // Marquer ce nœud comme terminal pour ce pattern
        current.terminal_patterns.push(pattern_id);
    }
}
```

#### 4.2.3 Shared Run

```rust
/// Run partagé entre plusieurs patterns
pub struct SharedRun {
    /// Position dans le trie
    pub trie_node_id: usize,
    
    /// Événements capturés
    pub captured: HashMap<String, SharedEvent>,
    
    /// Patterns encore actifs (certains peuvent avoir divergé)
    pub active_patterns: HashSet<PatternId>,
    
    /// Timestamp de début
    pub started_at: Instant,
}

impl SharedRun {
    /// Quand on atteint un nœud avec plusieurs enfants, on peut devoir "forker"
    pub fn fork_for_patterns(&self, pattern_ids: &[PatternId]) -> Self {
        SharedRun {
            trie_node_id: self.trie_node_id,
            captured: self.captured.clone(),
            active_patterns: pattern_ids.iter().cloned().collect(),
            started_at: self.started_at,
        }
    }
}
```

#### 4.2.4 Traitement partagé

```rust
impl SharedExecutionEngine {
    pub fn process(&mut self, event: &Event) -> Vec<(PatternId, MatchResult)> {
        let shared_event = Arc::new(event.clone());
        let mut results = Vec::new();
        
        // Avancer les runs partagés
        let mut i = 0;
        while i < self.shared_runs.len() {
            let run = &mut self.shared_runs[i];
            let trie_node = self.trie.get_node(run.trie_node_id);
            
            // Trouver les enfants qui matchent cet événement
            let matching_children: Vec<(usize, &[PatternId])> = trie_node.children
                .iter()
                .filter(|child| {
                    child.condition.as_ref().map_or(false, |c| {
                        event.event_type == c.event_type &&
                        c.predicate.as_ref().map_or(true, |p| eval_predicate(p, event, &run.captured))
                    })
                })
                .map(|child| (child.node_id, child.terminal_patterns.as_slice()))
                .collect();
            
            match matching_children.len() {
                0 => {
                    // Aucun match, run continue d'attendre ou expire
                    i += 1;
                }
                1 => {
                    // Un seul enfant match, continuer avec ce run
                    let (child_id, terminals) = matching_children[0];
                    run.trie_node_id = child_id;
                    
                    // Capturer l'événement
                    if let Some(ref cond) = self.trie.get_node(child_id).condition {
                        if let Some(ref alias) = cond.alias {
                            run.captured.insert(alias.clone(), Arc::clone(&shared_event));
                        }
                    }
                    
                    // Vérifier les patterns terminaux
                    for &pattern_id in terminals {
                        if run.active_patterns.contains(&pattern_id) {
                            results.push((pattern_id, MatchResult {
                                captured: run.captured.clone(),
                                // ...
                            }));
                        }
                    }
                    
                    i += 1;
                }
                _ => {
                    // Plusieurs enfants matchent — il faut forker
                    let original_run = self.shared_runs.remove(i);
                    
                    for (child_id, _) in matching_children {
                        let mut forked = original_run.clone();
                        forked.trie_node_id = child_id;
                        // Capturer...
                        self.shared_runs.push(forked);
                    }
                    // Ne pas incrémenter i, les nouveaux runs sont à la fin
                }
            }
        }
        
        // Démarrer de nouveaux runs partagés
        self.try_start_shared_run(&shared_event);
        
        results
    }
}
```

### 4.3 Gain attendu

| Scénario | Sans partage | Avec partage | Gain mémoire |
|----------|-------------|--------------|--------------|
| 100 patterns, préfixe commun de 3 états | 100 runs | 1 run (puis fork) | ~97% |
| 1000 patterns, 50% de préfixes communs | 1000 runs | ~500 runs | ~50% |

---

## 5. Absence de backpressure

### 5.1 Le problème

```rust
if self.runs.len() < self.max_runs {
    self.runs.push(run);
}
// Sinon : run silencieusement perdu
```

Conséquences :
- Perte de données invisible
- Pas de feedback au producteur
- Pas de métriques pour alerter

### 5.2 Solution : Stratégies de backpressure

#### 5.2.1 Enum des stratégies

```rust
/// Stratégie quand la limite de runs est atteinte
#[derive(Debug, Clone)]
pub enum BackpressureStrategy {
    /// Ignorer silencieusement (comportement actuel, déconseillé)
    Drop,
    
    /// Retourner une erreur
    Error,
    
    /// Bloquer jusqu'à ce qu'un slot se libère (avec timeout)
    Block { timeout: Duration },
    
    /// Évincer les runs les plus anciens
    EvictOldest,
    
    /// Évincer les runs avec le moins de progression
    EvictLeastProgress,
    
    /// Échantillonnage : accepter 1 run sur N
    Sample { rate: f64 },
    
    /// Buffer externe avec spill-to-disk
    Spill { path: PathBuf, max_memory: usize },
}
```

#### 5.2.2 Implémentation

```rust
impl SaseEngine {
    pub fn with_backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = strategy;
        self
    }
    
    fn handle_backpressure(&mut self, new_run: Run) -> Result<(), BackpressureError> {
        if self.runs.len() < self.max_runs {
            self.runs.push(new_run);
            return Ok(());
        }
        
        match &self.backpressure {
            BackpressureStrategy::Drop => {
                self.stats.dropped_runs += 1;
                Ok(())
            }
            
            BackpressureStrategy::Error => {
                Err(BackpressureError::MaxRunsExceeded {
                    current: self.runs.len(),
                    max: self.max_runs,
                })
            }
            
            BackpressureStrategy::EvictOldest => {
                // Trouver le run le plus ancien
                let oldest_idx = self.runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.started_at)
                    .map(|(idx, _)| idx);
                
                if let Some(idx) = oldest_idx {
                    self.stats.evicted_runs += 1;
                    self.runs.remove(idx);
                    self.runs.push(new_run);
                }
                Ok(())
            }
            
            BackpressureStrategy::EvictLeastProgress => {
                // Évincer le run avec le moins de stack entries
                let least_progress_idx = self.runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.stack.len())
                    .map(|(idx, _)| idx);
                
                if let Some(idx) = least_progress_idx {
                    self.stats.evicted_runs += 1;
                    self.runs.remove(idx);
                    self.runs.push(new_run);
                }
                Ok(())
            }
            
            BackpressureStrategy::Sample { rate } => {
                if rand::random::<f64>() < *rate {
                    // Évincer un run aléatoire pour faire de la place
                    if !self.runs.is_empty() {
                        let idx = rand::random::<usize>() % self.runs.len();
                        self.runs.remove(idx);
                        self.runs.push(new_run);
                    }
                } else {
                    self.stats.sampled_out_runs += 1;
                }
                Ok(())
            }
            
            BackpressureStrategy::Block { timeout } => {
                // Dans un contexte async, on attendrait
                // Ici on retourne une erreur spéciale
                Err(BackpressureError::WouldBlock {
                    timeout: *timeout,
                })
            }
            
            BackpressureStrategy::Spill { path, max_memory } => {
                self.spill_to_disk(path, *max_memory)?;
                self.runs.push(new_run);
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub enum BackpressureError {
    MaxRunsExceeded { current: usize, max: usize },
    WouldBlock { timeout: Duration },
    SpillFailed(std::io::Error),
}
```

#### 5.2.3 API de résultat enrichie

```rust
/// Résultat du traitement d'un événement
#[derive(Debug)]
pub struct ProcessResult {
    /// Matches complétés
    pub matches: Vec<MatchResult>,
    
    /// Avertissements (backpressure, etc.)
    pub warnings: Vec<ProcessWarning>,
    
    /// Statistiques de ce traitement
    pub stats: ProcessStats,
}

#[derive(Debug)]
pub enum ProcessWarning {
    RunDropped { reason: String },
    RunEvicted { age: Duration },
    BackpressureActive { utilization: f64 },
    ApproachingLimit { current: usize, max: usize },
}

impl SaseEngine {
    pub fn process_with_result(&mut self, event: &Event) -> ProcessResult {
        let mut warnings = Vec::new();
        
        // Avertissement préventif
        let utilization = self.runs.len() as f64 / self.max_runs as f64;
        if utilization > 0.8 {
            warnings.push(ProcessWarning::ApproachingLimit {
                current: self.runs.len(),
                max: self.max_runs,
            });
        }
        
        let matches = self.process(event);
        
        ProcessResult {
            matches,
            warnings,
            stats: ProcessStats {
                active_runs: self.runs.len(),
                // ...
            },
        }
    }
}
```

---

## 6. Métriques insuffisantes

### 6.1 Le problème actuel

```rust
#[derive(Debug, Clone)]
pub struct SaseStats {
    pub active_runs: usize,
    pub partitions: usize,
    pub nfa_states: usize,
}
```

C'est insuffisant pour :
- Diagnostiquer les problèmes de performance
- Monitorer en production
- Optimiser les patterns

### 6.2 Solution : Métriques complètes

```rust
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};

/// Métriques complètes du moteur SASE+
#[derive(Debug, Default)]
pub struct SaseMetrics {
    // === Compteurs d'événements ===
    
    /// Événements traités (total)
    pub events_processed: AtomicU64,
    
    /// Événements qui ont déclenché au moins une transition
    pub events_matched: AtomicU64,
    
    /// Événements ignorés (aucun run intéressé)
    pub events_ignored: AtomicU64,
    
    // === Compteurs de runs ===
    
    /// Runs créés (total)
    pub runs_created: AtomicU64,
    
    /// Runs complétés (matches émis)
    pub runs_completed: AtomicU64,
    
    /// Runs expirés (timeout)
    pub runs_expired: AtomicU64,
    
    /// Runs invalidés (négation)
    pub runs_invalidated: AtomicU64,
    
    /// Runs droppés (backpressure)
    pub runs_dropped: AtomicU64,
    
    /// Runs évincés (backpressure avec éviction)
    pub runs_evicted: AtomicU64,
    
    // === Compteurs de matches ===
    
    /// Matches émis (total)
    pub matches_emitted: AtomicU64,
    
    // === Métriques de latence (nécessite histogramme) ===
    
    /// Histogramme de latence de traitement par événement
    pub event_latency: LatencyHistogram,
    
    /// Histogramme de durée des matches (du premier au dernier événement)
    pub match_duration: LatencyHistogram,
    
    // === Métriques de mémoire ===
    
    /// Pic de runs actifs
    pub peak_active_runs: AtomicU64,
    
    /// Estimation mémoire utilisée (bytes)
    pub estimated_memory_bytes: AtomicU64,
    
    // === Métriques ZDD (si implémenté) ===
    
    /// Nœuds ZDD créés
    pub zdd_nodes_created: AtomicU64,
    
    /// Nœuds ZDD réutilisés (cache hit)
    pub zdd_cache_hits: AtomicU64,
    
    // === Timestamps ===
    
    /// Dernier événement traité
    pub last_event_time: AtomicU64, // epoch millis
    
    /// Watermark actuel (event-time)
    pub current_watermark: AtomicU64, // epoch millis
}

/// Histogramme de latence simple (buckets exponentiels)
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    /// Buckets : <1µs, <10µs, <100µs, <1ms, <10ms, <100ms, <1s, >1s
    buckets: [AtomicU64; 8],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl LatencyHistogram {
    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        
        let bucket_idx = match micros {
            0..=1 => 0,
            2..=10 => 1,
            11..=100 => 2,
            101..=1_000 => 3,
            1_001..=10_000 => 4,
            10_001..=100_000 => 5,
            100_001..=1_000_000 => 6,
            _ => 7,
        };
        
        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn percentile(&self, p: f64) -> Duration {
        let total = self.count.load(Ordering::Relaxed);
        let target = (total as f64 * p) as u64;
        
        let mut cumulative = 0u64;
        let bucket_maxes = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, u64::MAX];
        
        for (idx, &max_micros) in bucket_maxes.iter().enumerate() {
            cumulative += self.buckets[idx].load(Ordering::Relaxed);
            if cumulative >= target {
                return Duration::from_micros(max_micros.min(1_000_000));
            }
        }
        
        Duration::from_secs(1)
    }
    
    pub fn mean(&self) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        Duration::from_micros(self.sum_micros.load(Ordering::Relaxed) / count)
    }
}
```

### 6.3 Instrumentation du moteur

```rust
impl SaseEngine {
    pub fn process_instrumented(&mut self, event: &Event) -> Vec<MatchResult> {
        let start = Instant::now();
        
        self.metrics.events_processed.fetch_add(1, Ordering::Relaxed);
        
        let results = self.process(event);
        
        // Métriques post-traitement
        if results.is_empty() {
            self.metrics.events_ignored.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.events_matched.fetch_add(1, Ordering::Relaxed);
            self.metrics.matches_emitted.fetch_add(results.len() as u64, Ordering::Relaxed);
        }
        
        // Latence
        self.metrics.event_latency.record(start.elapsed());
        
        // Peak runs
        let current_runs = self.runs.len() as u64;
        self.metrics.peak_active_runs.fetch_max(current_runs, Ordering::Relaxed);
        
        results
    }
}
```

### 6.4 Export Prometheus

```rust
impl SaseMetrics {
    /// Export au format Prometheus
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        
        // Compteurs
        output.push_str(&format!(
            "# HELP varpulis_events_total Total events processed\n\
             # TYPE varpulis_events_total counter\n\
             varpulis_events_total {}\n\n",
            self.events_processed.load(Ordering::Relaxed)
        ));
        
        output.push_str(&format!(
            "# HELP varpulis_matches_total Total matches emitted\n\
             # TYPE varpulis_matches_total counter\n\
             varpulis_matches_total {}\n\n",
            self.matches_emitted.load(Ordering::Relaxed)
        ));
        
        // Gauges
        output.push_str(&format!(
            "# HELP varpulis_active_runs Current active runs\n\
             # TYPE varpulis_active_runs gauge\n\
             varpulis_active_runs {}\n\n",
            self.peak_active_runs.load(Ordering::Relaxed)
        ));
        
        // Histogrammes
        output.push_str(&format!(
            "# HELP varpulis_event_latency_p50 Event processing latency p50\n\
             # TYPE varpulis_event_latency_p50 gauge\n\
             varpulis_event_latency_p50 {}\n\n",
            self.event_latency.percentile(0.5).as_micros()
        ));
        
        output.push_str(&format!(
            "# HELP varpulis_event_latency_p99 Event processing latency p99\n\
             # TYPE varpulis_event_latency_p99 gauge\n\
             varpulis_event_latency_p99 {}\n\n",
            self.event_latency.percentile(0.99).as_micros()
        ));
        
        output
    }
}
```

---

## 7. Gestion event-time fragile

### 7.1 Le problème

```rust
pub fn with_event_time_deadline(mut self, timeout: Duration) -> Self {
    if let Some(started_at) = self.event_time_started_at {
        self.event_time_deadline =
            Some(started_at + chrono::Duration::from_std(timeout).unwrap_or_default());
            //                                          ^^^^^^^^^^^^^^^^^^^^^^^^
            //                                          Danger ! Masque les erreurs
    }
    self
}
```

`chrono::Duration::from_std` peut échouer si la durée est trop grande. `unwrap_or_default()` retourne `Duration::zero()`, ce qui crée une deadline dans le passé → run immédiatement expiré.

### 7.2 Autres problèmes event-time

#### 7.2.1 Pas de gestion des événements tardifs

```rust
fn update_watermark(&mut self, event: &Event) {
    let event_ts = event.timestamp;
    
    // On met à jour max_timestamp...
    match self.max_timestamp {
        Some(max_ts) if event_ts > max_ts => {
            self.max_timestamp = Some(event_ts);
        }
        // ...
    }
    // Mais que faire si event_ts << watermark actuel ?
    // L'événement est "en retard" et potentiellement ignoré
}
```

#### 7.2.2 Pas de side-output pour les événements tardifs

Les systèmes comme Flink permettent de récupérer les événements arrivés après le watermark dans un "late data side output".

### 7.3 Solution complète

```rust
/// Configuration du traitement event-time
#[derive(Debug, Clone)]
pub struct EventTimeConfig {
    /// Tolérance pour les événements en désordre
    pub max_out_of_orderness: Duration,
    
    /// Tolérance supplémentaire pour les événements tardifs
    pub allowed_lateness: Duration,
    
    /// Callback pour les événements tardifs
    pub late_event_handler: Option<LateEventHandler>,
    
    /// Intervalle d'émission du watermark
    pub watermark_interval: Duration,
}

pub type LateEventHandler = Box<dyn Fn(&Event) + Send + Sync>;

/// Gestion robuste des timestamps
#[derive(Debug, Clone)]
pub struct EventTimeManager {
    config: EventTimeConfig,
    
    /// Watermark actuel
    watermark: Option<DateTime<Utc>>,
    
    /// Max timestamp observé
    max_timestamp: Option<DateTime<Utc>>,
    
    /// Buffer pour événements en attente de watermark
    pending_events: Vec<(DateTime<Utc>, SharedEvent)>,
    
    /// Compteur d'événements tardifs
    late_events_count: u64,
}

impl EventTimeManager {
    pub fn new(config: EventTimeConfig) -> Self {
        Self {
            config,
            watermark: None,
            max_timestamp: None,
            pending_events: Vec::new(),
            late_events_count: 0,
        }
    }
    
    /// Traite un événement et met à jour le watermark
    pub fn process_event(&mut self, event: &Event) -> EventTimeResult {
        let event_ts = event.timestamp;
        
        // Vérifier si l'événement est tardif
        if let Some(wm) = self.watermark {
            if event_ts < wm {
                // Événement en retard
                let lateness = wm - event_ts;
                let allowed = chrono::Duration::from_std(self.config.allowed_lateness)
                    .unwrap_or(chrono::Duration::zero());
                
                if lateness > allowed {
                    // Trop tard, rejeter
                    self.late_events_count += 1;
                    
                    if let Some(ref handler) = self.config.late_event_handler {
                        handler(event);
                    }
                    
                    return EventTimeResult::TooLate { 
                        lateness: lateness.to_std().unwrap_or(Duration::MAX),
                    };
                } else {
                    // Tardif mais acceptable
                    return EventTimeResult::Late {
                        lateness: lateness.to_std().unwrap_or(Duration::ZERO),
                    };
                }
            }
        }
        
        // Mettre à jour max_timestamp
        match self.max_timestamp {
            Some(max_ts) if event_ts > max_ts => {
                self.max_timestamp = Some(event_ts);
            }
            None => {
                self.max_timestamp = Some(event_ts);
            }
            _ => {}
        }
        
        // Mettre à jour le watermark
        self.update_watermark();
        
        EventTimeResult::OnTime
    }
    
    fn update_watermark(&mut self) {
        if let Some(max_ts) = self.max_timestamp {
            let out_of_orderness = chrono::Duration::from_std(self.config.max_out_of_orderness)
                .unwrap_or(chrono::Duration::zero());
            
            let new_watermark = max_ts - out_of_orderness;
            
            // Le watermark ne recule jamais
            match self.watermark {
                Some(wm) if new_watermark > wm => {
                    self.watermark = Some(new_watermark);
                }
                None => {
                    self.watermark = Some(new_watermark);
                }
                _ => {}
            }
        }
    }
    
    /// Calcule une deadline event-time de façon robuste
    pub fn compute_deadline(&self, start: DateTime<Utc>, timeout: Duration) -> Option<DateTime<Utc>> {
        chrono::Duration::from_std(timeout)
            .ok()
            .map(|d| start + d)
    }
}

#[derive(Debug)]
pub enum EventTimeResult {
    /// Événement à l'heure
    OnTime,
    /// Événement en retard mais acceptable
    Late { lateness: Duration },
    /// Événement trop tard, rejeté
    TooLate { lateness: Duration },
}
```

### 7.4 Intégration dans SaseEngine

```rust
impl SaseEngine {
    pub fn with_event_time_config(mut self, config: EventTimeConfig) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self.event_time_manager = Some(EventTimeManager::new(config));
        self
    }
    
    pub fn process(&mut self, event: &Event) -> Vec<MatchResult> {
        // Gestion event-time robuste
        if let Some(ref mut etm) = self.event_time_manager {
            match etm.process_event(event) {
                EventTimeResult::TooLate { lateness } => {
                    self.metrics.late_events_dropped.fetch_add(1, Ordering::Relaxed);
                    // Optionnel : émettre vers un side-output
                    return Vec::new();
                }
                EventTimeResult::Late { .. } => {
                    self.metrics.late_events_accepted.fetch_add(1, Ordering::Relaxed);
                    // Continuer le traitement
                }
                EventTimeResult::OnTime => {}
            }
            
            self.watermark = etm.watermark;
        }
        
        // Reste du traitement...
        self.process_internal(event)
    }
}
```

---

## 8. Tableau récapitulatif

| # | Problème | Sévérité | Impact | Effort | Priorité |
|---|----------|----------|--------|--------|----------|
| 1 | Négation incomplète | **Haute** | Correctness | Moyen | P1 |
| 2 | AND incorrect | **Haute** | Correctness | Moyen | P1 |
| 3 | Pas d'indexation | Moyenne | Performance | Faible | P2 |
| 4 | Pas de Shared Execution | Moyenne | Mémoire/Perf | Élevé | P3 |
| 5 | Pas de backpressure | **Haute** | Robustesse | Moyen | P1 |
| 6 | Métriques insuffisantes | Moyenne | Observabilité | Faible | P2 |
| 7 | Event-time fragile | Moyenne | Correctness | Faible | P2 |
| 8 | Attention: projections répétées | **Haute** | Performance | Faible | **P1** |
| 9 | Attention: cache LRU O(n) | **Haute** | Performance | Trivial | **P1** |
| 10 | Attention: history O(n) | Moyenne | Performance | Trivial | **P1** |
| 11 | Attention: pas de SIMD | Moyenne | Performance | Moyen | P2 |
| 12 | Attention: pas d'ANN (HNSW) | Basse | Scalabilité | Élevé | P3 |

### Dépendances

```
ZDD (déjà planifié)
    │
    ├── Peut être fait en parallèle avec :
    │   ├── Négation (P1)
    │   ├── AND (P1)
    │   ├── Backpressure (P1)
    │   └── Attention P1 (triviales, à faire en premier)
    │
    └── Devrait précéder :
        └── Shared Execution (bénéficie du ZDD)

Attention P1 (quick wins)
    │
    ├── LRU cache → crate `lru` (5 min)
    ├── VecDeque pour history (5 min)
    └── Pré-calcul Key/Value (1-2h)
        │
        └── Débloque SIMD et HNSW (P2/P3)
```

---

## 9. Plan d'implémentation global

### Phase 0 : Quick wins Attention (1 jour)

**À faire immédiatement** — gains massifs pour effort trivial :

1. **Remplacer le cache LRU** (5 minutes)
   ```toml
   [dependencies]
   lru = "0.12"
   ```

2. **VecDeque pour history** (5 minutes)
   - Remplacer `Vec` par `VecDeque`
   - `push_back` + `pop_front` au lieu de `remove(0)`

3. **Pré-calcul des projections Key/Value** (1-2 heures)
   - Modifier `HistoryEntry` pour stocker les projections
   - Calculer une seule fois à l'ajout

**Impact estimé** : throughput ×500 sur l'attention engine.

### Phase 1 : Correctness (2-3 semaines)

**En parallèle avec ZDD :**

1. **Négation temporelle** (1 semaine)
   - Ajouter `pending_negations` à Run
   - Modifier la compilation NFA
   - Implémenter la confirmation par watermark
   - Tests exhaustifs

2. **Opérateur AND** (1 semaine)
   - Créer `AndState` et `AndBranchState`
   - Modifier le traitement runtime
   - Gérer AND avec sous-patterns complexes
   - Tests : ordre varié, bruit, partiels

3. **Backpressure** (0.5 semaine)
   - Implémenter `BackpressureStrategy`
   - Enrichir l'API de retour (`ProcessResult`)
   - Tests de charge

### Phase 2 : Performance (1-2 semaines)

4. **Indexation** (0.5 semaine)
   - `EventTypeIndex`
   - `RunIndex`
   - Optionnel : Bloom filter
   - Benchmarks

5. **Event-time robuste** (0.5 semaine)
   - `EventTimeManager`
   - Gestion des événements tardifs
   - Tests avec désordre

6. **Métriques** (0.5 semaine)
   - `SaseMetrics` complet
   - `LatencyHistogram`
   - Export Prometheus
   - Dashboard Grafana (optionnel)

7. **Attention SIMD** (0.5 semaine)
   - Vectorisation du dot product avec AVX2
   - Pool d'allocations pour projections
   - Benchmarks comparatifs

### Phase 3 : Scalabilité (2-3 semaines)

8. **Shared Execution** (2-3 semaines)
   - `PatternTrie`
   - `SharedRun`
   - Intégration avec ZDD
   - Benchmarks multi-patterns

9. **Attention HNSW** (1 semaine, optionnel)
   - Intégration `instant-distance` ou `hnswlib`
   - Seuil adaptatif (exhaustif < 500, HNSW > 500)
   - Rebuild périodique de l'index

---

## Annexe A : Prompt Claude Code pour les correctifs P1

```markdown
# Objectif
Corriger les problèmes de correctness dans le moteur SASE+ de Varpulis.

# Contexte
Voir le fichier `varpulis-sase-improvements.md` pour l'analyse détaillée.

# Tâches prioritaires

## 1. Négation temporelle
Le fichier `crates/varpulis-runtime/src/sase.rs` a une implémentation incomplète 
de la négation. Actuellement, elle invalide immédiatement si l'événement interdit 
arrive, mais ne confirme jamais positivement si l'événement n'arrive PAS dans le délai.

Modifications requises :
- Ajouter `pending_negations: Vec<NegationConstraint>` à `Run`
- Modifier `compile_pattern` pour `SasePattern::Not`
- Ajouter `process_negations()` et `check_negation_completions()`
- Utiliser le watermark pour confirmer les négations

## 2. Opérateur AND
L'implémentation actuelle avec epsilon transitions ne gère pas correctement 
AND(A, B) où A et B peuvent arriver dans n'importe quel ordre.

Modifications requises :
- Créer `AndState` pour tracker les branches complétées
- Ajouter `and_state: Option<AndState>` à `Run`
- Modifier `compile_pattern` pour `SasePattern::And`
- Implémenter `advance_and_state()`

## 3. Backpressure
Actuellement les runs excédentaires sont silencieusement ignorés.

Modifications requises :
- Créer `enum BackpressureStrategy`
- Créer `enum BackpressureError`
- Ajouter `handle_backpressure()` à `SaseEngine`
- Créer `ProcessResult` avec warnings

# Contraintes
- Maintenir la compatibilité avec les tests existants
- Ajouter des tests pour chaque correction
- Documenter les changements d'API
```

---

## 10. Attention Engine : Analyse et optimisations

### 10.1 Rôle du module

L'attention engine (`attention.rs`) implémente un mécanisme d'attention **déterministe** pour la corrélation d'événements. Contrairement aux LLMs :
- Pas de génération, uniquement du scoring de similarité
- Résultats reproductibles (mêmes inputs = mêmes outputs)
- Embeddings rule-based ou pré-entraînés

L'idée : pour chaque nouvel événement, calculer des scores d'attention par rapport aux événements récents pour détecter des corrélations.

### 10.2 Problèmes identifiés

#### 10.2.1 Complexité O(n × h × d²) par événement

```rust
pub fn compute_attention(&mut self, current: &Event) -> AttentionResult {
    for (hist_event, hist_embedding) in &self.history {  // O(n) - max 1000
        for head in 0..self.config.num_heads {           // O(h) - typiquement 4
            let q = self.embedding_engine.project(...);  // O(d²) - 64×16=1024 ops
            let k = self.embedding_engine.project(...);  // O(d²)
            total += self.dot_product(&q, &k);           // O(d)
        }
    }
}
```

**Calcul avec les paramètres par défaut :**
- `max_history = 1000`
- `num_heads = 4`
- `embedding_dim = 64`, `head_dim = 16`

Par événement :
- 1000 × 4 × 2 = 8000 projections
- Chaque projection = 64 × 16 = 1024 multiplications
- **Total : ~8 millions d'opérations par événement**

À 1000 événements/seconde → **8 milliards d'opérations/seconde**

#### 10.2.2 Cache LRU en O(n)

```rust
impl EmbeddingCache {
    pub fn get(&mut self, event_hash: u64) -> Option<Vec<f32>> {
        // ...
        self.access_order.retain(|&h| h != event_hash);  // O(n) scan !
        self.access_order.push(event_hash);
        // ...
    }
}
```

Avec `max_size = 10000`, chaque accès cache fait un scan de 10000 éléments. Le cache censé accélérer devient un goulot d'étranglement.

#### 10.2.3 History avec suppression O(n)

```rust
while self.history.len() > self.config.max_history { 
    self.history.remove(0);  // O(n) - décale tout le Vec
}
```

`Vec::remove(0)` déplace tous les éléments. Avec 1000 éléments, chaque ajout coûte potentiellement 1000 copies.

#### 10.2.4 Absence de vectorisation SIMD

```rust
fn dot_product(&self, a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}
```

Code correct mais naïf. Avec AVX2, on traite 8 floats en parallèle. Avec AVX-512, 16 floats.

#### 10.2.5 Projections Key recalculées

Les projections Key des événements historiques sont **recalculées à chaque appel** alors qu'elles sont déterministes et pourraient être pré-calculées lors de l'ajout à l'historique.

#### 10.2.6 Allocations dans les boucles chaudes

```rust
pub fn project(&self, embedding: &[f32], ...) -> Vec<f32> {
    let mut result = vec![0.0f32; head_dim];  // Allocation !
    // ...
}
```

8000 projections = 8000 allocations par événement. L'allocateur devient saturé.

#### 10.2.7 Recherche exhaustive au lieu d'ANN

Pour trouver les événements corrélés, on compare avec TOUS les événements historiques. Un index **Approximate Nearest Neighbor** (HNSW, IVF) réduirait ça à O(log n).

### 10.3 Solutions proposées

#### 10.3.1 Pré-calcul des projections Key/Value

```rust
pub struct AttentionEngine {
    // Stocker les projections pré-calculées
    history: Vec<HistoryEntry>,
}

struct HistoryEntry {
    event: Event,
    embedding: Vec<f32>,
    // Pré-calculées une seule fois à l'ajout
    key_projections: Vec<Vec<f32>>,    // [num_heads][head_dim]
    value_projections: Vec<Vec<f32>>,  // [num_heads][head_dim]
}

impl AttentionEngine {
    pub fn add_event(&mut self, event: Event) {
        let embedding = self.compute_embedding(&event);
        
        // Pré-calculer toutes les projections
        let key_projections: Vec<Vec<f32>> = (0..self.config.num_heads)
            .map(|h| self.embedding_engine.project(&embedding, h, ProjectionType::Key))
            .collect();
        
        let value_projections: Vec<Vec<f32>> = (0..self.config.num_heads)
            .map(|h| self.embedding_engine.project(&embedding, h, ProjectionType::Value))
            .collect();
        
        self.history.push(HistoryEntry {
            event,
            embedding,
            key_projections,
            value_projections,
        });
    }
    
    pub fn compute_attention(&mut self, current: &Event) -> AttentionResult {
        let current_embedding = self.compute_embedding(current);
        
        // Calculer Query une seule fois
        let query_projections: Vec<Vec<f32>> = (0..self.config.num_heads)
            .map(|h| self.embedding_engine.project(&current_embedding, h, ProjectionType::Query))
            .collect();
        
        for entry in &self.history {
            for head in 0..self.config.num_heads {
                // Key déjà calculée !
                let score = self.dot_product(
                    &query_projections[head],
                    &entry.key_projections[head]  // Pas de recalcul
                );
                // ...
            }
        }
    }
}
```

**Gain** : Réduit les projections de 8000 à 4 par événement (juste les Query).

#### 10.3.2 Cache LRU en O(1) avec LinkedHashMap

```rust
use std::collections::HashMap;

/// LRU Cache avec O(1) pour toutes les opérations
pub struct LruCache<K, V> {
    map: HashMap<K, usize>,           // key → node index
    entries: Vec<Option<CacheNode<K, V>>>,
    head: usize,                       // Most recent
    tail: usize,                       // Least recent
    free_list: Vec<usize>,
    capacity: usize,
}

struct CacheNode<K, V> {
    key: K,
    value: V,
    prev: usize,
    next: usize,
    timestamp: Instant,
}

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(&idx) = self.map.get(key) {
            self.move_to_front(idx);
            self.entries[idx].as_ref().map(|n| &n.value)
        } else {
            None
        }
    }
    
    pub fn insert(&mut self, key: K, value: V) {
        if self.map.len() >= self.capacity {
            self.evict_lru();
        }
        
        let idx = self.alloc_node();
        self.entries[idx] = Some(CacheNode {
            key: key.clone(),
            value,
            prev: usize::MAX,
            next: self.head,
            timestamp: Instant::now(),
        });
        
        if self.head != usize::MAX {
            if let Some(ref mut head_node) = self.entries[self.head] {
                head_node.prev = idx;
            }
        }
        self.head = idx;
        if self.tail == usize::MAX {
            self.tail = idx;
        }
        
        self.map.insert(key, idx);
    }
    
    fn move_to_front(&mut self, idx: usize) {
        // Unlink from current position
        // Link at head
        // O(1) pointer manipulation
    }
    
    fn evict_lru(&mut self) {
        // Remove tail
        // O(1)
    }
}
```

**Alternative simple** : utiliser la crate `lru` qui implémente exactement ça.

```toml
[dependencies]
lru = "0.12"
```

```rust
use lru::LruCache;
use std::num::NonZeroUsize;

pub struct EmbeddingCache {
    cache: LruCache<u64, CacheEntry>,
    hits: u64,
    misses: u64,
}

impl EmbeddingCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(config.max_size).unwrap()),
            hits: 0,
            misses: 0,
        }
    }
    
    pub fn get(&mut self, key: u64) -> Option<Vec<f32>> {
        match self.cache.get(&key) {
            Some(entry) if entry.timestamp.elapsed() <= self.ttl => {
                self.hits += 1;
                Some(entry.embedding.clone())
            }
            Some(_) => {
                self.cache.pop(&key);  // TTL expired
                self.misses += 1;
                None
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }
}
```

#### 10.3.3 VecDeque pour l'historique

```rust
use std::collections::VecDeque;

pub struct AttentionEngine {
    history: VecDeque<HistoryEntry>,  // O(1) push_back et pop_front
    // ...
}

impl AttentionEngine {
    pub fn add_event(&mut self, event: Event) {
        // ...
        self.history.push_back(entry);
        
        while self.history.len() > self.config.max_history {
            self.history.pop_front();  // O(1) !
        }
    }
}
```

#### 10.3.4 Vectorisation SIMD

```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Dot product optimisé avec AVX2
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;
    
    unsafe {
        let mut sum = _mm256_setzero_ps();
        
        for i in 0..chunks {
            let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
            sum = _mm256_fmadd_ps(va, vb, sum);  // Fused multiply-add
        }
        
        // Réduction horizontale
        let mut result = hsum_avx(sum);
        
        // Traiter le reste
        for i in (chunks * 8)..n {
            result += a[i] * b[i];
        }
        
        result
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
unsafe fn hsum_avx(v: __m256) -> f32 {
    let hi = _mm256_extractf128_ps(v, 1);
    let lo = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(hi, lo);
    let hi64 = _mm_movehl_ps(sum128, sum128);
    let sum64 = _mm_add_ps(sum128, hi64);
    let hi32 = _mm_shuffle_ps(sum64, sum64, 0x1);
    let sum32 = _mm_add_ss(sum64, hi32);
    _mm_cvtss_f32(sum32)
}

// Fallback pour autres architectures
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}
```

**Alternative** : utiliser la crate `simdeez` ou `packed_simd` pour la portabilité.

#### 10.3.5 Pool d'allocations pour les projections

```rust
/// Pool de buffers réutilisables pour les projections
pub struct ProjectionPool {
    buffers: Vec<Vec<f32>>,
    head_dim: usize,
}

impl ProjectionPool {
    pub fn new(head_dim: usize, initial_capacity: usize) -> Self {
        Self {
            buffers: (0..initial_capacity)
                .map(|_| vec![0.0f32; head_dim])
                .collect(),
            head_dim,
        }
    }
    
    pub fn acquire(&mut self) -> Vec<f32> {
        self.buffers.pop().unwrap_or_else(|| vec![0.0f32; self.head_dim])
    }
    
    pub fn release(&mut self, mut buffer: Vec<f32>) {
        // Reset et retourner au pool
        buffer.fill(0.0);
        self.buffers.push(buffer);
    }
}

impl EmbeddingEngine {
    pub fn project_into(
        &self,
        embedding: &[f32],
        head: usize,
        proj_type: ProjectionType,
        output: &mut [f32],  // Buffer pré-alloué
    ) {
        let projection = match proj_type { /* ... */ };
        
        output.fill(0.0);
        for i in 0..output.len() {
            for j in 0..self.embedding_dim.min(embedding.len()) {
                output[i] += projection[i * self.embedding_dim + j] * embedding[j];
            }
        }
    }
}
```

#### 10.3.6 Index HNSW pour recherche approximative

Pour les grands historiques, remplacer la recherche exhaustive par un index HNSW (Hierarchical Navigable Small World).

```rust
use instant_distance::{Builder, HnswMap, Search};

pub struct AttentionEngineWithHnsw {
    config: AttentionConfig,
    embedding_engine: EmbeddingEngine,
    
    // Index HNSW pour recherche approximative
    hnsw_index: Option<HnswMap<EmbeddingPoint, usize>>,
    history: Vec<HistoryEntry>,
    
    // Seuil pour basculer vers HNSW
    hnsw_threshold: usize,
}

#[derive(Clone)]
struct EmbeddingPoint(Vec<f32>);

impl instant_distance::Point for EmbeddingPoint {
    fn distance(&self, other: &Self) -> f32 {
        // Distance cosine (1 - similarité)
        let dot: f32 = self.0.iter().zip(&other.0).map(|(a, b)| a * b).sum();
        1.0 - dot  // Embeddings sont normalisés
    }
}

impl AttentionEngineWithHnsw {
    pub fn compute_attention(&mut self, current: &Event) -> AttentionResult {
        let current_embedding = self.compute_embedding(current);
        
        if self.history.len() < self.hnsw_threshold {
            // Recherche exhaustive pour petits historiques
            self.compute_attention_exhaustive(&current_embedding)
        } else {
            // Recherche approximative pour grands historiques
            self.compute_attention_hnsw(&current_embedding)
        }
    }
    
    fn compute_attention_hnsw(&self, query_embedding: &[f32]) -> AttentionResult {
        let index = self.hnsw_index.as_ref().unwrap();
        let query_point = EmbeddingPoint(query_embedding.to_vec());
        
        let mut search = Search::default();
        let k = 50;  // Top-K voisins
        
        let neighbors = index.search(&query_point, &mut search)
            .take(k)
            .collect::<Vec<_>>();
        
        // Calculer attention seulement sur les voisins proches
        let mut scores = Vec::with_capacity(k);
        for neighbor in neighbors {
            let entry = &self.history[*neighbor.value];
            let score = self.compute_attention_score(query_embedding, entry);
            scores.push((entry.event.id(), score));
        }
        
        AttentionResult {
            scores,
            // ...
        }
    }
    
    fn rebuild_hnsw_index(&mut self) {
        let points: Vec<_> = self.history
            .iter()
            .enumerate()
            .map(|(idx, entry)| (EmbeddingPoint(entry.embedding.clone()), idx))
            .collect();
        
        self.hnsw_index = Some(Builder::default().build(points));
    }
}
```

**Complexité comparée :**

| Historique | Exhaustif | HNSW |
|------------|-----------|------|
| 100 | O(100) | O(100) overhead |
| 1,000 | O(1,000) | O(50) |
| 10,000 | O(10,000) | O(100) |
| 100,000 | O(100,000) | O(150) |

#### 10.3.7 Batching des calculs

Au lieu de traiter événement par événement, accumuler et traiter par batch :

```rust
pub struct BatchedAttentionEngine {
    pending_events: Vec<Event>,
    batch_size: usize,
    // ...
}

impl BatchedAttentionEngine {
    pub fn add_event(&mut self, event: Event) -> Option<Vec<AttentionResult>> {
        self.pending_events.push(event);
        
        if self.pending_events.len() >= self.batch_size {
            Some(self.process_batch())
        } else {
            None
        }
    }
    
    fn process_batch(&mut self) -> Vec<AttentionResult> {
        let batch = std::mem::take(&mut self.pending_events);
        
        // Calculer tous les embeddings en une fois
        let embeddings: Vec<Vec<f32>> = batch
            .iter()
            .map(|e| self.embedding_engine.embed(e))
            .collect();
        
        // Calculer toutes les projections Query en batch
        // (potentiellement parallélisable)
        let all_queries: Vec<Vec<Vec<f32>>> = embeddings
            .iter()
            .map(|emb| {
                (0..self.num_heads)
                    .map(|h| self.project(emb, h, Query))
                    .collect()
            })
            .collect();
        
        // Calcul matriciel pour les scores
        // Q × K^T en une opération BLAS
        
        // ...
    }
}
```

### 10.4 Tableau récapitulatif des optimisations

| Optimisation | Gain estimé | Effort | Priorité |
|--------------|-------------|--------|----------|
| Pré-calcul Key/Value | 99% réduction projections | Faible | **P1** |
| LRU O(1) avec crate `lru` | ~100x sur cache | Trivial | **P1** |
| VecDeque pour history | ~1000x sur suppression | Trivial | **P1** |
| SIMD dot product | 4-8x sur dot product | Moyen | P2 |
| Pool d'allocations | Réduit GC pressure | Moyen | P2 |
| HNSW pour grands historiques | O(log n) vs O(n) | Élevé | P3 |
| Batching | Amortit overhead | Moyen | P3 |

### 10.5 Impact global estimé

Avec les optimisations P1 uniquement :

| Métrique | Avant | Après P1 | Amélioration |
|----------|-------|----------|--------------|
| Projections/événement | 8000 | 4 | 2000x |
| Cache access | O(n) | O(1) | 10000x |
| History insert | O(n) | O(1) | 1000x |
| **Throughput estimé** | ~100 evt/s | ~50,000 evt/s | 500x |

### 10.6 Code de test de performance

```rust
#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;
    
    #[test]
    fn bench_compute_attention() {
        let config = AttentionConfig {
            max_history: 1000,
            ..Default::default()
        };
        let mut engine = AttentionEngine::new(config);
        
        // Remplir l'historique
        for i in 0..1000 {
            let event = Event::new("test")
                .with_field("value", Value::Int(i));
            engine.add_event(event);
        }
        
        // Benchmark
        let iterations = 1000;
        let start = Instant::now();
        
        for i in 0..iterations {
            let event = Event::new("query")
                .with_field("value", Value::Int(i));
            let _ = engine.compute_attention(&event);
        }
        
        let elapsed = start.elapsed();
        let per_event = elapsed / iterations;
        let throughput = iterations as f64 / elapsed.as_secs_f64();
        
        println!("Per event: {:?}", per_event);
        println!("Throughput: {:.0} events/sec", throughput);
        
        // Assert minimum performance
        assert!(throughput > 100.0, "Throughput too low: {}", throughput);
    }
}
```

---

*Document généré pour le projet Varpulis — Cyril, Février 2026*