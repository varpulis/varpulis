# Varpulis SASE+ : Analyse et Optimisation avec ZDD

> Document de spécification technique pour l'implémentation de Zero-suppressed Decision Diagrams dans le moteur SASE+ de Varpulis.

---

## Table des matières

1. [Contexte : SASE+ et le problème du Kleene](#1-contexte-sase-et-le-problème-du-kleene)
2. [Analyse de l'implémentation actuelle](#2-analyse-de-limplémentation-actuelle)
3. [Introduction aux ZDD](#3-introduction-aux-zdd)
4. [Spécification technique varpulis-zdd](#4-spécification-technique-varpulis-zdd)
5. [Algorithme product_with_optional](#5-algorithme-product_with_optional)
6. [Structure UniqueTable](#6-structure-uniquetable)
7. [Intégration dans Run](#7-intégration-dans-run)
8. [Plan d'implémentation](#8-plan-dimplémentation)
9. [Références](#9-références)

---

## 1. Contexte : SASE+ et le problème du Kleene

### 1.1 Le modèle SASE+

SASE+ (Stream-based And Shared Execution) est un algorithme de Complex Event Processing qui permet de détecter des patterns temporels dans des flux d'événements. Un pattern SASE+ se définit ainsi :

```
PATTERN <structure>
WHERE <predicates>
WITHIN <window>
RETURN <output>
```

La **structure** suit une grammaire formelle :

```
P ::= EVENT(type, var)           -- événement simple
    | P ; P                       -- séquence (suivi de)
    | P | P                       -- disjonction (ou)
    | !P                          -- négation (absence)
    | P+                          -- Kleene+ (1 ou plus)
    | P*                          -- Kleene* (0 ou plus)
```

### 1.2 Le concept de "Run"

Un **run** représente une exécution partielle d'un pattern — un match en cours de construction. Formellement :

```
run = (q, match, t_start)
```

- **q** : état courant dans l'automate NFA du pattern
- **match** : substitution partielle {variable → événement}
- **t_start** : timestamp du premier événement matché

À chaque nouvel événement, pour chaque run actif :

```
transition(run, event) → {run'₁, run'₂, ..., run'ₖ} ∪ {∅}
```

### 1.3 Le problème du Kleene : explosion combinatoire

Considérons le pattern `A ; B+ ; C` — une séquence où B peut se répéter une ou plusieurs fois.

Quand un événement de type B arrive, l'automate a un choix **non-déterministe** :
- **Continuer** le Kleene (rester dans l'état "accumule des B")
- **Sortir** du Kleene (passer à l'état "attend C")

Avec la stratégie `skip-till-any-match`, chaque événement B peut être inclus ou exclu indépendamment. Pour n événements B consécutifs, cela génère **2ⁿ combinaisons** valides.

| n (événements B) | Combinaisons | Mémoire (naïf) |
|------------------|--------------|----------------|
| 10 | 1 024 | ~100 KB |
| 20 | 1 048 576 | ~100 MB |
| 30 | 1 073 741 824 | ~100 GB |

C'est le cœur du problème de scalabilité.

### 1.4 Stratégies de sélection SASE+

SASE+ définit des stratégies qui contraignent le non-déterminisme :

| Stratégie | Sémantique | Complexité |
|-----------|------------|------------|
| **Strict contiguity** | Événements adjacents uniquement | O(1) runs |
| **Skip-till-next-match** | Premier événement valide seulement | O(n) |
| **Skip-till-any-match** | Toutes les combinaisons | O(2ⁿ) |

**Important** : La stratégie est un paramètre **sémantique** choisi par l'utilisateur, pas une optimisation interne. Différentes stratégies produisent des résultats différents.

---

## 2. Analyse de l'implémentation actuelle

### 2.1 Points forts

#### 2.1.1 SharedEvent via Arc

```rust
pub type SharedEvent = Arc<Event>;
```

Les événements ne sont jamais deep-clonés. Quand un run capture un événement, il stocke un `Arc::clone()` — un simple incrément de compteur de référence, O(1).

#### 2.1.2 Séparation données / métadonnées

```rust
pub struct StackEntry {
    pub event: SharedEvent,      // Arc, pas de copie
    pub alias: Option<String>,
    pub timestamp: Instant,
}
```

#### 2.1.3 Partitionnement SASEXT

```rust
pub fn with_partition_by(mut self, field: String) -> Self {
    self.partition_by = Some(field);
    self
}
```

Le partitionnement par attribut (ex: `account_id`) permet de traiter les runs indépendamment par partition, réduisant les comparaisons croisées.

### 2.2 Le problème : branching explicite sur Kleene

Voici ce qui se passe quand un événement matche un état Kleene :

```rust
// Dans advance_run_shared()
if next_state.state_type == StateType::Kleene && next_state.self_loop {
    // ...
    let branch = run.branch();  // ← PROBLÈME ICI
    return RunAdvanceResult::Branch(branch);
}
```

La méthode `branch()` :

```rust
impl Run {
    pub fn branch(&self) -> Self {
        self.clone()  // Clone TOUT le Run
    }
}
```

Ce clone duplique :
- `Vec<StackEntry>` — le vecteur (pas les events, mais la structure)
- `HashMap<String, SharedEvent>` — les clés sont clonées
- Tous les champs de métadonnées

**Résultat** : avec n événements B dans `A ; B+ ; C`, on crée **n runs explicites** en mémoire. Avec `skip-till-any-match`, potentiellement **2ⁿ runs**.

### 2.3 Protection brutale : max_runs

```rust
pub struct SaseEngine {
    max_runs: usize,  // Défaut: 10000
    // ...
}
```

Quand la limite est atteinte, les nouveaux runs sont **silencieusement ignorés** :

```rust
if self.runs.len() < self.max_runs {
    self.runs.push(run);
}
// Sinon : run perdu !
```

**Conséquence** : des matches valides sont perdus sans avertissement.

### 2.4 Complexité résultante

Dans `process_runs_shared` :

```rust
while i < self.runs.len() {           // Pour chaque run (m)
    match advance_run_shared(...) {
        RunAdvanceResult::Branch(new_run) => {
            new_runs.push(new_run);    // Accumulation
        }
        // ...
    }
}
```

Complexité : **O(n × m)** où :
- n = nombre d'événements dans la fenêtre temporelle
- m = nombre de runs actifs (peut croître exponentiellement)

### 2.5 Tableau récapitulatif

| Aspect | Implémentation actuelle | Problème |
|--------|------------------------|----------|
| Stockage événements | `Arc<Event>` ✓ | Aucun |
| Représentation runs Kleene | Clone explicite | O(2ⁿ) mémoire |
| Énumération combinaisons | Eager (à chaque événement) | CPU gaspillé |
| Protection overflow | `max_runs = 10000` | Perte de matches |
| Stratégie par défaut | `SkipTillAnyMatch` | Trop permissif |

---

## 3. Introduction aux ZDD

### 3.1 Qu'est-ce qu'un ZDD ?

Un **Zero-suppressed Decision Diagram** (ZDD) est une structure de données qui représente un **ensemble d'ensembles** de façon compacte.

Dans le contexte CEP : étant donné une séquence d'événements [e₁, e₂, e₃, e₄] matchant un Kleene, chaque **combinaison valide** est un sous-ensemble. Le ZDD représente l'ensemble de tous ces sous-ensembles sans les énumérer explicitement.

### 3.2 Structure d'un nœud ZDD

```
       eᵢ?
      /    \
    LO      HI
   (exclu)  (inclus)
```

- **LO** (branche gauche) : "l'élément eᵢ n'est PAS inclus"
- **HI** (branche droite) : "l'élément eᵢ EST inclus"

Deux terminaux spéciaux :
- **⊥ (Empty)** : aucune combinaison valide sur ce chemin
- **⊤ (Base)** : une combinaison valide (l'ensemble des éléments sur le chemin HI)

### 3.3 La règle Zero-Suppression

Ce qui distingue ZDD de BDD classique :

> **Si la branche HI pointe vers ⊥, on supprime le nœud et retourne directement LO.**

Intuition : si un élément n'est jamais inclus dans aucune combinaison valide, inutile de créer un nœud pour lui.

Cette règle garantit :
1. **Forme canonique** : deux ZDD représentant le même ensemble sont structurellement identiques
2. **Compacité** : les ensembles sparse (peu d'éléments) sont très compacts

### 3.4 Exemple visuel

Représentons `S = { {e₁}, {e₂}, {e₁,e₂} }` :

```
        e₁?
       /    \
      /      \
    e₂?      e₂?
   /  \     /  \
  ⊥    ⊤   ⊥    ⊤

Lecture des chemins (de la racine vers ⊤) :
- e₁=LO, e₂=HI → {e₂}
- e₁=HI, e₂=LO → {e₁}
- e₁=HI, e₂=HI → {e₁, e₂}
```

Avec le **partage de sous-structures** (même sous-arbre = même pointeur), les deux nœuds `e₂?` identiques sont fusionnés :

```
        e₁?
       /    \
      e₂?────┘
     /  \
    ⊥    ⊤

3 combinaisons représentées par 2 nœuds seulement.
```

### 3.5 Complexité comparée

| n éléments | Combinaisons | Mémoire naïve | Mémoire ZDD (typique) |
|------------|--------------|---------------|----------------------|
| 10 | 1 024 | O(2¹⁰) | O(10²) |
| 20 | 1M | O(2²⁰) | O(20²) |
| 100 | 10³⁰ | impossible | O(100²) |

Le ZDD reste polynomial là où l'approche naïve explose.

### 3.6 Opérations clés

| Opération | Description | Complexité |
|-----------|-------------|------------|
| `union(Z₁, Z₂)` | S₁ ∪ S₂ | O(\|Z₁\| × \|Z₂\|) |
| `intersection(Z₁, Z₂)` | S₁ ∩ S₂ | O(\|Z₁\| × \|Z₂\|) |
| `difference(Z₁, Z₂)` | S₁ \ S₂ | O(\|Z₁\| × \|Z₂\|) |
| `count(Z)` | \|S\| | O(\|Z\|) |
| `contains(Z, s)` | s ∈ S ? | O(\|s\|) |
| `iter(Z)` | énumération | O(output) |

Où \|Z\| = nombre de nœuds dans le ZDD (généralement << 2ⁿ).

---

## 4. Spécification technique varpulis-zdd

### 4.1 Architecture des crates

```
crates/
  varpulis-zdd/
    src/
      lib.rs              # Re-exports publics
      refs.rs             # ZddRef enum (Empty, Base, Node)
      node.rs             # ZddNode struct
      table.rs            # UniqueTable (canonicalization + cache)
      zdd.rs              # Zdd struct principal
      ops/
        mod.rs
        union.rs          # Union ensembliste
        intersection.rs   # Intersection
        difference.rs     # Différence
        product.rs        # Produit cartésien et product_with_optional
      iter.rs             # ZddIterator (lazy enumeration)
      count.rs            # Comptage avec mémoïsation
      debug.rs            # Affichage et export DOT
    tests/
      algebraic_laws.rs   # Propriétés algébriques
      equivalence.rs      # Comparaison avec HashSet<BTreeSet>
      stress.rs           # Tests grande échelle
      kleene_sim.rs       # Simulation de patterns Kleene
    benches/
      vs_naive.rs         # Benchmark ZDD vs Vec<Vec<Event>>
```

### 4.2 Types fondamentaux

```rust
/// Référence vers un nœud ZDD ou un terminal
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ZddRef {
    /// Terminal ⊥ : ensemble vide, aucune combinaison valide
    Empty,
    /// Terminal ⊤ : {∅}, contient l'ensemble vide (base case)
    Base,
    /// Référence vers un nœud interne
    Node(u32),  // u32 suffit pour ~4 milliards de nœuds
}

/// Nœud interne du ZDD
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ZddNode {
    /// Variable (index de l'événement dans la séquence Kleene)
    pub var: u32,
    /// Branche LO : l'élément n'est PAS inclus
    pub lo: ZddRef,
    /// Branche HI : l'élément EST inclus
    pub hi: ZddRef,
}

impl ZddNode {
    pub fn new(var: u32, lo: ZddRef, hi: ZddRef) -> Self {
        Self { var, lo, hi }
    }
}
```

### 4.3 API publique minimale

```rust
pub struct Zdd {
    // ... (détails internes)
}

impl Zdd {
    // === Constructeurs ===
    
    /// Crée un ZDD vide (représente ∅)
    pub fn empty() -> Self;
    
    /// Crée le ZDD base (représente {∅})
    pub fn base() -> Self;
    
    /// Crée un ZDD singleton (représente {{var}})
    pub fn singleton(var: u32) -> Self;
    
    /// Crée un ZDD avec un seul sous-ensemble arbitraire
    pub fn from_set(elements: &[u32]) -> Self;
    
    // === Opérations ensemblistes ===
    
    /// Union : S₁ ∪ S₂
    pub fn union(&self, other: &Zdd) -> Zdd;
    
    /// Intersection : S₁ ∩ S₂
    pub fn intersection(&self, other: &Zdd) -> Zdd;
    
    /// Différence : S₁ \ S₂
    pub fn difference(&self, other: &Zdd) -> Zdd;
    
    // === Opération clé pour Kleene ===
    
    /// Étend chaque combinaison avec un nouvel élément optionnel
    /// S × {∅, {var}} = S ∪ {s ∪ {var} | s ∈ S}
    pub fn product_with_optional(&self, var: u32) -> Zdd;
    
    // === Requêtes ===
    
    /// Compte le nombre de combinaisons (sans énumérer)
    pub fn count(&self) -> usize;
    
    /// Teste si une combinaison appartient à l'ensemble
    pub fn contains(&self, elements: &[u32]) -> bool;
    
    /// Teste si le ZDD est vide
    pub fn is_empty(&self) -> bool;
    
    // === Itération ===
    
    /// Itérateur lazy sur toutes les combinaisons
    pub fn iter(&self) -> ZddIterator<'_>;
    
    // === Debug ===
    
    /// Nombre de nœuds dans le ZDD
    pub fn node_count(&self) -> usize;
    
    /// Export au format DOT (Graphviz)
    pub fn to_dot(&self) -> String;
}
```

### 4.4 Contraintes d'implémentation

1. **Thread-safety** : `Zdd` doit être `Send + Sync` pour le partage entre workers
2. **Forme canonique** : deux `Zdd` représentant le même ensemble doivent être structurellement identiques
3. **Zero-copy** : les opérations retournent de nouveaux `Zdd`, les originaux restent immutables
4. **No unsafe** : sauf justification documentée (ex: optimisation SIMD)

---

## 5. Algorithme product_with_optional

### 5.1 Sémantique

Cette opération est **cruciale** pour l'intégration Kleene. À chaque nouvel événement qui matche le Kleene :

```
product_with_optional(S, e) = S ∪ { s ∪ {e} | s ∈ S }
```

Autrement dit : chaque combinaison existante peut soit rester telle quelle, soit inclure le nouvel événement.

### 5.2 Exemple

```
S = { {e₁}, {e₂} }
product_with_optional(S, e₃) = { {e₁}, {e₂}, {e₁,e₃}, {e₂,e₃} }
```

### 5.3 Implémentation naïve (pour comprendre)

```rust
fn product_with_optional_naive(sets: &HashSet<BTreeSet<u32>>, var: u32) -> HashSet<BTreeSet<u32>> {
    let mut result = sets.clone();  // S (sans le nouvel élément)
    
    for s in sets {
        let mut extended = s.clone();
        extended.insert(var);
        result.insert(extended);  // s ∪ {var}
    }
    
    result
}
```

Complexité : O(|S| × taille moyenne des ensembles) — explose avec 2ⁿ combinaisons.

### 5.4 Implémentation ZDD

L'insight clé : `product_with_optional(Z, var)` équivaut à :

```
Z_union = union(Z, offset(Z, var))
```

Où `offset(Z, var)` ajoute `var` à chaque combinaison de Z.

Mais on peut faire mieux avec une implémentation directe :

```rust
impl Zdd {
    /// S × {∅, {var}} = S ∪ {s ∪ {var} | s ∈ S}
    pub fn product_with_optional(&self, var: u32) -> Zdd {
        // Cache pour éviter les recalculs
        let mut cache: HashMap<ZddRef, ZddRef> = HashMap::new();
        
        let new_root = self.product_with_optional_rec(self.root, var, &mut cache);
        
        // Construire le nouveau ZDD avec le nouveau root
        self.with_root(new_root)
    }
    
    fn product_with_optional_rec(
        &self,
        node: ZddRef,
        var: u32,
        cache: &mut HashMap<ZddRef, ZddRef>,
    ) -> ZddRef {
        // Cas terminaux
        match node {
            ZddRef::Empty => return ZddRef::Empty,
            ZddRef::Base => {
                // {∅} × {∅, {var}} = {∅, {var}}
                // Cela crée un nœud : var? -> (⊤, ⊤)
                // Mais avec zero-suppression, on doit créer le nœud correctement
                return self.unique_table.get_or_create(var, ZddRef::Base, ZddRef::Base);
            }
            ZddRef::Node(_) => {}
        }
        
        // Vérifier le cache
        if let Some(&cached) = cache.get(&node) {
            return cached;
        }
        
        let n = self.get_node(node);
        
        let result = if n.var < var {
            // var est "plus bas" dans l'ordre des variables
            // On doit d'abord traiter les variables existantes
            let new_lo = self.product_with_optional_rec(n.lo, var, cache);
            let new_hi = self.product_with_optional_rec(n.hi, var, cache);
            self.unique_table.get_or_create(n.var, new_lo, new_hi)
        } else if n.var == var {
            // Le nœud correspond déjà à var
            // On fusionne les branches : chaque combinaison peut avoir var ou pas
            let combined_lo = self.union_refs(n.lo, n.hi);  // Sans var
            let combined_hi = self.union_refs(n.lo, n.hi);  // Avec var
            self.unique_table.get_or_create(var, combined_lo, combined_hi)
        } else {
            // n.var > var : on doit insérer var au-dessus
            // Chaque combinaison existante peut avoir var ou pas
            // new_lo = nœud original (sans var)
            // new_hi = nœud original (avec var ajouté à chaque combinaison)
            self.unique_table.get_or_create(var, node, node)
        };
        
        cache.insert(node, result);
        result
    }
}
```

### 5.5 Cas particulier : initialisation

Au démarrage d'un Kleene, le ZDD contient uniquement `{∅}` (Base). Le premier événement :

```
product_with_optional(Base, e₁) = {∅, {e₁}}
```

Le second :

```
product_with_optional({∅, {e₁}}, e₂) = {∅, {e₁}, {e₂}, {e₁,e₂}}
```

Et ainsi de suite.

### 5.6 Complexité

- **Temps** : O(|Z|) où |Z| est le nombre de nœuds du ZDD
- **Espace** : au pire double le nombre de nœuds, mais avec partage souvent bien moins

Comparaison avec l'approche naïve :

| n événements | Combinaisons | Naïf | ZDD |
|--------------|--------------|------|-----|
| 10 | 1024 | O(1024) | O(10-100) |
| 20 | 1M | O(1M) | O(100-1000) |
| 30 | 1G | impossible | O(1000-10000) |

---

## 6. Structure UniqueTable

### 6.1 Rôle

La **UniqueTable** garantit deux propriétés essentielles :

1. **Canonicité** : un triplet (var, lo, hi) unique → un seul nœud
2. **Partage maximal** : sous-structures identiques → même pointeur

Sans UniqueTable, deux ZDD identiques auraient des structures différentes en mémoire.

### 6.2 Structure de données

```rust
use std::collections::HashMap;
use std::sync::RwLock;

/// Table d'unicité pour la canonicalization des nœuds ZDD
pub struct UniqueTable {
    /// Stockage des nœuds
    nodes: Vec<ZddNode>,
    
    /// Index inversé : (var, lo, hi) → node_id
    index: HashMap<ZddNode, u32>,
    
    /// Cache des opérations (optionnel, pour performance)
    operation_cache: OperationCache,
}

/// Cache pour les opérations binaires (union, intersection, etc.)
struct OperationCache {
    union: HashMap<(ZddRef, ZddRef), ZddRef>,
    intersection: HashMap<(ZddRef, ZddRef), ZddRef>,
    // ... autres opérations
}
```

### 6.3 Opération clé : get_or_create

```rust
impl UniqueTable {
    /// Obtient ou crée un nœud canonique.
    /// Applique automatiquement la règle zero-suppression.
    pub fn get_or_create(&mut self, var: u32, lo: ZddRef, hi: ZddRef) -> ZddRef {
        // RÈGLE ZERO-SUPPRESSION : si hi = Empty, on saute ce nœud
        if hi == ZddRef::Empty {
            return lo;
        }
        
        let node = ZddNode::new(var, lo, hi);
        
        // Vérifier si le nœud existe déjà
        if let Some(&existing_id) = self.index.get(&node) {
            return ZddRef::Node(existing_id);
        }
        
        // Créer un nouveau nœud
        let id = self.nodes.len() as u32;
        self.nodes.push(node);
        self.index.insert(node, id);
        
        ZddRef::Node(id)
    }
    
    /// Récupère un nœud par son ID
    pub fn get_node(&self, id: u32) -> &ZddNode {
        &self.nodes[id as usize]
    }
    
    /// Nombre total de nœuds
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}
```

### 6.4 Version thread-safe

Pour le multi-threading :

```rust
use std::sync::{Arc, RwLock};

pub struct SharedUniqueTable {
    inner: Arc<RwLock<UniqueTable>>,
}

impl SharedUniqueTable {
    pub fn get_or_create(&self, var: u32, lo: ZddRef, hi: ZddRef) -> ZddRef {
        // Fast path : lecture seule pour vérifier l'existence
        {
            let read_guard = self.inner.read().unwrap();
            let node = ZddNode::new(var, lo, hi);
            if let Some(&existing_id) = read_guard.index.get(&node) {
                return ZddRef::Node(existing_id);
            }
        }
        
        // Slow path : écriture pour créer le nœud
        {
            let mut write_guard = self.inner.write().unwrap();
            // Re-vérifier (un autre thread a pu le créer)
            write_guard.get_or_create(var, lo, hi)
        }
    }
}
```

### 6.5 Gestion du cache d'opérations

Le cache évite de recalculer les mêmes opérations :

```rust
impl UniqueTable {
    pub fn cached_union(&mut self, a: ZddRef, b: ZddRef) -> Option<ZddRef> {
        // Normaliser l'ordre pour le cache (union est commutative)
        let key = if a <= b { (a, b) } else { (b, a) };
        self.operation_cache.union.get(&key).copied()
    }
    
    pub fn cache_union(&mut self, a: ZddRef, b: ZddRef, result: ZddRef) {
        let key = if a <= b { (a, b) } else { (b, a) };
        self.operation_cache.union.insert(key, result);
    }
}
```

### 6.6 Garbage collection (optionnel)

Pour les longues exécutions, on peut implémenter un GC :

```rust
impl UniqueTable {
    /// Mark-and-sweep GC
    /// roots : les ZddRef actuellement utilisés
    pub fn gc(&mut self, roots: &[ZddRef]) {
        let mut marked = HashSet::new();
        
        // Phase Mark
        for &root in roots {
            self.mark(root, &mut marked);
        }
        
        // Phase Sweep : reconstruire les tables avec seulement les nœuds marqués
        // (implémentation complexe, nécessite de renuméroter les nœuds)
    }
    
    fn mark(&self, node: ZddRef, marked: &mut HashSet<u32>) {
        if let ZddRef::Node(id) = node {
            if marked.insert(id) {
                let n = self.get_node(id);
                self.mark(n.lo, marked);
                self.mark(n.hi, marked);
            }
        }
    }
}
```

---

## 7. Intégration dans Run

### 7.1 Modification de la structure Run

```rust
// AVANT
pub struct Run {
    pub current_state: usize,
    pub stack: Vec<StackEntry>,                    // ← À remplacer pour Kleene
    pub captured: HashMap<String, SharedEvent>,
    pub started_at: Instant,
    pub deadline: Option<Instant>,
    // ...
}

// APRÈS
pub struct Run {
    pub current_state: usize,
    pub stack: Vec<StackEntry>,                    // Conservé pour non-Kleene
    pub kleene_capture: Option<KleeneCapture>,     // ← NOUVEAU
    pub captured: HashMap<String, SharedEvent>,
    pub started_at: Instant,
    pub deadline: Option<Instant>,
    // ...
}
```

### 7.2 Structure KleeneCapture

```rust
/// Capture optimisée pour les états Kleene utilisant ZDD
pub struct KleeneCapture {
    /// Le ZDD représentant toutes les combinaisons valides
    zdd: Zdd,
    
    /// Mapping index → événement capturé
    events: Vec<SharedEvent>,
    
    /// Alias associés aux événements
    aliases: Vec<Option<String>>,
    
    /// Index du prochain événement
    next_var: u32,
}

impl KleeneCapture {
    /// Crée une nouvelle capture Kleene vide
    pub fn new() -> Self {
        Self {
            zdd: Zdd::base(),  // Commence avec {∅}
            events: Vec::new(),
            aliases: Vec::new(),
            next_var: 0,
        }
    }
    
    /// Ajoute un événement qui peut être inclus ou non dans les combinaisons
    pub fn extend(&mut self, event: SharedEvent, alias: Option<String>) {
        let var = self.next_var;
        self.next_var += 1;
        
        self.events.push(event);
        self.aliases.push(alias);
        
        // Chaque combinaison existante peut maintenant inclure ou exclure cet événement
        self.zdd = self.zdd.product_with_optional(var);
    }
    
    /// Applique un filtre sur les combinaisons
    pub fn filter<F>(&mut self, predicate: F)
    where
        F: Fn(&[&Event]) -> bool,
    {
        // Construire un nouveau ZDD avec seulement les combinaisons valides
        let mut valid_combinations = Zdd::empty();
        
        for combination in self.zdd.iter() {
            let events: Vec<&Event> = combination
                .iter()
                .map(|&idx| self.events[idx as usize].as_ref())
                .collect();
            
            if predicate(&events) {
                valid_combinations = valid_combinations.union(&Zdd::from_set(&combination));
            }
        }
        
        self.zdd = valid_combinations;
    }
    
    /// Nombre de combinaisons valides (sans les énumérer)
    pub fn count(&self) -> usize {
        self.zdd.count()
    }
    
    /// Itère sur les combinaisons, produisant des MatchResult
    pub fn iter_matches(&self) -> impl Iterator<Item = Vec<SharedEvent>> + '_ {
        self.zdd.iter().map(move |combination| {
            combination
                .iter()
                .map(|&idx| Arc::clone(&self.events[idx as usize]))
                .collect()
        })
    }
    
    /// Vérifie si au moins une combinaison est valide
    pub fn has_valid_combinations(&self) -> bool {
        !self.zdd.is_empty()
    }
}
```

### 7.3 Modification de advance_run

```rust
fn advance_run_shared(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: SharedEvent,
) -> RunAdvanceResult {
    let current_state = &nfa.states[run.current_state];
    
    // ... (code existant pour non-Kleene) ...
    
    // Nouveau : gestion Kleene avec ZDD
    if current_state.state_type == StateType::Kleene && current_state.self_loop {
        if event_matches_state(nfa, &event, current_state, &run.captured) {
            // Initialiser KleeneCapture si nécessaire
            if run.kleene_capture.is_none() {
                run.kleene_capture = Some(KleeneCapture::new());
            }
            
            let kleene = run.kleene_capture.as_mut().unwrap();
            kleene.extend(Arc::clone(&event), current_state.alias.clone());
            
            // Mettre à jour captured avec le dernier événement (pour les références)
            if let Some(ref alias) = current_state.alias {
                run.captured.insert(alias.clone(), Arc::clone(&event));
            }
            
            // PAS DE BRANCH ! Le ZDD représente implicitement toutes les combinaisons
            return RunAdvanceResult::Continue;
        }
    }
    
    // ... (reste du code) ...
}
```

### 7.4 Production des résultats

Quand le pattern est complété, il faut matérialiser les combinaisons :

```rust
fn complete_run(run: &Run) -> Vec<MatchResult> {
    match &run.kleene_capture {
        None => {
            // Pas de Kleene : un seul résultat
            vec![MatchResult {
                captured: run.captured.clone(),
                stack: run.stack.clone(),
                duration: run.started_at.elapsed(),
            }]
        }
        Some(kleene) => {
            // Avec Kleene : une résultat par combinaison
            kleene
                .iter_matches()
                .map(|kleene_events| {
                    let mut captured = run.captured.clone();
                    // Ajouter les événements Kleene aux captured
                    // (logique spécifique selon ton schéma d'alias)
                    
                    MatchResult {
                        captured,
                        stack: run.stack.clone(),  // ou reconstruire depuis kleene_events
                        duration: run.started_at.elapsed(),
                    }
                })
                .collect()
        }
    }
}
```

### 7.5 Stratégies de matérialisation

On peut être encore plus lazy :

```rust
pub enum MatchOutput {
    /// Matérialisation complète (comportement actuel)
    Eager(Vec<MatchResult>),
    
    /// Matérialisation lazy via itérateur
    Lazy(Box<dyn Iterator<Item = MatchResult>>),
    
    /// Juste le compte (pour agrégations)
    CountOnly(usize),
    
    /// Premier match seulement
    First(Option<MatchResult>),
}

impl SaseEngine {
    pub fn process_lazy(&mut self, event: &Event) -> impl Iterator<Item = MatchResult> {
        // Retourne un itérateur qui matérialise à la demande
    }
    
    pub fn count_matches(&mut self, event: &Event) -> usize {
        // Compte sans matérialiser
    }
}
```

---

## 8. Plan d'implémentation

### 8.1 Phase 1 : Crate varpulis-zdd standalone (1-2 semaines)

**Objectifs :**
- Implémenter ZddRef, ZddNode, UniqueTable
- Implémenter les opérations de base : union, intersection, difference
- Implémenter product_with_optional
- Implémenter count et iter
- Tests exhaustifs contre implémentation naïve

**Critères de validation :**
```rust
#[test]
fn test_equivalence_with_naive() {
    let mut naive: HashSet<BTreeSet<u32>> = HashSet::new();
    naive.insert(BTreeSet::new());  // {∅}
    
    let mut zdd = Zdd::base();
    
    for var in 0..20 {
        // Opération naïve
        let mut new_naive = naive.clone();
        for s in &naive {
            let mut extended = s.clone();
            extended.insert(var);
            new_naive.insert(extended);
        }
        naive = new_naive;
        
        // Opération ZDD
        zdd = zdd.product_with_optional(var);
        
        // Vérification
        assert_eq!(zdd.count(), naive.len());
        
        for combination in zdd.iter() {
            let set: BTreeSet<u32> = combination.into_iter().collect();
            assert!(naive.contains(&set));
        }
    }
}
```

### 8.2 Phase 2 : Intégration dans varpulis-runtime (1 semaine)

**Objectifs :**
- Ajouter KleeneCapture à Run
- Modifier advance_run pour utiliser ZDD sur états Kleene
- Adapter la production de MatchResult

**Critères de validation :**
- Tous les tests existants passent
- Nouveau test avec Kleene long (50+ événements)
- Benchmark montrant l'amélioration mémoire

### 8.3 Phase 3 : Optimisations (1 semaine)

**Objectifs :**
- Cache d'opérations dans UniqueTable
- Version thread-safe pour parallélisation
- Garbage collection pour longues exécutions
- Intégration des filtres WHERE dans le ZDD

### 8.4 Phase 4 : Documentation et benchmarks (quelques jours)

**Objectifs :**
- Documentation API complète
- Benchmarks publiables (vs Flink, Kafka Streams sur patterns Kleene)
- Article de blog technique (pour positionnement)

---

## 9. Références

### 9.1 Papers académiques

1. **Minato, S.** (1993). "Zero-suppressed BDDs for Set Manipulation in Combinatorial Problems". *IEEE Design Automation Conference*.
   - Paper fondateur des ZDD

2. **Wu, E., Diao, Y., Rizvi, S.** (2006). "High-Performance Complex Event Processing over Streams". *SIGMOD*.
   - Paper SASE+ original

3. **Agrawal, J., et al.** (2008). "Efficient Pattern Matching over Event Streams". *SIGMOD*.
   - Optimisations SASE+

### 9.2 Livres

1. **Knuth, D.** "The Art of Computer Programming, Volume 4A", Section 7.1.4.
   - Traitement exhaustif des BDD/ZDD

### 9.3 Implémentations de référence

1. **CUDD** (Colorado University Decision Diagram) - C
   - http://vlsi.colorado.edu/~fabio/CUDD/
   
2. **Sylvan** - C, parallèle
   - https://github.com/trolando/sylvan

3. **biodivine-lib-bdd** - Rust, BDD (pas ZDD mais utile pour patterns)
   - https://github.com/sybila/biodivine-lib-bdd

---

## Annexe A : Prompt pour Claude Code

```markdown
# Objectif
Créer une crate Rust `varpulis-zdd` implémentant des Zero-suppressed Decision Diagrams 
optimisés pour le pattern matching CEP (Complex Event Processing).

# Contexte
Cette crate sera utilisée dans le moteur SASE+ de Varpulis pour représenter efficacement 
les combinaisons d'événements dans les patterns Kleene (A+, A*). Elle remplacera 
l'approche actuelle qui clone explicitement les runs, causant une explosion O(2ⁿ) mémoire.

# Document de spécification
Voir le fichier `varpulis-zdd-specification.md` pour :
- L'analyse complète du problème
- Les algorithmes détaillés (notamment product_with_optional)
- La structure UniqueTable
- L'intégration prévue dans Run

# Contraintes techniques
- Rust stable 2021 edition, no unsafe sauf si justifié et documenté
- Zero-copy où possible (Arc pour les données partagées)
- Thread-safe : Zdd doit être Send + Sync
- Forme canonique obligatoire (deux ZDD représentant le même ensemble = même structure)
- Aucune dépendance externe sauf std (pour l'instant)

# Structure attendue
crates/varpulis-zdd/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── refs.rs         # ZddRef enum
│   ├── node.rs         # ZddNode struct  
│   ├── table.rs        # UniqueTable
│   ├── zdd.rs          # Zdd struct principal
│   ├── ops/
│   │   ├── mod.rs
│   │   ├── union.rs
│   │   ├── intersection.rs
│   │   ├── difference.rs
│   │   └── product.rs
│   ├── iter.rs
│   ├── count.rs
│   └── debug.rs
└── tests/
    ├── algebraic_laws.rs
    ├── equivalence.rs
    └── stress.rs

# API minimale requise
Voir section 4.3 du document de spécification.

# Tests requis
1. Propriétés algébriques (union commutative/associative, etc.)
2. Équivalence avec HashSet<BTreeSet<u32>> pour product_with_optional
3. Stress test : 30 variables, vérifier mémoire bornée et count correct (2³⁰)
4. Test forme canonique : mêmes opérations dans ordre différent = même ZDD

# Priorité d'implémentation
1. ZddRef, ZddNode, UniqueTable avec zero-suppression rule
2. Zdd::base(), Zdd::empty(), Zdd::singleton()
3. Zdd::union() - c'est la brique de base
4. Zdd::product_with_optional() - critique pour Kleene
5. Zdd::count() avec mémoïsation
6. Zdd::iter() - itérateur lazy
7. Tests d'équivalence
8. Optimisations (cache, thread-safety)
```

---

*Document généré pour le projet Varpulis — Cyril, Février 2026*