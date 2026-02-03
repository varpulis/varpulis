# varpulis-zdd — Revue d'Implémentation

> Analyse comparative du code `crates/varpulis-zdd/` vs la spécification `varpulis-zdd-specification.md`.
> Couvre : correctness, architecture, performance, intégration CEP.

---

## Résumé exécutif

| Dimension | Évaluation | Détail |
|-----------|-----------|--------|
| **Conformité spec** | 90% | API complète, algorithmes corrects sauf un cas |
| **Correctness** | ⚠️ 1 bug | `product_with_optional` cas `n.var == var` |
| **Architecture** | B | Table-per-Zdd empêche le partage structurel |
| **Performance** | B+ | Algorithmes O(|Z|), mais overhead sur opérations binaires |
| **Tests** | A- | Excellente couverture, sauf le cas bugué |
| **Intégration CEP** | Non commencée | Crate standalone, pas de KleeneCapture |
| **Grade global** | **B+** | Fondation solide, quelques corrections nécessaires |

---

## 1. Conformité avec la spécification

### 1.1 Ce qui est conforme ✅

**Types fondamentaux** — Fidèles à la spec :

```
ZddRef::Empty     ⊥  (famille vide)
ZddRef::Base      ⊤  ({∅}, cas de base Kleene)
ZddRef::Node(u32) référence vers UniqueTable
```

`ZddRef` dérive `Ord` (nécessaire pour normaliser les clés de cache `(a,b)` dans les opérations commutatives) — bon choix non spécifié mais essentiel.

**UniqueTable** — Implémentation exacte :

```rust
struct UniqueTable {
    nodes: Vec<ZddNode>,                // Stockage contigu
    index: HashMap<ZddNode, u32>,       // Lookup canonique
}
```

Règle de zero-suppression correctement appliquée dans `get_or_create()` : si `hi == Empty`, retourner `lo` directement. C'est le cœur de l'efficacité ZDD.

**API publique** — Toutes les méthodes spécifiées sont présentes :

| Méthode spec | Implémentée | Notes |
|------|------|-------|
| `empty()` | ✅ | |
| `base()` | ✅ | |
| `singleton(var)` | ✅ | |
| `from_set(&[u32])` | ✅ | Gère tri + dédup |
| `union(&Zdd)` | ✅ | |
| `intersection(&Zdd)` | ✅ | |
| `difference(&Zdd)` | ✅ | |
| `product_with_optional(var)` | ⚠️ | Bug cas `n.var == var` |
| `product(&Zdd)` | ✅ | Non dans spec initiale, bonus |
| `count()` | ✅ | Mémoïsé via HashMap |
| `contains(&[u32])` | ✅ | Itératif, O(depth) |
| `is_empty()` | ✅ | |
| `iter()` | ✅ | Stack-based |
| `node_count()` | ✅ | |
| `to_dot()` | ✅ | Graphviz DOT |

Méthode bonus non spécifiée : `dump()` (debug texte), `product()` (produit cartésien), `to_sets()` (collect convenience).

### 1.2 Ce qui dévie de la spec ⚠️

| Élément spec | Statut | Impact |
|------|--------|--------|
| `SharedUniqueTable` (RwLock) | ❌ Non implémenté | Pas de thread-safety |
| `OperationCache` dans UniqueTable | ❌ Caches éphémères | Recalcul sur ops répétées |
| `gc()` mark-and-sweep | ❌ Non implémenté | Croissance monotone mémoire |
| `KleeneCapture` struct | ❌ Non implémenté | Pas d'intégration sase.rs |
| Modification `advance_run` | ❌ Non implémenté | Phase 2 attendue |
| Table partagée entre ZDDs | ❌ Table-per-Zdd | Perte partage structurel |

---

## 2. Bug critique : `product_with_optional` cas `n.var == var`

### 2.1 Le problème

Quand le ZDD contient déjà un nœud de décision pour la variable `var` passée à `product_with_optional`, le cas `n.var == var` produit un résultat incorrect.

Code actuel (product.rs L102-108) :

```rust
} else if n.var == var {
    let union_lo_hi = union_refs(n.lo, n.hi, table);
    table.get_or_create(var, union_lo_hi, union_lo_hi)
    //                       ^^^^^^^^^    ^^^^^^^^^
    //                       LO = union   HI = union
}
```

### 2.2 Dérivation mathématique

Soit F une famille décomposée au nœud `var` :
- F₀ = ensembles sans `var` (branche LO)  
- F₁ = ensembles avec `var` (branche HI stocke les résidus sans `var`)

```
product_with_optional(F, var) = F ∪ {s ∪ {var} | s ∈ F}
```

Décomposons :
- Pour s ∈ F₀ (sans `var`) : `s ∪ {var}` ajoute `var` → va dans HI
- Pour s ∈ F₁ (avec `var`) : `s ∪ {var} = s` → inchangé dans HI

Résultat correct :
- **Result_LO** = F₀ *(les ensembles sans `var` restent inchangés)*
- **Result_HI** = union(F₀, HI_original) *(résidus : anciens + nouveaux provenant de F₀)*

Le code met `union(lo, hi)` aux **deux** branches, ce qui ajoute à tort des ensembles dans LO qui n'y étaient pas.

### 2.3 Trace d'exécution — Preuve du bug

```
F = { {0} }
product_with_optional(F, 0)

Attendu : { {0} } ∪ { {0} } = { {0} }

ZDD : node(0, Empty, Base)
  lo = Empty   (aucun ensemble sans 0)
  hi = Base    (un ensemble : {0} avec 0 retiré = {})

Code :
  union_lo_hi = union(Empty, Base) = Base
  Résultat : node(0, Base, Base)
  
  LO = Base → { {} }       ← FAUX : {} n'était pas dans F
  HI = Base → { {0} }      ← correct
  Total : { {}, {0} }      ← FAUX : attendu { {0} }
```

Cas mixte plus complexe :

```
F = { {1}, {0,2} }
product_with_optional(F, 0)

Attendu : { {1}, {0,2} } ∪ { {0,1}, {0,2} } = { {1}, {0,1}, {0,2} }

ZDD : node(0, lo=ZDD({1}), hi=ZDD({2}))

Code : node(0, union({1},{2}), union({1},{2}))
     = node(0, ZDD({1},{2}), ZDD({1},{2}))
Total : { {1}, {2}, {0,1}, {0,2} }   ← FAUX : {2} en trop
                     
Fix :  node(0, ZDD({1}), union({1},{2}))
Total : { {1}, {0,1}, {0,2} }        ← CORRECT ✅
```

### 2.4 Fix

```rust
} else if n.var == var {
    // FIX: LO reste lo (pas de nouvelles combinaisons sans var)
    // HI est union(lo, hi) (anciens résidus + F₀ ajoutés au HI)
    let new_hi = union_refs(n.lo, n.hi, table);
    table.get_or_create(var, n.lo, new_hi)
}
```

### 2.5 Impact réel

**Sévérité : BASSE pour le cas d'usage Kleene, HAUTE en théorie.**

Dans l'usage Kleene typique, `product_with_optional` est toujours appelé avec un variable *nouveau* (index événement croissant monotone). L'appel séquentiel :

```rust
zdd = zdd.product_with_optional(0);  // var=0, pas de nœud 0 existant
zdd = zdd.product_with_optional(1);  // var=1, nœuds existants ont var=0 < 1
zdd = zdd.product_with_optional(2);  // var=2, nœuds existants ont var ∈ {0,1} < 2
```

garantit que `n.var < var` toujours. Le cas `n.var == var` n'est jamais atteint.

Mais le bug serait déclenché par :
- Appel `product_with_optional(0)` suivi de `product_with_optional(0)` (même variable deux fois)
- Utilisation générale en dehors du contexte Kleene séquentiel

Le test `test_product_with_optional_equivalence` utilise des vars 0..8 séquentiels et ne couvre jamais ce cas.

### 2.6 Note

La spécification (§5.4) contenait déjà ce bug — le code a fidèlement implémenté la spec incorrecte. Le bug est dans les deux.

---

## 3. Analyse architecturale

### 3.1 Table-per-Zdd — Problème structurel majeur

Chaque `Zdd` possède sa propre `UniqueTable`. Les opérations binaires (union, intersection, etc.) procèdent ainsi :

```
1. Clone la table de self               → O(n) mémoire + CPU
2. Remap les nœuds de other dans cette table → O(m) 
3. Calcul récursif sur la table unifiée  → O(n+m)
4. Retourne un nouveau Zdd avec nouvelle table
```

**Conséquences :**

- **Pas de partage structurel entre ZDDs** — La force des ZDDs est le partage de sous-graphes entre structures. Avec une table par ZDD, deux familles {A, B, C} et {A, B, D} qui partagent la sous-structure pour {A, B} n'en bénéficient pas.

- **Overhead O(n+m) sur chaque opération** — Même une union de deux ZDDs identiques nécessite un clone + remap complet.

- **Impact CEP critique** — Dans le scénario Kleene avec 1000 Runs actifs, chaque Run aurait son propre ZDD avec sa propre table. Aucun partage des nœuds communs entre Runs.

**Recommandation : Table partagée (Phase 2)**

```rust
// Architecture cible
pub struct ZddArena {
    table: UniqueTable,  // UNE table pour tout le système
}

pub struct ZddHandle {
    root: ZddRef,
    arena: Rc<RefCell<ZddArena>>,  // ou Arc<RwLock<...>> pour multi-thread
}
```

Cela permettrait :
- O(1) pour opérations entre ZDDs du même arena
- Partage structurel naturel
- GC centralisé

### 3.2 Code dupliqué — `remap_nodes`

La fonction `remap_nodes` / `remap_ref` est copiée **identiquement** dans 4 fichiers :

| Fichier | Lignes |
|---------|--------|
| `ops/union.rs` | L48-74 |
| `ops/intersection.rs` | L48-74 |
| `ops/difference.rs` | L48-74 |
| `ops/product.rs` | L155-181 |

C'est un signal que l'abstraction est incomplète. Ces fonctions devraient être dans un module partagé `ops/common.rs` ou directement dans `UniqueTable` :

```rust
impl UniqueTable {
    pub fn import_from(&mut self, source: &Zdd) -> ZddRef {
        let mut node_map = HashMap::new();
        self.import_ref(source.root(), source, &mut node_map)
    }
}
```

### 3.3 Union dupliquée dans product.rs

`product.rs` contient une réimplémentation complète de l'union (~60 lignes, `union_refs` + `union_refs_rec`) parce que l'union dans `ops/union.rs` opère sur `&Zdd` (qui possède sa table) alors que `product_with_optional_rec` a besoin d'opérer sur des `ZddRef` dans une table mutable partagée.

C'est une conséquence directe de l'architecture Table-per-Zdd. Avec une table partagée (arena), toutes les opérations travailleraient naturellement sur `ZddRef` + `&mut table`.

---

## 4. Analyse module par module

### 4.1 refs.rs — Grade : A

Rien à signaler. Enum minimal, derives corrects (`Copy`, `Eq`, `Hash`, `Ord`). `Default` = `Empty` est sémantiquement correct. Les méthodes helper (`is_empty()`, `is_base()`, `node_id()`) sont inline et sans allocation.

### 4.2 node.rs — Grade : A

`ZddNode` est `Copy` (12 bytes : u32 + 2×enum). `is_redundant()` non utilisé mais documenté — potentiellement utile pour un futur BDD bridge. Hash/Eq dérivés sont corrects pour l'indexation dans UniqueTable.

### 4.3 table.rs — Grade : A-

Le cœur est impeccable. Points mineurs :

- `get_node(id)` panic sur ID invalide — en interne c'est acceptable (les IDs sont toujours produits par `get_or_create`), mais `try_get_node()` existe sans être utilisé nulle part. Supprimer l'un ou l'autre.

- Pas de `with_capacity` hints pour l'usage typique. Pour le Kleene pattern avec n variables, on sait à l'avance que le nombre de nœuds est O(n²) au pire. Un `UniqueTable::with_capacity(n * n)` éviterait les rehash.

- `clear()` est un reset nucléaire qui invalide tous les `ZddRef::Node` existants. Dangereux sans garde-fou.

### 4.4 zdd.rs — Grade : A-

API propre, bien documentée. Issues :

- **`Clone` coûteux** — `#[derive(Clone)]` clone la totalité de la `UniqueTable` (Vec + HashMap). Pour un ZDD de 10K nœuds, c'est ~400KB copiés. Dans le contexte Kleene où `product_with_optional` crée un nouveau ZDD à chaque appel, ça signifie :

  ```
  Événement 1: clone table (10 nœuds)
  Événement 2: clone table (100 nœuds)
  ...
  Événement n: clone table (O(n²) nœuds)
  Total: O(n³) copies cumulées
  ```

  Avec une table partagée, ce serait O(1) par opération.

- **`count()` sans mémoire** — Crée un `HashMap<ZddRef, usize>` à chaque appel. Pour un ZDD stable dont le count est demandé plusieurs fois (ex: métriques), un champ `cached_count: Option<usize>` serait trivial.

- `from_set()` construit correctement une chaîne de la plus haute variable à la plus basse — ordre correct pour ZDD (variables croissantes du root vers les feuilles).

- `contains()` est itératif (pas récursif) — bon choix, pas de risque stack overflow.

### 4.5 iter.rs — Grade : B+

L'itérateur stack-based est correct et évite la récursion. Problème de performance :

```rust
0 => {
    // Clone le path pour pouvoir l'envoyer dans deux directions
    self.stack.push((node, path.clone(), 1));  // ← CLONE
    self.stack.push((n.lo, path, 0));
}
```

Chaque nœud interne sur la première visite clone le chemin accumulé. Pour un ZDD de profondeur d avec b nœuds, c'est O(d) par clone × O(b) nœuds = O(d×b) allocations totales.

**Alternative plus efficace :** encoder les décisions (0=LO, 1=HI) dans un vecteur de bits, et reconstruire le path seulement aux terminaux Base :

```rust
// Approche bitmap (esquisse)
struct ZddIterator<'a> {
    zdd: &'a Zdd,
    stack: Vec<(ZddRef, u8)>,  // (node, next_branch)
    path: Vec<u32>,            // réutilisé via push/pop
}
```

Pour l'usage actuel (principalement `take(N)` ou vérification count), l'impact est limité. Pour l'énumération exhaustive de grandes familles, ça deviendrait un bottleneck.

### 4.6 debug.rs — Grade : A

DOT output propre avec gestion correcte du `visited` set pour éviter les cycles. Terminaux stylés distinctement (⊥ gris, ⊤ vert). `dump()` avec troncature à 20 ensembles — bon choix pour le debugging.

### 4.7 ops/union.rs — Grade : B+

Algorithme standard de union ZDD avec mémoïsation. Correct.

La normalisation `(a, b) = if a <= b { (a, b) } else { (b, a) }` pour le cache exploite la commutativité — bon.

Le `get_node_info()` qui retourne `(None, Base, Empty)` pour Base est une convention astucieuse : elle traite Base comme un nœud fictif "sans variable" dont le LO est Base (le ∅ est dans la famille) et le HI est Empty (pas de variable à inclure). Cela simplifie les cas terminaux dans la récursion.

### 4.8 ops/intersection.rs — Grade : B+

Même structure que union. Algorithme correct.

Le cas clé : quand une variable est présente dans A mais pas dans B (ou inversement), l'intersection suit seulement la branche LO (la variable manquante ne peut pas être dans les ensembles de l'autre famille — c'est la zero-suppression qui garantit cela). Correct.

### 4.9 ops/difference.rs — Grade : B+

Pas de normalisation cache (non commutatif) — correct.

Le cas `av > bv` est subtil :

```rust
if av > bv {
    // b a une variable que a n'a pas
    // Les ensembles de a ne contiennent jamais bv (zero-suppression)
    // Donc seule la branche b_lo (ensembles de b sans bv) peut contenir
    // des éléments aussi dans a
    difference_rec(a, b_lo, table, cache)
}
```

Correct : si A ne décide jamais sur `bv`, alors aucun ensemble de A ne contient `bv`. Seuls les ensembles de B sans `bv` (branche lo) peuvent être identiques à des ensembles de A.

### 4.10 ops/product.rs — Grade : B

Module le plus complexe et le plus critique. Contient `product_with_optional` (opération Kleene) et `product` (produit cartésien).

**product_with_optional** : algorithme globalement correct pour le cas d'usage principal (variables monotones), avec le bug `n.var == var` détaillé en §2.

**product (Cartesian)** : algorithme correct. Le cas same-variable :

```
new_lo = product(a_lo, b_lo)                       // Ni a ni b n'a var
new_hi = union(product(a_hi, b_lo),                 // a a var, b non
               union(product(a_lo, b_hi),            // b a var, a non  
                     product(a_hi, b_hi)))            // les deux ont var
```

C'est la décomposition correcte : l'union d'ensembles est idempotente (x ∪ x = x), donc les résidus HI combinent tous les cas où au moins un opérande avait la variable.

---

## 5. Tests — Évaluation

### 5.1 Points forts

- **test_equivalence_with_naive** (lib.rs) — Vérifie chaque étape de `product_with_optional` contre une implémentation naïve `HashSet<BTreeSet<u32>>`. C'est le test gold standard. Couvre 12 variables (4096 combinaisons).

- **test_algebra_laws** — Commutativité, associativité, idempotence, identité pour union/intersection/difference. Important pour la confiance dans les opérations de base.

- **test_large_scale** — 25 variables (33M combinaisons) avec assertion sur le node_count polynomial (<1000 nœuds). Prouve empiriquement la compression ZDD.

- **test_canonicity** — Vérifie que deux constructions différentes du même ensemble produisent le même count/node_count. (Note : ne vérifie pas l'identité structurelle — voir §5.2.)

- Chaque module a ses propres tests unitaires ciblés.

### 5.2 Lacunes

| Lacune | Impact | Priorité |
|--------|--------|----------|
| `product_with_optional` avec variable existante non testé | Bug §2 non détecté | HAUTE |
| Canonicité vérifiée par count, pas par identité de root | Pourrait masquer des bugs | MOYENNE |
| Pas de test product × product_with_optional combiné | Scénarios CEP avancés | BASSE |
| Pas de benchmark CI intégré | Régression perf non détectable | BASSE |
| Pas de stress test concurrent | Thread-safety non vérifiée | N/A (pas implémenté) |

**Test manquant critique :**

```rust
#[test]
fn test_product_with_optional_duplicate_var() {
    // Bug §2: product_with_optional avec variable déjà dans le ZDD
    let zdd = Zdd::singleton(0);  // { {0} }
    let result = zdd.product_with_optional(0);
    
    // Attendu: { {0} } ∪ { {0} ∪ {0} } = { {0} }
    assert_eq!(result.count(), 1);          // ÉCHOUE: retourne 2
    assert!(result.contains(&[0]));
    assert!(!result.contains(&[]));         // ÉCHOUE: contient {}
}

#[test]
fn test_product_with_optional_mixed_var() {
    let zdd = Zdd::from_set(&[1]).union(&Zdd::from_set(&[0, 2]));
    // { {1}, {0,2} }
    
    let result = zdd.product_with_optional(0);
    // Attendu: { {1}, {0,2} } ∪ { {0,1}, {0,2} } = { {1}, {0,1}, {0,2} }
    
    assert_eq!(result.count(), 3);          // ÉCHOUE: retourne 4
    assert!(!result.contains(&[2]));        // ÉCHOUE: contient {2}
}
```

---

## 6. Analyse de performance

### 6.1 Complexité théorique

| Opération | Complexité | Notes |
|-----------|-----------|-------|
| `product_with_optional(var)` | O(\|Z\|) | Cache par nœud, 1 passe |
| `union(A, B)` | O(\|A\| × \|B\|) | Cache (a,b), pire cas |
| `intersection(A, B)` | O(\|A\| × \|B\|) | Idem |
| `difference(A, B)` | O(\|A\| × \|B\|) | Non commutatif |
| `product(A, B)` | O(\|A\|² × \|B\|²) | union interne |
| `count()` | O(\|Z\|) | Cache par nœud |
| `contains(set)` | O(depth) | Itératif |
| `iter().next()` | O(depth) | Amorti |

### 6.2 Overhead pratique

**Opérations binaires :** clone table + remap = O(n+m) *avant* le calcul réel. Pour un ZDD de 10K nœuds, c'est ~40KB de HashMap réallocé.

**product_with_optional séquentiel :**

```
Étape 1:  table=empty → product_with_optional(0) → clone empty, ~O(1)
Étape 2:  table=~3 nœuds → product_with_optional(1) → clone 3, compute ~O(3)
...
Étape n:  table=O(n²) nœuds → product_with_optional(n) → clone O(n²)
```

Cumulé sur n événements : O(Σ k²) pour k=1..n = **O(n³)** en copies de table seules.

Avec une table partagée (arena), ce serait **O(n²)** total (le calcul seul, sans copies).

### 6.3 Profil mémoire

Pour 25 variables (test existant) :
- Nœuds : <1000 × 12 bytes = ~12 KB
- Index HashMap : ~1000 entries × ~40 bytes = ~40 KB
- Total : **~52 KB** pour 33M combinaisons

Ratio compression : 33M × 4 bytes (naïf minimum) / 52 KB = **×2500** de compression.

C'est exactement le genre de gain qui justifie le ZDD pour Kleene.

---

## 7. Intégration CEP — Roadmap

### 7.1 Étape actuelle : Crate standalone ✅

Le crate est fonctionnel et auto-suffisant. Les tests prouvent la correctness et l'efficacité polynomiale. C'est une **Phase 1** complète.

### 7.2 Étape suivante : Corrections et optimisations

```
IMMÉDIAT (1-2 heures) :
├── Fix bug product_with_optional cas n.var == var
├── Ajouter tests de régression pour ce cas
├── Factoriser remap_nodes dans ops/common.rs
└── Factoriser union_refs comme opération partagée

COURT TERME (1 jour) :
├── count() avec cache optionnel (cached_count: Option<usize>)
├── iter() avec push/pop au lieu de path.clone()
└── UniqueTable::with_capacity() hints pour usage Kleene
```

### 7.3 Phase 2 : Table partagée (Arena)

C'est le changement architectural le plus impactant. Sans lui, l'intégration CEP sera sous-optimale.

```rust
/// Arène partagée pour tous les ZDDs d'un engine
pub struct ZddArena {
    table: UniqueTable,
    // Optionnel : caches persistants pour opérations fréquentes
    union_cache: HashMap<(ZddRef, ZddRef), ZddRef>,
}

/// Handle léger vers un ZDD dans l'arène
#[derive(Clone, Copy)]
pub struct ZddHandle {
    root: ZddRef,
}

impl ZddArena {
    pub fn product_with_optional(&mut self, handle: ZddHandle, var: u32) -> ZddHandle {
        // Opère directement sur la table, pas de clone ni remap
        let new_root = /* ... recursive on self.table ... */;
        ZddHandle { root: new_root }
    }
    
    pub fn union(&mut self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        // Pas de remap nécessaire — même table !
        let root = /* ... */;
        ZddHandle { root }
    }
}
```

Avantages :
- O(1) pour démarrer une opération (pas de clone/remap)
- Partage structurel naturel entre tous les ZDDs
- Caches d'opérations persistants
- GC centralisé (mark roots → sweep unreachable)

### 7.4 Phase 3 : KleeneCapture (intégration sase.rs)

Comme spécifié dans varpulis-zdd-specification.md §7, créer la structure d'intégration :

```rust
pub struct KleeneCapture {
    zdd_handle: ZddHandle,           // Référence dans l'arène partagée
    events: Vec<SharedEvent>,        // Mapping index → événement
    aliases: Vec<Option<String>>,
    next_var: u32,
}
```

Et modifier `advance_run_shared` pour utiliser `KleeneCapture::extend()` au lieu de `run.branch()` dans les états Kleene.

**Impact attendu :**

| Métrique | Avant (branch) | Après (ZDD) | Gain |
|----------|----------------|-------------|------|
| Runs actifs pour 100 B events | min(2¹⁰⁰, 10000) | 1 | ×10000 |
| Mémoire Kleene 100 events | 10000 × Run size | 1 Run + ~50KB ZDD | ×1000 |
| CPU par événement Kleene | O(runs × event_size) | O(\|Z\|) ≈ O(n²) | ×100+ |

---

## 8. Matrice qualité détaillée

| Module | Correctness | Performance | Code quality | Tests | Grade |
|--------|------------|-------------|-------------|-------|-------|
| refs.rs | ✅ | ✅ | ✅ | ✅ | **A** |
| node.rs | ✅ | ✅ | ✅ | ✅ | **A** |
| table.rs | ✅ | ✅ | ✅ | ✅ | **A-** |
| zdd.rs | ✅ | ⚠️ Clone coûteux | ✅ | ✅ | **A-** |
| iter.rs | ✅ | ⚠️ path.clone() | ✅ | ✅ | **B+** |
| debug.rs | ✅ | ✅ | ✅ | ✅ | **A** |
| ops/union.rs | ✅ | ⚠️ remap overhead | ⚠️ code dupliqué | ✅ | **B+** |
| ops/intersection.rs | ✅ | ⚠️ remap overhead | ⚠️ code dupliqué | ✅ | **B+** |
| ops/difference.rs | ✅ | ⚠️ remap overhead | ⚠️ code dupliqué | ✅ | **B+** |
| ops/product.rs | ⚠️ Bug §2 | ⚠️ remap + union dupliquée | ⚠️ plus complexe | ⚠️ cas non couvert | **B** |

---

## 9. Conclusion

Le crate `varpulis-zdd` est une fondation **solide et bien exécutée**. L'API est complète, les algorithmes sont pour l'essentiel corrects, les tests sont rigoureux avec la validation naive-vs-ZDD, et la documentation (lib.rs docstring, DOT export) est de qualité professionnelle.

Les trois priorités pour la suite :

1. **Fix immédiat** — Corriger `product_with_optional` cas `n.var == var` (30 minutes, 1 ligne changée + 2 tests ajoutés). Même si le cas Kleene séquentiel ne le déclenche pas, c'est une bombe à retardement pour tout usage non-séquentiel.

2. **Refactoring structurel** — Factoriser `remap_nodes` et `union_refs` pour éliminer la duplication (2-3 heures). C'est du housekeeping qui prépare le terrain pour l'arène.

3. **Architecture arène** — Migrer vers une table partagée (`ZddArena` + `ZddHandle`). C'est le changement qui débloque l'intégration CEP avec des gains d'ordres de grandeur. Plus complexe (2-3 jours) mais c'est le chemin vers la production.

Le crate est prêt pour le développement itératif. La qualité du code et des tests existants donne confiance pour les phases suivantes.