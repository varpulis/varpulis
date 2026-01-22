# Varpulis - Vue d'ensemble

## Vision

Varpulis est un moteur de streaming analytics nouvelle génération combinant :
- La syntaxe intuitive d'Apama (EPL)
- L'efficacité des hypertrees pour la détection de patterns
- Les capacités des architectures transformer (attention mechanisms) de manière **déterministe**
- Une approche unifiée simplifiant radicalement le développement par rapport à Apama

## Nom du projet

**Varpulis** - Dans la mythologie slave, Varpulis est l'esprit/compagnon du dieu du tonnerre, associé aux vents violents et aux tempêtes. Ce nom évoque :
- **Vitesse** : Les vents rapides qui transportent les événements
- **Puissance** : La force des tempêtes pour traiter des millions d'événements
- **Flux** : Le mouvement continu des streams de données

## Langage cible

**Rust** - choisi pour :
- Performance native (requis pour CEP haute fréquence)
- Sécurité mémoire sans garbage collector
- Excellent écosystème async (`tokio`, `async-std`)
- Support SIMD et optimisations bas niveau
- Facilité de déploiement (binaire statique)

## Différenciateurs clés vs Apama

| Caractéristique | Apama | Varpulis |
|----------------|-------|----------|
| **Agrégation multi-streams** | ❌ Impossible directement | ✅ Native |
| **Parallélisation** | `spawn` complexe et risqué | Déclarative et supervisée |
| **Listeners vs Streams** | Séparés et complexes | Unifiés dans un concept unique |
| **Debugging** | Difficile | Observabilité intégrée |
| **Pattern detection** | Hypertrees uniquement | Hypertrees + Attention |
| **Latence** | < 1ms possible | Configurable (< 1ms à < 100ms) |

## Composants principaux

1. **Compiler** - Compilation VarpulisQL → IR optimisé
2. **Runtime Engine** - Exécution des graphes de traitement
3. **Pattern Matcher** - Détection de patterns via hypertrees
4. **Attention Engine** - Corrélation déterministe entre événements
5. **State Manager** - Gestion de l'état (in-memory / RocksDB)
6. **Observability Layer** - Métriques, traces, logs

## Voir aussi

- [Architecture système](../architecture/system.md)
- [Langage VarpulisQL](../language/overview.md)
- [Roadmap](roadmap.md)
