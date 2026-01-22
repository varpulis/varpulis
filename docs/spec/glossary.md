# Glossaire

## A

### Agrégation
Opération de réduction sur une collection d'événements (sum, avg, count, etc.).

### Attention (mechanism)
Mécanisme de corrélation pondérée entre événements, inspiré des architectures transformer, utilisé de manière déterministe dans Varpulis.

### ANN (Approximate Nearest Neighbors)
Algorithme d'indexation pour trouver rapidement les vecteurs les plus similaires.

## B

### Backpressure
Mécanisme de contrôle de flux quand un consommateur est plus lent que le producteur.

## C

### CEP (Complex Event Processing)
Traitement d'événements complexes - détection de patterns dans des flux d'événements en temps réel.

### Checkpoint
Snapshot de l'état du système permettant la recovery après crash.

### Circuit Breaker
Pattern de résilience qui "ouvre le circuit" après plusieurs échecs pour éviter la surcharge.

## D

### DAG (Directed Acyclic Graph)
Graphe orienté sans cycle, utilisé pour représenter le plan d'exécution.

### DLQ (Dead Letter Queue)
File d'attente pour les messages qui n'ont pas pu être traités.

## E

### Embedding
Représentation vectorielle d'un événement dans un espace de dimension fixe.

### EPL (Event Processing Language)
Langage de traitement d'événements, terme générique (utilisé notamment par Apama).

### Event
Enregistrement immuable représentant un fait horodaté.

## F

### Fenêtre (Window)
Mécanisme pour borner les calculs sur des streams infinis.
- **Tumbling**: Fenêtres non-chevauchantes
- **Sliding**: Fenêtres chevauchantes
- **Session**: Fenêtres basées sur l'inactivité

## H

### HNSW (Hierarchical Navigable Small World)
Algorithme d'indexation pour la recherche de plus proches voisins.

### Hypertree
Structure de données optimisée pour le pattern matching sur des événements.

## I

### IR (Intermediate Representation)
Représentation intermédiaire du code entre le parsing et l'exécution.

## L

### LALRPOP
Générateur de parseur LR(1) pour Rust.

### Latence
Temps écoulé entre la réception d'un événement et la production du résultat.

## O

### OTLP (OpenTelemetry Protocol)
Protocole standard pour l'envoi de traces, métriques et logs.

## P

### Partition
Division logique d'un stream pour permettre le traitement parallèle.

### Pattern
Règle de détection définissant une séquence ou combinaison d'événements à identifier.

## R

### RocksDB
Base de données clé-valeur embarquée, utilisée pour la persistance de l'état.

## S

### Sink
Destination finale des événements traités (Kafka, HTTP, fichier, etc.).

### Source
Origine des événements (Kafka, fichier, HTTP, etc.).

### Stream
Séquence potentiellement infinie d'événements typés.

## T

### Throughput
Nombre d'événements traités par unité de temps.

### Tumbling Window
Fenêtre temporelle fixe sans chevauchement.

## V

### VarpulisQL
Le langage de requêtes et de définition de streams de Varpulis.

### Varpulis
Moteur de streaming analytics. Nommé d'après le dieu slave du vent.

## W

### Watermark
Marqueur temporel indiquant que tous les événements antérieurs ont été reçus.

### Window
Voir Fenêtre.
