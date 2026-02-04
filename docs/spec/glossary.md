# Glossary

## A

### Aggregation
Reduction operation on a collection of events (sum, avg, count, etc.).

### Attention (mechanism)
Weighted correlation mechanism between events, inspired by transformer architectures, used deterministically in Varpulis.

### ANN (Approximate Nearest Neighbors)
Indexing algorithm to quickly find the most similar vectors.

## B

### Backpressure
Flow control mechanism when a consumer is slower than the producer.

## C

### CEP (Complex Event Processing)
Complex event processing - pattern detection in real-time event streams.

### Checkpoint
System state snapshot enabling recovery after crash.

### Circuit Breaker
Resilience pattern that "opens the circuit" after multiple failures to avoid overload.

## D

### DAG (Directed Acyclic Graph)
Directed graph without cycles, used to represent the execution plan.

### DLQ (Dead Letter Queue)
Queue for messages that could not be processed.

## E

### Embedding
Vector representation of an event in a fixed-dimension space.

### EPL (Event Processing Language)
Event processing language, generic term (used notably by Apama/Cumulocity).

### Event
Immutable record representing a timestamped fact.

## F

### Flink
Apache Flink - distributed stream processing framework. See [comparison](overview.md#vs-apache-flink).

## H

### HNSW (Hierarchical Navigable Small World)
Indexing algorithm for nearest neighbor search.

### Hypertree
Data structure optimized for pattern matching on events.

## I

### IR (Intermediate Representation)
Intermediate code representation between parsing and execution.

## K

### Kafka Streams
Stream processing library for Apache Kafka. See [comparison](overview.md#vs-kafka-streams).

## L

### Latency
Time elapsed between receiving an event and producing the result.

## O

### OTLP (OpenTelemetry Protocol)
Standard protocol for sending traces, metrics, and logs.

## P

### Partition
Logical division of a stream to enable parallel processing.

### Pattern
Detection rule defining a sequence or combination of events to identify.

### Pest
PEG (Parsing Expression Grammar) parser generator for Rust, used by VarpulisQL.

## R

### RocksDB
Embedded key-value database, used for state persistence.

## S

### Output Connector
Final destination of processed events, routed via `.to()` (Kafka, HTTP, file, etc.).

### Source
Origin of events (Kafka, file, HTTP, etc.).

### Stream
Potentially infinite sequence of typed events.

## T

### Throughput
Number of events processed per unit of time.

### Tumbling Window
Fixed temporal window without overlap.

## V

### VarpulisQL
The query and stream definition language of Varpulis.

### Varpulis
Streaming analytics engine. Named after the Slavic wind spirit.

## W

### Watermark
Temporal marker indicating that all prior events have been received.

### Window
Mechanism to bound computations on infinite streams.
- **Tumbling**: Non-overlapping windows
- **Sliding**: Overlapping windows
- **Session**: Inactivity-based windows
