# Varpulis Production Deployment Guide

This guide covers deploying Varpulis CEP engine in production environments.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Docker Deployment](#docker-deployment)
3. [Kubernetes Deployment](#kubernetes-deployment)
4. [Configuration](#configuration)
5. [Connectors](#connectors)
6. [Monitoring](#monitoring)
7. [High Availability](#high-availability)
8. [Performance Tuning](#performance-tuning)
9. [Security](#security)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| Memory | 512 MB | 2+ GB |
| Disk | 1 GB | 10+ GB (with persistence) |

### Dependencies

- Docker 20.10+ or Kubernetes 1.24+
- (Optional) Kafka 2.8+ for Kafka connector
- (Optional) Prometheus for metrics collection

---

## Docker Deployment

### Building the Image

```bash
# Build from the project root
docker build -f deploy/docker/Dockerfile -t varpulis/varpulis:latest .

# Build with Kafka support
docker build -f deploy/docker/Dockerfile \
  --build-arg FEATURES="kafka" \
  -t varpulis/varpulis:latest-kafka .

# Build with all connectors
docker build -f deploy/docker/Dockerfile \
  --build-arg FEATURES="all-connectors" \
  -t varpulis/varpulis:latest-full .
```

### Running with Docker

```bash
# Basic run with a query file
docker run -d \
  --name varpulis \
  -p 9000:9000 \
  -p 9090:9090 \
  -v /path/to/queries:/app/queries:ro \
  varpulis/varpulis:latest \
  run /app/queries/queries.vpl

# With configuration file
docker run -d \
  --name varpulis \
  -p 9000:9000 \
  -p 9090:9090 \
  -v /path/to/config.yaml:/app/config.yaml:ro \
  -v /path/to/queries:/app/queries:ro \
  varpulis/varpulis:latest \
  --config /app/config.yaml \
  run /app/queries/queries.vpl

# With Kafka connector
docker run -d \
  --name varpulis \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -p 9000:9000 \
  -v /path/to/queries:/app/queries:ro \
  varpulis/varpulis:latest-kafka \
  run /app/queries/queries.vpl
```

### Docker Compose Example

```yaml
version: '3.8'

services:
  varpulis:
    image: varpulis/varpulis:latest
    ports:
      - "9000:9000"   # HTTP webhook
      - "9090:9090"   # Prometheus metrics
    volumes:
      - ./queries:/app/queries:ro
      - ./config.yaml:/app/config.yaml:ro
      - varpulis-state:/app/state
    command: ["--config", "/app/config.yaml", "run", "/app/queries/queries.vpl"]
    environment:
      - RUST_LOG=info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped

volumes:
  varpulis-state:
```

---

## Kubernetes Deployment

### Using Kustomize

```bash
# Deploy to development environment
kubectl apply -k deploy/kubernetes/overlays/development

# Deploy to production environment
kubectl apply -k deploy/kubernetes/overlays/production

# View generated manifests before applying
kubectl kustomize deploy/kubernetes/overlays/production
```

### Manual Deployment

```bash
# Create namespace
kubectl create namespace varpulis

# Apply base manifests
kubectl apply -f deploy/kubernetes/base/

# Apply ConfigMap with your queries
kubectl create configmap varpulis-queries \
  --from-file=queries.vpl=./your-queries.vpl \
  -n varpulis
```

### Custom Configuration

1. **Create a ConfigMap for queries:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: varpulis-config
  namespace: varpulis
data:
  queries.vpl: |
    event Transaction:
        amount: float
        user_id: str

    stream HighValue = Transaction
        .where(amount > 10000)
        .emit(alert_type: "high_value", user_id: user_id, amount: amount)
```

2. **Create a Secret for sensitive data:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: varpulis-secrets
  namespace: varpulis
type: Opaque
stringData:
  api-key: "your-secret-api-key"
  kafka-password: "kafka-secret"
```

3. **Update Deployment to use secrets:**

```yaml
env:
  - name: VARPULIS_API_KEY
    valueFrom:
      secretKeyRef:
        name: varpulis-secrets
        key: api-key
```

---

## Configuration

### Configuration File

Generate an example configuration:

```bash
# YAML format
varpulis config-gen --format yaml > config.yaml

# TOML format
varpulis config-gen --format toml > config.toml
```

### Example Configuration (YAML)

```yaml
# Path to query file
query_file: /app/queries/queries.vpl

# Server configuration
server:
  port: 9000
  bind: "0.0.0.0"
  metrics_enabled: true
  metrics_port: 9090

# Processing configuration
processing:
  workers: 8              # Number of worker threads
  partition_by: source_id # Partition key for parallel processing

# Kafka connector
kafka:
  bootstrap_servers: "kafka:9092"
  consumer_group: "varpulis-prod"
  input_topic: "events"
  output_topic: "alerts"

# HTTP webhook input
http_webhook:
  enabled: true
  port: 9000
  bind: "0.0.0.0"
  api_key: "${VARPULIS_API_KEY}"
  rate_limit: 10000
  max_batch_size: 1000

# Logging
logging:
  level: info
  format: json
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VARPULIS_CONFIG` | Path to config file | - |
| `VARPULIS_API_KEY` | API key for authentication | - |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | localhost:9092 |
| `RUST_LOG` | Log level | info |
| `RUST_BACKTRACE` | Enable backtraces | 0 |

---

## Connectors

### Kafka Connector

```yaml
# In your config.yaml
kafka:
  bootstrap_servers: "broker1:9092,broker2:9092"
  consumer_group: "varpulis-consumer"
  input_topic: "events"
  output_topic: "alerts"
  auto_commit: true
  auto_offset_reset: "earliest"  # or "latest"
```

### HTTP Webhook Input

Events can be sent via HTTP POST:

```bash
# Single event
curl -X POST http://localhost:9000/event \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{"event_type": "Transaction", "amount": 15000, "user_id": "user123"}'

# Batch events
curl -X POST http://localhost:9000/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '[{"event_type": "Transaction", "amount": 100}, {"event_type": "Transaction", "amount": 200}]'
```

---

## Monitoring

### Health Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness probe - returns 200 if running |
| `GET /ready` | Readiness probe - returns 200 if ready to accept traffic |
| `GET /metrics` | Prometheus metrics (port 9090) |

### Prometheus Metrics

Key metrics exposed:

```
# Events processed
varpulis_events_processed_total{stream="...", event_type="..."}

# Processing latency
varpulis_processing_duration_seconds{stream="..."}

# Alerts generated
varpulis_alerts_generated_total{alert_type="..."}

# Active streams
varpulis_streams_active

# Error counts
varpulis_errors_total{error_type="..."}
```

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'varpulis'
    static_configs:
      - targets: ['varpulis:9090']
    metrics_path: /metrics
```

### Grafana Dashboard

A sample Grafana dashboard JSON is available at `deploy/docker/grafana/dashboards/varpulis.json`.

---

## High Availability

### Multi-replica Deployment

For high availability, deploy multiple replicas:

```yaml
# In Kubernetes deployment
spec:
  replicas: 3

  # Pod anti-affinity to spread across nodes
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            labelSelector:
              matchLabels:
                app: varpulis
            topologyKey: kubernetes.io/hostname
```

### State Persistence

For stateful patterns (e.g., sequences, windows), enable RocksDB persistence:

```bash
# Build with persistence feature
docker build -f deploy/docker/Dockerfile \
  --build-arg FEATURES="persistence" \
  -t varpulis/varpulis:latest-persistent .
```

Mount a persistent volume for state:

```yaml
volumes:
  - name: state
    persistentVolumeClaim:
      claimName: varpulis-state
```

### Kafka Partitioning

For parallel processing with Kafka:

1. Use multiple partitions in your input topic
2. Deploy multiple Varpulis instances with the same consumer group
3. Use `--partition-by` to ensure related events go to the same instance

---

## Performance Tuning

### Worker Threads

```bash
# Set worker threads via CLI
varpulis simulate --workers 8 --program queries.vpl --events events.evt

# Or via config file
processing:
  workers: 8
```

**Guidelines:**
- For CPU-bound workloads: workers = number of CPU cores
- For I/O-bound workloads: workers = 2 × number of CPU cores
- Leave 1-2 cores for system tasks

### Memory Optimization

```yaml
# Kubernetes resource limits
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "4000m"
```

### Batch Processing

For high-throughput scenarios, use batch ingestion:

```bash
# Preload mode for event files
varpulis simulate --preload --immediate --program queries.vpl --events events.evt
```

---

## Security

### TLS/SSL

Enable TLS for WebSocket server:

```bash
varpulis server \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem \
  --bind 0.0.0.0 \
  --port 9443
```

### API Key Authentication

```bash
# Set API key via environment variable
export VARPULIS_API_KEY="your-secure-api-key"
varpulis server --api-key "$VARPULIS_API_KEY"

# Or in config file (use environment variable reference)
auth:
  api_key: "${VARPULIS_API_KEY}"
```

### Network Policies (Kubernetes)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: varpulis-network-policy
  namespace: varpulis
spec:
  podSelector:
    matchLabels:
      app: varpulis
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - port: 9090  # Prometheus
    - from:
        - namespaceSelector:
            matchLabels:
              name: api-gateway
      ports:
        - port: 9000  # HTTP webhook
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: kafka
      ports:
        - port: 9092
```

### Security Best Practices

1. **Run as non-root user** - The Docker image runs as user `varpulis` (UID 1000)
2. **Read-only filesystem** - Mount query files as read-only
3. **Network isolation** - Use network policies to restrict traffic
4. **Secret management** - Use Kubernetes Secrets or a vault for sensitive data
5. **Regular updates** - Keep the base image and dependencies updated

---

## Troubleshooting

### Common Issues

#### Container fails to start

```bash
# Check logs
docker logs varpulis

# Common causes:
# - Invalid query syntax: Check your .vpl files
# - Missing config file: Verify mount paths
# - Port already in use: Change port mapping
```

#### No events being processed

1. Check if the source is connected:
   ```bash
   curl http://localhost:9000/health
   ```

2. Verify Kafka connectivity:
   ```bash
   kafka-console-consumer --bootstrap-server kafka:9092 --topic events --from-beginning
   ```

3. Check logs for errors:
   ```bash
   kubectl logs -f deployment/varpulis -n varpulis
   ```

#### High memory usage

1. Reduce window sizes in queries
2. Limit the number of concurrent patterns
3. Enable persistence to offload state to disk
4. Increase memory limits if needed

#### High latency

1. Increase worker threads
2. Use partitioning for parallel processing
3. Check Kafka consumer lag
4. Review complex pattern matching queries

### Debug Mode

Enable debug logging:

```bash
RUST_LOG=debug varpulis run queries.vpl
```

Or in Kubernetes:

```yaml
env:
  - name: RUST_LOG
    value: "debug,varpulis=trace"
  - name: RUST_BACKTRACE
    value: "1"
```

### Getting Help

- GitHub Issues: https://github.com/varpulis/varpulis/issues
- Documentation: https://varpulis.io/docs

---

## Quick Reference

### CLI Commands

```bash
# Run a VPL file
varpulis run --file queries.vpl

# Check query syntax
varpulis check queries.vpl

# Start the API server (multi-tenant SaaS mode)
varpulis server --port 9000 --api-key "your-key" --metrics --state-dir /var/lib/varpulis/state

# Simulate with event file
varpulis simulate --program queries.vpl --events data.evt --workers 8

# Deploy a pipeline to a remote server
varpulis deploy --server http://localhost:9000 --api-key "key" --file queries.vpl --name "my-pipeline"

# List deployed pipelines
varpulis pipelines --server http://localhost:9000 --api-key "key"

# Generate config template
varpulis config-gen --format yaml > config.yaml
```

### Docker Quick Start

```bash
# Pull and run
docker run -d \
  --name varpulis \
  -p 9000:9000 \
  -v $(pwd)/queries.vpl:/app/queries/queries.vpl:ro \
  varpulis/varpulis:latest \
  run /app/queries/queries.vpl
```

### Kubernetes Quick Start

```bash
# Deploy
kubectl apply -k deploy/kubernetes/base/

# Check status
kubectl get pods -n varpulis

# View logs
kubectl logs -f -l app.kubernetes.io/name=varpulis -n varpulis

# Port forward for testing
kubectl port-forward svc/varpulis 9000:9000 -n varpulis
```
