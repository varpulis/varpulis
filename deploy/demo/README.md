# Varpulis Demo Deployment

One-command install for the full Varpulis demo stack (3 coordinators, 4 workers, Kafka, MQTT, Grafana, PostgreSQL, Caddy reverse proxy).

## Quick Install

```bash
curl -sSL https://raw.githubusercontent.com/varpulis/varpulis/main/deploy/demo/install.sh | bash
```

This will:
1. Clone the repo (or update if already cloned)
2. Generate API keys and secrets
3. Pull pre-built Docker images from GHCR (no Rust compile!)
4. Start all services
5. Deploy the demo pipeline with MQTT + Kafka connectors

**Runtime:** ~2 min fresh install (image pull), ~30s update.

## Prerequisites

- Linux server (tested on Ubuntu 22.04+, Debian 12+)
- Docker + Docker Compose v2
- Ports 80 and 443 open
- TLS certificate at `/etc/caddy/certs/origin.pem` and `/etc/caddy/certs/origin-key.pem`

### First-time server setup

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in for group to take effect

# Open firewall
sudo ufw allow 22/tcp && sudo ufw allow 80/tcp && sudo ufw allow 443/tcp && sudo ufw --force enable
```

## Update to Latest

Same command — it's idempotent:
```bash
curl -sSL https://raw.githubusercontent.com/varpulis/varpulis/main/deploy/demo/install.sh | bash
```

Or if you're already on the server:
```bash
cd ~/varpulis-demo/repo/deploy/demo && ./deploy-pull.sh
```

## Architecture

```
Internet → Caddy (TLS) → Coordinators ×3 (Raft consensus)
                         → Workers ×4 (event processing)
                         → Web UI (Vue SPA)
                         → Kafka (output topics)
                         → MQTT (input events)
                         → Grafana + Prometheus (monitoring)
                         → PostgreSQL (user DB)
```

## Deploy Your Own VPL Pipeline

Once the demo is running, deploy a custom pipeline via the REST API:

```bash
API_KEY=$(grep VARPULIS_API_KEY ~/varpulis-demo/repo/deploy/demo/.env | cut -d= -f2)
COORD="https://demo.varpulis-cep.com"  # or http://localhost:9100 if testing locally

# Deploy a pipeline
curl -X POST "$COORD/api/v1/cluster/pipeline-groups" \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-pipeline",
    "pipelines": [{
      "name": "HighTemp",
      "source": "stream HighTemp = TempReading .where(value > 30) .emit(sensor: sensor_id, temp: value)"
    }]
  }'
```

### Inject events via REST

```bash
# Get the pipeline group ID from the deploy response
GROUP_ID="<from deploy response>"

curl -X POST "$COORD/api/v1/cluster/pipeline-groups/$GROUP_ID/inject" \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_type": "TempReading", "fields": {"sensor_id": "S1", "value": 42.5}}'
```

### Inject events via MQTT

The demo includes an MQTT broker (Mosquitto). Publish events to topic `events/<event_type>`:

```bash
# From inside the Docker network (or expose port 1883)
mosquitto_pub -h mosquitto -t "events/TempReading" \
  -m '{"sensor_id": "S1", "value": 42.5}'
```

### Use Kafka connectors

VPL pipelines can read from and write to Kafka topics. The demo includes a Kafka broker at `kafka:9092` (internal network).

```vpl
connector MarketFeed = kafka(brokers: "kafka:9092", group_id: "my-consumer")
connector OutputTopic = kafka(brokers: "kafka:9092")

stream HighVolume = MarketFeed::TradeEvent
    .where(volume > 10000)
    .to(OutputTopic, topic: "high-volume-trades")
```

#### Kafka with SASL/SCRAM authentication

For production Kafka clusters with SCRAM auth, pass rdkafka properties:

```vpl
connector SecureBroker = kafka(
    brokers: "broker.example.com:9093",
    group_id: "varpulis-prod",
    security_protocol: "SASL_SSL",
    sasl_mechanism: "SCRAM-SHA-256",
    sasl_username: "varpulis",
    sasl_password: "secret"
)
```

All rdkafka configuration properties are supported via the connector declaration.

### Watch output events via WebSocket

```bash
# Install websocat: cargo install websocat
websocat "wss://demo.varpulis-cep.com/ws"
```

## Useful Commands

```bash
cd ~/varpulis-demo/repo/deploy/demo

# View logs
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f worker-0
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f coordinator-1

# Check status
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps

# Stop everything
docker compose -f docker-compose.yml -f docker-compose.prod.yml down

# View API key
grep VARPULIS_API_KEY .env
```

## Environment Variables

The `.env` file is auto-generated on first install. Key variables:

| Variable | Description |
|----------|-------------|
| `VARPULIS_API_KEY` | API key for coordinator (admin access) |
| `VARPULIS_WORKER_KEY` | API key for workers (same as coordinator by default) |
| `RUST_LOG` | Log level (default: `info`) |
| `GRAFANA_PASSWORD` | Grafana admin password |
| `JWT_SECRET` | JWT signing secret for OAuth |
| `POSTGRES_PASSWORD` | PostgreSQL password |

## Image Provenance

Docker images are built by CI on every push to `main` and pushed to GHCR:
- `ghcr.io/varpulis/varpulis:main` — backend (coordinator + worker binary)
- `ghcr.io/varpulis/varpulis-web-ui:main` — web UI (nginx + Vue SPA)

Each image is also tagged with `sha-<commit>` for auditability.
