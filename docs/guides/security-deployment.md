# Deploying Varpulis for Kill Chain Detection

## Architecture

```
Windows Hosts (Sysmon)
    |
    +-- Winlogbeat --> Syslog TCP (:514) --+
    +-- Splunk UF --> Splunk HEC (:8088) --+
    +-- Kafka --> Kafka Consumer ----------+
                                           |
                                   +-------v-------+
                                   |   Varpulis     |
                                   |   Server       |
                                   |   (:9000)      |
                                   +-------+-------+
                                           |
                       +-------------------+-------------------+
                       v                   v                   v
                  Syslog CEF          Slack Webhook        File/JSON
                  -> SIEM             -> SOC Channel       -> Forensics
```

## Quick Start

```bash
# Bootstrap a security deployment
varpulis security init --dir ./my-soc

cd my-soc
docker compose up -d

# Deploy detection rules
varpulis deploy-rules --dir rules/ --server http://localhost:9000 --api-key changeme

# Verify: inject test events and check for alerts
bash test-inject.sh
```

## Step-by-Step Setup

### 1. Install Varpulis

```bash
curl -sSf https://raw.githubusercontent.com/varpulis/varpulis/main/scripts/install.sh | sh
```

### 2. Configure Event Sources

#### Option A: Syslog TCP (Winlogbeat -> Varpulis)

Configure Winlogbeat on Windows hosts:

```yaml
# winlogbeat.yml
winlogbeat.event_logs:
  - name: Microsoft-Windows-Sysmon/Operational

output.syslog:
  hosts: ["varpulis-host:514"]
  protocol: tcp
```

#### Option B: Splunk HEC (Splunk UF -> Varpulis)

Configure Splunk Universal Forwarder:

```
# outputs.conf
[httpout]
httpEventCollectorToken = your-hec-token
uri = https://varpulis-host:8088/services/collector/event
```

#### Option C: Kafka

Configure Winlogbeat Kafka output:

```yaml
output.kafka:
  hosts: ["kafka:9092"]
  topic: sysmon-events
```

Then deploy VPL rules with Kafka source connectors.

### 3. Deploy Detection Rules

```bash
varpulis deploy-rules --dir rules/ \
  --server http://localhost:9000 \
  --api-key $VARPULIS_API_KEY
```

### 4. Configure Alert Routing

#### Syslog CEF -> SIEM

Add `.to(syslog(...))` to VPL rules for SIEM integration.

#### Slack Notifications

Add `.to(slack(...))` to VPL rules for team alerts.

### 5. Monitor

- Grafana dashboard: http://localhost:3000
- Prometheus metrics: http://localhost:9091
- Varpulis API: http://localhost:9000/api/v1/pipelines

### 6. Test with Synthetic Events

```bash
# Run detection against APT29 test data
varpulis detect \
  --rules rules/ \
  --events data/predictive_killchain.jsonl \
  -w 1
```

## What Makes Varpulis Different

| Capability | Sigma/SIEM | Varpulis |
|-----------|-----------|----------|
| Multi-step sequences (4+ steps) | Manual correlation | Native SASE+ |
| Predictive detection (.forecast()) | No | Yes -- alerts before attack completes |
| Kleene closures (N+ events) | Fixed thresholds only | Unbounded pattern matching |
| Behavioral detection | Filename/hash matching | OS-level behavior sequences |
| Throughput | ~10K evt/s | 250K+ evt/s single core |

## Production Hardening

- Enable TLS: `--tls-cert cert.pem --tls-key key.pem`
- API key auth: `--api-key $VARPULIS_API_KEY`
- Rate limiting: `--rate-limit 1000`
- State persistence: `--state-dir /data/state`
- Cluster mode: 3+ coordinators for HA

See [Production Deployment Guide](../PRODUCTION_DEPLOYMENT.md) for full details.
