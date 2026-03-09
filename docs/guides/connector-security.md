# Connector Security

## Overview

Production deployments require secure connections between Varpulis and external systems (Kafka, MQTT, NATS). Varpulis separates security credentials from pipeline logic:

- **VPL files contain no secrets.** Pipelines reference a named security profile; credentials are loaded from a separate credentials file at runtime.
- **AES-256-GCM encryption** for secrets at rest. Passwords and tokens in the credentials file can be encrypted with a master key.
- **File permission enforcement.** Varpulis refuses to start if certificate files or the credentials file have overly permissive permissions.

This design ensures VPL files can be safely committed to version control and shared across teams without exposing sensitive configuration.

---

## Credentials File

### Location

The credentials file is resolved in the following order:

1. `--credentials` CLI flag
2. `VARPULIS_CREDENTIALS` environment variable
3. `~/.varpulis/credentials.yaml` (default)

### File Permissions

The credentials file **must** have `0600` (owner read/write) or `0400` (owner read-only) permissions. Varpulis will refuse to load a credentials file that is group- or world-readable.

```bash
chmod 600 ~/.varpulis/credentials.yaml
```

### Format

```yaml
version: 1
require_encryption: false  # Set true in production

profiles:
  development:
    connector_type: kafka
    properties:
      security_protocol: PLAINTEXT

  production:
    connector_type: kafka
    properties:
      security_protocol: SASL_SSL
      sasl_mechanism: SCRAM-SHA-512
      sasl_username: varpulis-app
      sasl_password: "ENC[AES256-GCM,base64...]"
      ssl_ca_location: /etc/varpulis/certs/ca.pem
      ssl_certificate_location: /etc/varpulis/certs/client.pem
      ssl_key_location: /etc/varpulis/certs/client-key.pem

  mqtt-tls:
    connector_type: mqtt
    properties:
      username: sensor-gateway
      password: "ENC[AES256-GCM,base64...]"
```

Each profile has:

- **`connector_type`** -- The connector this profile applies to (`kafka`, `mqtt`, `nats`).
- **`properties`** -- Key-value pairs passed to the underlying client library (rdkafka, rumqttc, etc.).

When `require_encryption: true` is set, Varpulis rejects any `password` or token value that is not wrapped in `ENC[...]`. This prevents accidentally deploying with plaintext secrets in production.

---

## VPL Usage

Reference a credentials profile using the `profile` parameter in a connector declaration:

```vpl
connector Kafka = kafka (
    brokers: "kafka-1:9093,kafka-2:9093",
    topic: "events",
    profile: "production"
)
```

The `profile` value must match a profile name in the credentials file. At startup, Varpulis merges the profile properties with the connector declaration -- the profile supplies security parameters while the VPL supplies logical parameters (brokers, topics, group IDs).

If no `profile` is specified, the connector uses plaintext with no authentication (suitable for local development only).

---

## Master Key Setup

The master key is used to encrypt and decrypt `ENC[...]` values in the credentials file.

### Generate a Master Key

```bash
varpulis generate-master-key > /etc/varpulis/master.key
chmod 400 /etc/varpulis/master.key
```

### Provide the Master Key at Runtime

Either point to the key file:

```bash
export VARPULIS_MASTER_KEY_FILE=/etc/varpulis/master.key
```

Or provide the raw hex-encoded key directly (useful in container environments):

```bash
export VARPULIS_MASTER_KEY=a3f8...c7d2
```

### Encrypt Credentials

To encrypt plaintext passwords in an existing credentials file in-place:

```bash
varpulis encrypt-credentials --input credentials.yaml
```

This replaces each plaintext `password`, `sasl_password`, and `token` value with its `ENC[AES256-GCM,base64...]` equivalent. The original file is backed up as `credentials.yaml.bak`.

---

## Kafka SCRAM-SHA-512 + SSL Example

This walkthrough sets up Kafka with SCRAM-SHA-512 authentication over TLS.

### Step 1: Generate Certificates

```bash
# Create a CA
openssl req -new -x509 -keyout ca-key.pem -out ca.pem -days 365 \
  -subj "/CN=Varpulis-CA" -nodes

# Generate server key and CSR
openssl req -new -keyout server-key.pem -out server.csr \
  -subj "/CN=kafka-broker" -nodes
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out server.pem -days 365

# Generate client key and CSR
openssl req -new -keyout client-key.pem -out client.csr \
  -subj "/CN=varpulis-client" -nodes
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out client.pem -days 365

# Set permissions
chmod 644 ca.pem
chmod 600 server.pem server-key.pem client.pem client-key.pem
```

### Step 2: Configure Kafka Broker

Add to `server.properties`:

```properties
listeners=SASL_SSL://0.0.0.0:9093
advertised.listeners=SASL_SSL://kafka-broker:9093
security.inter.broker.protocol=SASL_SSL

ssl.keystore.location=/etc/kafka/certs/server.keystore.jks
ssl.keystore.password=changeit
ssl.truststore.location=/etc/kafka/certs/server.truststore.jks
ssl.truststore.password=changeit
ssl.client.auth=required

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
```

### Step 3: Create SCRAM User

```bash
kafka-configs.sh --bootstrap-server localhost:9093 \
  --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=s3cureP@ss]' \
  --entity-type users --entity-name varpulis-app
```

### Step 4: Create Credentials File

```yaml
version: 1
require_encryption: true

profiles:
  production:
    connector_type: kafka
    properties:
      security_protocol: SASL_SSL
      sasl_mechanism: SCRAM-SHA-512
      sasl_username: varpulis-app
      sasl_password: "s3cureP@ss"  # will be encrypted below
      ssl_ca_location: /etc/varpulis/certs/ca.pem
      ssl_certificate_location: /etc/varpulis/certs/client.pem
      ssl_key_location: /etc/varpulis/certs/client-key.pem
```

Then encrypt:

```bash
varpulis generate-master-key > /etc/varpulis/master.key
chmod 400 /etc/varpulis/master.key
export VARPULIS_MASTER_KEY_FILE=/etc/varpulis/master.key

varpulis encrypt-credentials --input credentials.yaml
chmod 600 credentials.yaml
```

### Step 5: Write the VPL Pipeline

```vpl
connector Kafka = kafka (
    brokers: "kafka-1:9093,kafka-2:9093",
    group_id: "varpulis-prod",
    topic: "sensor-events",
    profile: "production"
)

event SensorReading:
    sensor_id: str
    temperature: float
    humidity: float

stream Sensors = SensorReading
    .from(Kafka, topic: "sensor-events")

stream HighTemp = SensorReading
    .where(temperature > 50.0)
    .emit(alert: "HIGH_TEMP", sensor_id: sensor_id, temperature: temperature)
    .to(Kafka)
```

### Step 6: Run the Pipeline

```bash
varpulis run -f pipeline.vpl --credentials credentials.yaml
```

Or with the environment variable:

```bash
export VARPULIS_CREDENTIALS=/etc/varpulis/credentials.yaml
varpulis run -f pipeline.vpl
```

---

## Kafka mTLS Example

Mutual TLS (mTLS) uses client certificates for authentication instead of username/password. No SASL mechanism is needed.

### Credentials File

```yaml
version: 1

profiles:
  kafka-mtls:
    connector_type: kafka
    properties:
      security_protocol: SSL
      ssl_ca_location: /etc/varpulis/certs/ca.pem
      ssl_certificate_location: /etc/varpulis/certs/client.pem
      ssl_key_location: /etc/varpulis/certs/client-key.pem
```

### VPL

```vpl
connector Kafka = kafka (
    brokers: "kafka-1:9093,kafka-2:9093",
    group_id: "varpulis-mtls",
    profile: "kafka-mtls"
)

stream Events = MyEvent
    .from(Kafka, topic: "events")
```

With mTLS, the Kafka broker authenticates the client by verifying the client certificate against its truststore. No passwords are transmitted.

---

## File Permission Requirements

Varpulis enforces strict file permissions on security-sensitive files. The process will exit with an error if permissions are too open.

| File | Required Permissions | Description |
|------|---------------------|-------------|
| `credentials.yaml` | `0600` or `0400` | Credentials file |
| `ca.pem` | `0644` (public) | CA certificate |
| `client.pem` | `0600` or `0400` | Client certificate |
| `client-key.pem` | `0600` or `0400` | Client private key |
| `master.key` | `0400` | Master encryption key |

---

## Supported Authentication Methods

| Method | `security_protocol` | `sasl_mechanism` | Use Case |
|--------|---------------------|------------------|----------|
| Plaintext | `PLAINTEXT` | -- | Development only |
| TLS encryption | `SSL` | -- | Encrypt traffic, no client auth |
| mTLS | `SSL` | -- | Certificate-based client auth |
| SASL/PLAIN + TLS | `SASL_SSL` | `PLAIN` | Username/password (simple) |
| SASL/SCRAM + TLS | `SASL_SSL` | `SCRAM-SHA-256` or `SCRAM-SHA-512` | Username/password (challenge-response) |
| SASL/OAUTHBEARER | `SASL_SSL` | `OAUTHBEARER` | Token-based (OAuth 2.0) |

> **Never use SASL/PLAIN or SASL/SCRAM without TLS.** The `SASL_PLAINTEXT` protocol transmits credentials in cleartext and should not be used outside of isolated test networks.

---

## Security Best Practices

- **Never commit credentials files to version control.** Add `credentials.yaml` and `*.key` to `.gitignore`.
- **Use `require_encryption: true` in production.** This ensures no plaintext secrets can accidentally slip into the credentials file.
- **Rotate master keys periodically.** Generate a new master key, re-encrypt credentials, and deploy the new key file.
- **Use short-lived certificates.** Automate certificate renewal with a tool like cert-manager or Vault PKI.
- **Monitor certificate expiry.** Set up alerts for certificates expiring within 30 days.
- **Use separate profiles per environment.** Keep `development`, `staging`, and `production` profiles distinct to avoid cross-environment credential leaks.
- **Restrict network access.** Use firewall rules or security groups to limit which hosts can connect to your Kafka/MQTT brokers.
- **Audit credential access.** Log when the credentials file is read and which profiles are loaded.

---

## See Also

- [Connectors Reference](../language/connectors.md) -- Connector declaration syntax and parameters
- [Configuration Guide](configuration.md) -- CLI and server configuration
- [Performance Tuning](performance-tuning.md) -- Kafka batching and throughput tuning
