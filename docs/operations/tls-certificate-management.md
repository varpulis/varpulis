# TLS Certificate Management

## Overview

Varpulis supports TLS encryption for all external-facing connections, including the coordinator API, worker endpoints, and connector integrations. TLS is **opt-in** and not enabled by default. When no TLS flags are provided, Varpulis listens on plaintext HTTP.

Varpulis uses `rustls` for TLS termination, which means there is no dependency on OpenSSL at runtime. Certificates must be PEM-encoded.

**Important:** TLS configuration is loaded once at process startup. Runtime certificate rotation is not supported. To apply new certificates, you must restart the Varpulis process. See [Rolling Rotation Procedure](#rolling-rotation-procedure) for how to do this without downtime in a clustered deployment.

The CLI exposes five TLS-related flags:

| Flag | Purpose |
|------|---------|
| `--tls-cert` | Path to the server certificate (PEM) |
| `--tls-key` | Path to the server private key (PEM) |
| `--tls-ca-cert` | Path to a CA certificate for verifying client certificates (enables mTLS) |
| `--tls-client-cert` | Client certificate for outbound connections to TLS-enabled connectors |
| `--tls-client-key` | Client private key for outbound connections |

NATS, used for inter-node cluster communication, has its own independent TLS configuration managed through the NATS server config file. This guide covers only the Varpulis process TLS settings.

## Generating Certificates

All examples below use `openssl` to generate PEM-encoded certificates suitable for Varpulis.

### Self-Signed Certificate Authority

Create a CA that will sign both server and client certificates:

```bash
# Generate CA private key
openssl genrsa -out ca-key.pem 4096

# Generate CA certificate (valid for 10 years)
openssl req -new -x509 -key ca-key.pem -sha256 \
  -subj "/CN=Varpulis Internal CA" \
  -days 3650 \
  -out ca-cert.pem
```

### Server Certificate

Generate a server certificate signed by the CA. Adjust the `subjectAltName` to match the hostnames and IPs your nodes use:

```bash
# Generate server private key
openssl genrsa -out server-key.pem 2048

# Create a certificate signing request
openssl req -new -key server-key.pem \
  -subj "/CN=varpulis-server" \
  -out server.csr

# Sign the certificate with the CA (valid for 1 year)
openssl x509 -req -in server.csr \
  -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -days 365 -sha256 \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:varpulis.example.com,IP:127.0.0.1") \
  -out server-cert.pem

# Clean up the CSR
rm server.csr
```

### Client Certificate for mTLS

Generate a client certificate for services that need to authenticate to Varpulis:

```bash
# Generate client private key
openssl genrsa -out client-key.pem 2048

# Create a certificate signing request
openssl req -new -key client-key.pem \
  -subj "/CN=varpulis-client" \
  -out client.csr

# Sign the certificate with the CA (valid for 1 year)
openssl x509 -req -in client.csr \
  -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -days 365 -sha256 \
  -out client-cert.pem

# Clean up the CSR
rm client.csr
```

## Configuring TLS

### Server-Only TLS

The minimum configuration for enabling TLS requires a server certificate and its private key:

```bash
varpulis coordinator \
  --tls-cert /etc/varpulis/tls/server-cert.pem \
  --tls-key /etc/varpulis/tls/server-key.pem
```

```bash
varpulis worker \
  --tls-cert /etc/varpulis/tls/server-cert.pem \
  --tls-key /etc/varpulis/tls/server-key.pem
```

This encrypts traffic in transit but does not verify client identity.

### Mutual TLS (mTLS)

To require clients to present a valid certificate, add the `--tls-ca-cert` flag pointing to the CA that signed the client certificates:

```bash
varpulis coordinator \
  --tls-cert /etc/varpulis/tls/server-cert.pem \
  --tls-key /etc/varpulis/tls/server-key.pem \
  --tls-ca-cert /etc/varpulis/tls/ca-cert.pem
```

Clients must then present a certificate signed by that CA when connecting.

### Client Certificates for Connectors

When Varpulis connects to external systems (Kafka brokers, MQTT brokers) that require client certificate authentication, use the client certificate flags:

```bash
varpulis worker \
  --tls-cert /etc/varpulis/tls/server-cert.pem \
  --tls-key /etc/varpulis/tls/server-key.pem \
  --tls-client-cert /etc/varpulis/tls/client-cert.pem \
  --tls-client-key /etc/varpulis/tls/client-key.pem
```

These client credentials are used for outbound connections from Varpulis to TLS-enabled connectors, independently of the server TLS settings.

## Rolling Rotation Procedure

Because Varpulis loads TLS certificates at startup and does not support runtime rotation, certificate changes require a process restart. In a clustered deployment, you can perform rolling restarts without downtime because Varpulis automatically migrates pipelines away from a worker that is shutting down.

### Step 1: Generate New Certificates

Generate new certificates using the same CA, or a new CA if you are rotating the CA itself:

```bash
# If rotating the CA, generate a new CA first (see above),
# then generate new server/client certs signed by the new CA.

# If keeping the same CA, just generate new server/client certs.
openssl genrsa -out server-key-new.pem 2048
openssl req -new -key server-key-new.pem \
  -subj "/CN=varpulis-server" \
  -out server-new.csr
openssl x509 -req -in server-new.csr \
  -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -days 365 -sha256 \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:varpulis.example.com,IP:127.0.0.1") \
  -out server-cert-new.pem
rm server-new.csr
```

### Step 2: Distribute New Certificates

Copy the new certificate files to all nodes, placing them alongside (not replacing) the existing files:

```bash
# Example using scp
for host in worker-1 worker-2 worker-3 coordinator; do
  scp server-cert-new.pem server-key-new.pem "${host}:/etc/varpulis/tls/"
done
```

### Step 3: Restart Workers One at a Time

Restart each worker individually, waiting for pipeline migration to complete before proceeding to the next:

```bash
# On worker-1: replace cert files and restart
mv /etc/varpulis/tls/server-cert-new.pem /etc/varpulis/tls/server-cert.pem
mv /etc/varpulis/tls/server-key-new.pem /etc/varpulis/tls/server-key.pem
systemctl restart varpulis-worker

# Wait for the worker to rejoin the cluster and receive migrated pipelines
# before proceeding to the next worker.
```

The cluster maintains availability throughout this process. When a worker shuts down, its pipelines are automatically migrated to the remaining healthy workers. When the worker comes back up with the new certificates, it rejoins the cluster and can receive pipeline assignments again.

Repeat for each worker node.

### Step 4: Restart the Coordinator

After all workers are running with new certificates, restart the coordinator:

```bash
mv /etc/varpulis/tls/server-cert-new.pem /etc/varpulis/tls/server-cert.pem
mv /etc/varpulis/tls/server-key-new.pem /etc/varpulis/tls/server-key.pem
systemctl restart varpulis-coordinator
```

### Step 5: Verify

Confirm that TLS is working with the new certificates:

```bash
# Check the certificate served by the coordinator
openssl s_client -connect coordinator:9100 -CAfile /etc/varpulis/tls/ca-cert.pem </dev/null 2>/dev/null \
  | openssl x509 -noout -dates -subject

# Or using curl
curl --cacert /etc/varpulis/tls/ca-cert.pem https://coordinator:9100/api/v1/cluster/status
```

## Kubernetes cert-manager Integration

In Kubernetes deployments, [cert-manager](https://cert-manager.io/) can automate certificate issuance and renewal. Below is an example configuration.

### Certificate Custom Resource

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: varpulis-tls
  namespace: varpulis
spec:
  secretName: varpulis-tls-secret
  duration: 720h        # 30 days
  renewBefore: 360h     # renew 15 days before expiry
  issuerRef:
    name: varpulis-ca-issuer
    kind: ClusterIssuer
  commonName: varpulis-server
  dnsNames:
    - varpulis-coordinator
    - varpulis-coordinator.varpulis.svc.cluster.local
    - "*.varpulis-workers.varpulis.svc.cluster.local"
  privateKey:
    algorithm: ECDSA
    size: 256
```

### Mounting Certificates into Varpulis Pods

Reference the cert-manager Secret as a volume in your Deployment or StatefulSet:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: varpulis-coordinator
  namespace: varpulis
spec:
  template:
    spec:
      containers:
        - name: varpulis
          args:
            - coordinator
            - --tls-cert=/tls/tls.crt
            - --tls-key=/tls/tls.key
            - --tls-ca-cert=/tls/ca.crt
          volumeMounts:
            - name: tls-certs
              mountPath: /tls
              readOnly: true
      volumes:
        - name: tls-certs
          secret:
            secretName: varpulis-tls-secret
```

Since Varpulis does not support runtime certificate rotation, you need to restart pods when cert-manager renews the Secret. Use a tool like [Reloader](https://github.com/stakater/Reloader) to trigger rolling restarts automatically:

```yaml
metadata:
  annotations:
    reloader.stakater.com/auto: "true"
```

## Monitoring Certificate Expiry

### Check Certificate Expiry from the Command Line

```bash
openssl x509 -enddate -noout -in /etc/varpulis/tls/server-cert.pem
```

Example output:

```
notAfter=Mar 25 12:00:00 2027 GMT
```

To check a running server's certificate remotely:

```bash
openssl s_client -connect coordinator:9100 </dev/null 2>/dev/null \
  | openssl x509 -enddate -noout
```

### Prometheus Alerting

Use the `x509_cert_not_after` metric exposed by [node_exporter](https://github.com/prometheus/node_exporter) (with the `textfile` collector) or [blackbox_exporter](https://github.com/prometheus/blackbox_exporter) (with the `tcp` probe module configured for TLS) to monitor certificate expiry.

Example Prometheus alerting rule:

```yaml
groups:
  - name: tls-expiry
    rules:
      - alert: VarpulisCertExpiringSoon
        expr: (x509_cert_not_after - time()) / 86400 < 30
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Varpulis TLS certificate expires in {{ $value | humanizeDuration }}"
          description: "Certificate on {{ $labels.instance }} expires in less than 30 days. Rotate certificates and perform a rolling restart."
```

### Recommended Schedule

Rotate certificates at least **30 days before expiry**. This provides a buffer for operational delays and avoids service disruption from expired certificates.

## Troubleshooting

### "certificate verify failed"

The client does not trust the server's certificate, or the server does not trust the client's certificate during mTLS.

- Verify the CA certificate used by the verifying side matches the CA that signed the presented certificate:
  ```bash
  openssl verify -CAfile ca-cert.pem server-cert.pem
  ```
- Ensure the `--tls-ca-cert` flag points to the correct CA file.
- If using a certificate chain, make sure the full chain is included in the certificate PEM file (leaf certificate first, followed by intermediates).

### "certificate has expired"

The certificate's validity period has passed.

- Check the expiry date:
  ```bash
  openssl x509 -enddate -noout -in /etc/varpulis/tls/server-cert.pem
  ```
- Generate a new certificate, distribute it, and perform a rolling restart as described in the [Rolling Rotation Procedure](#rolling-rotation-procedure).

### "no client certificate"

mTLS is configured on the server (via `--tls-ca-cert`) but the connecting client did not present a certificate.

- Ensure the client is configured with `--tls-client-cert` and `--tls-client-key`.
- Verify the client certificate is signed by the CA specified in the server's `--tls-ca-cert`.
- Test the client certificate manually:
  ```bash
  curl --cert client-cert.pem --key client-key.pem \
    --cacert ca-cert.pem \
    https://coordinator:9100/api/v1/cluster/status
  ```
