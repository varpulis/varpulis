# Encryption at Rest Tutorial

This tutorial teaches you to enable encryption at rest for Varpulis state persistence. You'll learn to generate encryption keys, enable the feature, verify encryption, use passphrase-based key derivation, and perform key rotation.

## What You'll Learn

- Why encrypt state at rest
- Generating and configuring an AES-256-GCM encryption key
- Enabling encryption via feature flag and environment variable
- Passphrase-based key derivation with Argon2id
- Verifying that state data is encrypted
- Key rotation procedure

## Prerequisites

- Varpulis built with the `encryption` feature (`cargo build --release --features encryption`)
- State persistence enabled (`--state-dir` or `--features persistence`)
- Basic VPL knowledge (see [Getting Started](getting-started.md))
- Familiarity with checkpointing (see [Checkpointing Tutorial](checkpointing-tutorial.md))

---

## Part 1: Why Encrypt State

Varpulis checkpoints engine state (windows, variables, pattern buffers) to disk or RocksDB for crash recovery. This state may contain sensitive data from your event streams:

- Financial transaction amounts
- User identifiers and personal data
- API keys or tokens seen in events
- Healthcare or compliance-regulated data

Encryption at rest ensures that checkpoint files are unreadable without the encryption key, even if an attacker gains access to the storage volume.

Varpulis uses **AES-256-GCM** (Galois/Counter Mode), which provides both confidentiality and integrity:
- **AES-256**: Industry-standard symmetric encryption
- **GCM**: Authenticated encryption — tampered data is detected and rejected

---

## Part 2: Generating an Encryption Key

The encryption key must be exactly 32 bytes (256 bits), provided as a 64-character hex string.

### Generate with OpenSSL

```bash
openssl rand -hex 32
# Output: e.g. a1b2c3d4e5f6...  (64 hex characters)
```

### Generate with Python

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### Store the Key Securely

```bash
# Write to a file with restricted permissions
openssl rand -hex 32 > /etc/varpulis/encryption.key
chmod 600 /etc/varpulis/encryption.key
```

Never commit the key to version control. Use a secrets manager (Vault, AWS Secrets Manager, Kubernetes Secrets) in production.

---

## Part 3: Enabling Encryption

### Build with the Feature Flag

```bash
cargo build --release --features encryption
# Or combine with persistence:
cargo build --release --features encryption,persistence
```

### Configure the Key

Set the key via environment variable:

```bash
export VARPULIS_ENCRYPTION_KEY="a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
```

Or load from a file:

```bash
export VARPULIS_ENCRYPTION_KEY="$(cat /etc/varpulis/encryption.key)"
```

### Start with Encrypted State

```bash
varpulis server \
    --port 9000 \
    --state-dir /var/lib/varpulis/state \
    --api-key "$(cat /run/secrets/api-key)"
```

When `VARPULIS_ENCRYPTION_KEY` is set and the `encryption` feature is enabled, all checkpoint data is automatically encrypted before writing to disk.

---

## Part 4: Passphrase-Based Key Derivation

Instead of managing a raw hex key, you can derive the encryption key from a human-readable passphrase using Argon2id:

```bash
export VARPULIS_ENCRYPTION_PASSPHRASE="my-secure-passphrase-here"
```

When `VARPULIS_ENCRYPTION_PASSPHRASE` is set (and `VARPULIS_ENCRYPTION_KEY` is not), Varpulis automatically derives a 256-bit key using Argon2id with:

- **Memory**: 64 MB
- **Iterations**: 3
- **Parallelism**: 4 threads
- **Salt**: Derived from the passphrase (deterministic for the same passphrase)

Argon2id is the recommended password hashing algorithm per OWASP guidelines. It provides resistance against both GPU and side-channel attacks.

**Trade-off:** Passphrase derivation adds ~200ms startup time due to the Argon2id computation. For latency-sensitive deployments, pre-derive the key and use `VARPULIS_ENCRYPTION_KEY` directly.

---

## Part 5: Verifying Encryption

### Check that State Files Are Encrypted

After running Varpulis with encryption enabled:

```bash
# Try to read a checkpoint file directly — should be binary gibberish
hexdump -C /var/lib/varpulis/state/checkpoint_latest | head -5
# Should show random-looking bytes, NOT readable JSON or MessagePack
```

### Verify via Logs

Varpulis logs encryption status at startup:

```
INFO varpulis_runtime::persistence: Encryption at rest enabled (AES-256-GCM)
INFO varpulis_runtime::persistence: State directory: /var/lib/varpulis/state
```

### Test Tamper Detection

If a checkpoint file is modified, decryption will fail with an authentication error:

```
ERROR varpulis_runtime::persistence: Checkpoint decryption failed: aead::Error
```

GCM mode detects any modification to the ciphertext, preventing silent data corruption.

---

## Part 6: Key Rotation

To rotate the encryption key:

1. **Stop Varpulis** gracefully (triggers a final checkpoint with the current key)

2. **Decrypt existing checkpoints** with the old key:
   ```bash
   # Keep the old key temporarily
   export VARPULIS_ENCRYPTION_KEY="<old-key>"
   varpulis server --state-dir /var/lib/varpulis/state --port 9000
   # Let it start and load the checkpoint, then stop it
   ```

3. **Re-encrypt with the new key**:
   ```bash
   # Set the new key
   export VARPULIS_ENCRYPTION_KEY="<new-key>"
   varpulis server --state-dir /var/lib/varpulis/state --port 9000
   ```

   On startup, Varpulis will:
   - Load the existing checkpoint (still in memory, unencrypted)
   - Write new checkpoints encrypted with the new key

4. **Verify** the new key works by restarting once more.

5. **Destroy the old key** once you've confirmed the rotation succeeded.

### Zero-Downtime Rotation (Cluster Mode)

In a cluster deployment, rotate one node at a time:

1. Drain the node (stop sending it events)
2. Rotate its key
3. Rejoin the cluster
4. Repeat for the next node

---

## Docker Configuration

```yaml
services:
  varpulis:
    image: varpulis:latest
    environment:
      VARPULIS_ENCRYPTION_KEY: "${ENCRYPTION_KEY}"
      VARPULIS_API_KEY: "${API_KEY}"
    volumes:
      - varpulis-state:/var/lib/varpulis/state
    command: >
      varpulis server
        --port 9000
        --state-dir /var/lib/varpulis/state

volumes:
  varpulis-state:
```

### Kubernetes Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: varpulis-encryption
type: Opaque
data:
  key: <base64-encoded-hex-key>
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: varpulis
          env:
            - name: VARPULIS_ENCRYPTION_KEY
              valueFrom:
                secretKeyRef:
                  name: varpulis-encryption
                  key: key
```

---

## Quick Reference

| Setting | Value |
|---------|-------|
| Feature flag | `--features encryption` |
| Algorithm | AES-256-GCM |
| Key format | 64-character hex string (32 bytes) |
| Key env var | `VARPULIS_ENCRYPTION_KEY` |
| Passphrase env var | `VARPULIS_ENCRYPTION_PASSPHRASE` |
| KDF algorithm | Argon2id (64 MB, 3 iterations) |
| Integrity | GCM authentication tag (tamper detection) |

---

## Next Steps

- [Checkpointing Tutorial](checkpointing-tutorial.md) -- State persistence fundamentals
- [Security Policy](../../SECURITY.md) -- Security measures and reporting
- [Configuration Guide](../guides/configuration.md) -- All configuration options
- [Production Deployment](../PRODUCTION_DEPLOYMENT.md) -- Production best practices
