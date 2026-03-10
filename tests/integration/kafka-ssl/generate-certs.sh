#!/bin/bash
# Generate self-signed CA, broker, and client certificates for Kafka SASL_SSL testing.
# Output:
#   certs/   — PEM files (used by Varpulis/rdkafka client)
#   secrets/ — PKCS12 keystores (used by Kafka broker) + admin.properties
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="$BASE_DIR/certs"
SECRETS_DIR="$BASE_DIR/secrets"

rm -rf "$CERTS_DIR" "$SECRETS_DIR"
mkdir -p "$CERTS_DIR" "$SECRETS_DIR"

PASSWORD="test-password"
VALIDITY=365

echo "=== Generating CA ==="
openssl req -new -x509 -keyout "$CERTS_DIR/ca-key.pem" -out "$CERTS_DIR/ca-cert.pem" \
    -days $VALIDITY -subj "/CN=VarpulisTestCA/O=Varpulis/C=LV" \
    -passout "pass:$PASSWORD" 2>/dev/null

echo "=== Generating Broker key + cert ==="
openssl req -new -nodes -keyout "$CERTS_DIR/broker-key.pem" -out "$CERTS_DIR/broker.csr" \
    -subj "/CN=kafka/O=Varpulis/C=LV" 2>/dev/null

# Sign with SAN for hostname resolution inside Docker
openssl x509 -req -in "$CERTS_DIR/broker.csr" -CA "$CERTS_DIR/ca-cert.pem" \
    -CAkey "$CERTS_DIR/ca-key.pem" -CAcreateserial -out "$CERTS_DIR/broker-cert.pem" \
    -days $VALIDITY -passin "pass:$PASSWORD" \
    -extfile <(printf "subjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1") 2>/dev/null

echo "=== Generating Client key + cert ==="
openssl req -new -nodes -keyout "$CERTS_DIR/client-key.pem" -out "$CERTS_DIR/client.csr" \
    -subj "/CN=varpulis-client/O=Varpulis/C=LV" 2>/dev/null

openssl x509 -req -in "$CERTS_DIR/client.csr" -CA "$CERTS_DIR/ca-cert.pem" \
    -CAkey "$CERTS_DIR/ca-key.pem" -CAcreateserial -out "$CERTS_DIR/client-cert.pem" \
    -days $VALIDITY -passin "pass:$PASSWORD" 2>/dev/null

# Clean up CSR files
rm -f "$CERTS_DIR"/*.csr "$CERTS_DIR"/*.srl

echo "=== Creating PKCS12 keystores for Kafka broker ==="
# Broker keystore (cert + key)
openssl pkcs12 -export -in "$CERTS_DIR/broker-cert.pem" -inkey "$CERTS_DIR/broker-key.pem" \
    -CAfile "$CERTS_DIR/ca-cert.pem" -chain \
    -out "$SECRETS_DIR/broker-keystore.p12" -passout "pass:$PASSWORD" -name broker 2>/dev/null

# Truststore (CA cert)
openssl pkcs12 -export -nokeys -in "$CERTS_DIR/ca-cert.pem" \
    -out "$SECRETS_DIR/truststore.p12" -passout "pass:$PASSWORD" -name ca 2>/dev/null

echo "=== Creating password files for Confluent Docker ==="
echo -n "$PASSWORD" > "$SECRETS_DIR/keystore-creds"
echo -n "$PASSWORD" > "$SECRETS_DIR/truststore-creds"

echo "=== Creating admin client properties ==="
# For SASL_SSL connections (external listener)
cat > "$SECRETS_DIR/admin.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=test-password
ssl.truststore.type=PKCS12
ssl.endpoint.identification.algorithm=
EOF

# For SASL_PLAINTEXT connections (internal listener)
cat > "$SECRETS_DIR/admin-internal.properties" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
EOF

# Set permissions
chmod 600 "$CERTS_DIR"/*-key.pem
chmod 644 "$CERTS_DIR"/*-cert.pem "$CERTS_DIR"/ca-cert.pem

echo "=== Done ==="
echo "PEM certs:  $CERTS_DIR/"
ls -la "$CERTS_DIR"
echo ""
echo "Keystores:  $SECRETS_DIR/"
ls -la "$SECRETS_DIR"
