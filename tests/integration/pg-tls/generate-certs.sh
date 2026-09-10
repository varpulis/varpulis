#!/usr/bin/env bash
# Generate the certificate material for the TLS PostgreSQL fixture
# (docker-compose.pg-tls.yml).
#
# Produces a private CA and a server certificate signed by it:
#
#   ca.crt / ca.key          self-signed CA, in NO system trust store
#   server.crt / server.key  server cert, CN/SAN localhost, EKU serverAuth
#
# That shape is what makes all three CDC TLS gates meaningful:
#
#   * sslmode=require            must succeed  -> real TLS was negotiated
#     (the fixture's pg_hba.conf refuses plaintext, so it cannot succeed
#      any other way)
#   * sslmode=verify-full        must FAIL with system roots -> the WebPki
#     with no CA                 verifier is genuinely consulted
#   * sslmode=verify-full        must succeed -> the pinned CA is actually
#     with ca_cert=ca.crt        used and the SAN matches the host
#
# A single self-signed *server* cert cannot play both roles: rustls/webpki
# will not accept a leaf certificate as its own trust anchor, so the pinned-CA
# case fails the handshake. Hence the two-cert chain.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "$DIR/ca.crt" && -f "$DIR/server.crt" && -f "$DIR/server.key" && "${FORCE:-0}" != "1" ]]; then
    echo "certs already present in $DIR (FORCE=1 to regenerate)"
    exit 0
fi

rm -f "$DIR"/ca.crt "$DIR"/ca.key "$DIR"/server.crt "$DIR"/server.key "$DIR"/server.csr

# --- private CA ------------------------------------------------------------
openssl req -new -x509 -days 3650 -nodes \
    -newkey rsa:2048 \
    -keyout "$DIR/ca.key" \
    -out "$DIR/ca.crt" \
    -subj "/CN=Varpulis Test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

# --- server cert signed by that CA -----------------------------------------
openssl req -new -nodes \
    -newkey rsa:2048 \
    -keyout "$DIR/server.key" \
    -out "$DIR/server.csr" \
    -subj "/CN=localhost" 2>/dev/null

openssl x509 -req -days 3650 \
    -in "$DIR/server.csr" \
    -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" -CAcreateserial \
    -out "$DIR/server.crt" \
    -extfile <(printf '%s\n' \
        "basicConstraints=critical,CA:FALSE" \
        "keyUsage=critical,digitalSignature,keyEncipherment" \
        "extendedKeyUsage=serverAuth" \
        "subjectAltName=DNS:localhost,DNS:postgres-tls,IP:127.0.0.1") 2>/dev/null

rm -f "$DIR/server.csr" "$DIR/ca.srl"

# postgres refuses to start if the key is group/world readable.
chmod 600 "$DIR/server.key" "$DIR/ca.key"
chmod 644 "$DIR/server.crt" "$DIR/ca.crt"
echo "wrote $DIR/{ca.crt,ca.key,server.crt,server.key}"
