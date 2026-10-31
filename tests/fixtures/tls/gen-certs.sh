#!/usr/bin/env bash
# Regenerate the throwaway TLS fixtures used by tests/test_tls.py.
#
# TEST-ONLY: the private keys in this directory are committed on purpose so the suite
# can run without openssl on the machine. Never reuse them for anything real.
#
#   ./gen-certs.sh            # regenerate all fixtures (needs openssl)
#   DAYS=3650 ./gen-certs.sh  # override validity
set -euo pipefail
cd "$(dirname "$0")"
DAYS="${DAYS:-36500}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/ca.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = dn
x509_extensions = v3_ca
[dn]
CN = hermes-mqtt test CA
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
EOF
openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" -sha256 \
  -keyout "$tmp/ca-key.pem" -out ca.pem -config "$tmp/ca.cnf"

openssl req -newkey rsa:2048 -nodes -sha256 -keyout server-key.pem -out "$tmp/server.csr" \
  -subj "/CN=localhost"
cat > "$tmp/server.ext" <<'EOF'
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
subjectAltName = @alt
[alt]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
openssl x509 -req -in "$tmp/server.csr" -CA ca.pem -CAkey "$tmp/ca-key.pem" \
  -CAcreateserial -days "$DAYS" -sha256 -out server.pem -extfile "$tmp/server.ext"

openssl req -newkey rsa:2048 -nodes -sha256 -keyout client-key.pem -out "$tmp/client.csr" \
  -subj "/CN=hermes-mqtt test client"
cat > "$tmp/client.ext" <<'EOF'
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
EOF
openssl x509 -req -in "$tmp/client.csr" -CA ca.pem -CAkey "$tmp/ca-key.pem" \
  -CAcreateserial -days "$DAYS" -sha256 -out client.pem -extfile "$tmp/client.ext"

# AES-encrypted copy of the client key (password: test-key-password) for the
# encrypted-key password handling tests.
openssl pkey -in client-key.pem -aes-256-cbc -passout pass:test-key-password \
  -out client-key-encrypted.pem

rm -f ca.srl
chmod 600 server-key.pem client-key.pem client-key-encrypted.pem
echo "regenerated TLS fixtures in $(pwd)"
