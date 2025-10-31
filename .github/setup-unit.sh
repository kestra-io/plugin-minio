#!/usr/bin/env bash
set -euo pipefail

MTLS_DIR="src/test/resources/mtls"
STOREPASS="keystorePassword"
TRUSTPASS="changeit"

mkdir -p "${MTLS_DIR}"
rm -f "${MTLS_DIR}"/* || true

echo "Generating CA certificate..."
openssl req -x509 -newkey rsa:4096 -days 1825 -nodes \
  -keyout "${MTLS_DIR}/ca-key.pem" \
  -out "${MTLS_DIR}/ca-cert.pem" \
  -subj "/CN=KestraTestCA"

echo "Generating server certificate..."
openssl genrsa -out "${MTLS_DIR}/server-key.pem" 4096

cat > "${MTLS_DIR}/server-ext.cnf" <<EOF
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF

openssl req -new -key "${MTLS_DIR}/server-key.pem" \
  -out "${MTLS_DIR}/server.csr" \
  -subj "/CN=localhost"

openssl x509 -req -in "${MTLS_DIR}/server.csr" \
  -CA "${MTLS_DIR}/ca-cert.pem" -CAkey "${MTLS_DIR}/ca-key.pem" -CAcreateserial \
  -out "${MTLS_DIR}/server-cert.pem" -days 825 -sha256 \
  -extfile "${MTLS_DIR}/server-ext.cnf"

cat "${MTLS_DIR}/server-cert.pem" "${MTLS_DIR}/ca-cert.pem" > "${MTLS_DIR}/server-chain.pem"

echo "Generating client certificate..."
openssl genrsa -out "${MTLS_DIR}/client-key.pem" 4096
openssl req -new -key "${MTLS_DIR}/client-key.pem" \
  -out "${MTLS_DIR}/client.csr" -subj "/CN=KestraTestClient"
openssl x509 -req -in "${MTLS_DIR}/client.csr" \
  -CA "${MTLS_DIR}/ca-cert.pem" -CAkey "${MTLS_DIR}/ca-key.pem" -CAcreateserial \
  -out "${MTLS_DIR}/client-cert.pem" -days 825 -sha256
cat "${MTLS_DIR}/client-cert.pem" "${MTLS_DIR}/client-key.pem" > "${MTLS_DIR}/client-cert-key.pem"

openssl pkcs12 -export \
  -inkey "${MTLS_DIR}/server-key.pem" \
  -in "${MTLS_DIR}/server-chain.pem" \
  -out "${MTLS_DIR}/server-keystore.p12" \
  -name "wiremock-server" \
  -password pass:${STOREPASS}

keytool -importcert -noprompt \
  -alias kestra-ca \
  -file "${MTLS_DIR}/ca-cert.pem" \
  -keystore "${MTLS_DIR}/client-truststore.p12" \
  -storetype PKCS12 \
  -storepass "${TRUSTPASS}"

rm -f "${MTLS_DIR}/server.csr" "${MTLS_DIR}/client.csr" "${MTLS_DIR}/ca-cert.srl"

cp "${MTLS_DIR}/server-cert.pem" "${MTLS_DIR}/public.crt"
cp "${MTLS_DIR}/server-key.pem" "${MTLS_DIR}/private.key"

echo "Generated test certs in ${MTLS_DIR}"
ls -1 "${MTLS_DIR}"

if [ -d src/test/resources/mtls ]; then
  sudo chown -R "$(id -u):$(id -g)" src/test/resources/mtls || true
  chmod -R a+rX src/test/resources/mtls || true
fi
