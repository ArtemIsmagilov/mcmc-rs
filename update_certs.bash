# remove old certs
rm ca.crt ca.key server.crt server.key server.csr server.ext
# private key CA
openssl genrsa -out ca.key 4096
# Self CA cert
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -out ca.crt \
  -subj "/CN=MyMemcachedTestCA"
# Key server
openssl genrsa -out server.key 4096
# CSR with CN=localhost
openssl req -new -key server.key -out server.csr \
  -subj "/CN=localhost"
# extention SAN file
cat >server.ext <<'EOF'
basicConstraints=CA:FALSE
subjectAltName=@alt_names

[alt_names]
DNS.1 = localhost
EOF
# Sign the server certificate with a SAN
openssl x509 -req -in server.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365 -sha256 \
  -extfile server.ext
# look SAN
openssl x509 -in server.crt -noout -text | grep -A2 "Subject Alternative Name"
# CA:FALSE
openssl x509 -in server.crt -noout -text | grep -A1 "Basic Constraints"
# server.crt: OK
openssl verify -CAfile ca.crt server.crt
# Key client
openssl genrsa -out client.key 4096
# CSR with CN=memcached-client (имя может быть любым)
openssl req -new -key client.key -out client.csr \
  -subj "/CN=memcached-client"
# Extension file for client (без SAN, только базовые ограничения)
cat >client.ext <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
# Sign the client certificate with the existing CA
openssl x509 -req -in client.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 365 -sha256 \
  -extfile client.ext
# Verify client cert against CA
openssl verify -CAfile ca.crt client.crt
