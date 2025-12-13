#!/bin/bash
set -euo pipefail

cp /docker-entrypoint-initdb.d/ca.pem          /var/lib/mysql/ca.pem
cp /docker-entrypoint-initdb.d/server-cert.pem /var/lib/mysql/server-cert.pem
cp /docker-entrypoint-initdb.d/server-key.pem  /var/lib/mysql/server-key.pem

chown mysql:mysql /var/lib/mysql/ca.pem
chown mysql:mysql /var/lib/mysql/server-cert.pem
chown mysql:mysql /var/lib/mysql/server-key.pem

chmod 600 /var/lib/mysql/server-key.pem || true
chmod 644 /var/lib/mysql/ca.pem /var/lib/mysql/server-cert.pem || true
