#!/usr/bin/env bash
# setup_fresh_scylla.sh
#
# Creates the fluxer keyspace and runs all CQL migrations against a fresh
# ScyllaDB instance. Assumes the container is named fluxer-scylla and is
# reachable via the fluxer podman network.
#
# Usage:
#   ./setup_fresh_scylla.sh
#   MIGRATIONS_DIR=/path/to/migrations ./setup_fresh_scylla.sh
#
# Env vars:
#   CONTAINER      — podman container name (default: fluxer-scylla)
#   CQLSH_USER     — CQL username (default: cassandra)
#   CQLSH_PASS     — CQL password (default: cassandra)
#   KEYSPACE       — keyspace name (default: fluxer)
#   MIGRATIONS_DIR — path to .cql migration files (default: same dir as this script/../migrations)

set -euo pipefail

CONTAINER="${CONTAINER:-fluxer-scylla}"
CQLSH_USER="${CQLSH_USER:-cassandra}"
CQLSH_PASS="${CQLSH_PASS:-cassandra}"
KEYSPACE="${KEYSPACE:-fluxer}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$SCRIPT_DIR/../migrations}"

cqlsh() {
  sudo podman exec -i "$CONTAINER" cqlsh -u "$CQLSH_USER" -p "$CQLSH_PASS" "$@"
}

echo "==> Waiting for ScyllaDB to be ready..."
until cqlsh -e "SELECT now() FROM system.local" &>/dev/null; do
  echo "    Not ready yet, retrying in 5s..."
  sleep 5
done
echo "    Ready."

echo ""
echo "==> Creating keyspace '$KEYSPACE'..."
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

echo ""
echo "==> Running migrations from $MIGRATIONS_DIR..."
for f in $(ls -1 "$MIGRATIONS_DIR"/*.cql | sort); do
  echo "    Running $(basename "$f")..."
  cqlsh -k "$KEYSPACE" < "$f"
done

echo ""
echo "==> Done. Keyspace '$KEYSPACE' is ready."
