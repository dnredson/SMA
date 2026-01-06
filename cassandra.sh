#!/usr/bin/env bash
# Enable Cassandra storage (writer/reader) for Magistrala
# Usage: run from the repository root (where ./docker/docker-compose.yaml exists).
#   chmod +x enable_cassandra.sh && ./enable_cassandra.sh
set -euo pipefail

# --- helpers ---------------------------------------------------------------
die(){ echo "ERROR: $*" >&2; exit 1; }
have(){ command -v "$1" >/dev/null 2>&1; }

# docker compose wrapper (works with Docker Compose v2 or legacy docker-compose)
compose(){
  if have docker && docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  elif have docker-compose; then
    docker-compose "$@"
  else
    die "Docker Compose not found. Install Docker Desktop/Engine with Compose v2."
  fi
}

# --- sanity checks ---------------------------------------------------------
[ -f "./docker/docker-compose.yaml" ] || die "Run this script from the project root (missing ./docker/docker-compose.yaml)."
[ -f "./docker/.env" ] || die "Missing ./docker/.env (the main environment file)."

# --- constants -------------------------------------------------------------
ADDON_DIR="docker/addons/cassandra"
CASSANDRA_IMG="cassandra:4.1"
MG_CASS_WRITER_IMG="ghcr.io/absmach/magistrala/cassandra-writer:${MG_CASSANDRA_WRITER_TAG:-0.9.0}"
MG_CASS_READER_IMG="ghcr.io/absmach/magistrala/cassandra-reader:${MG_CASSANDRA_READER_TAG:-0.9.0}"

# Ports for the add-on
WRITER_HTTP_PORT="${MG_CASSANDRA_WRITER_HTTP_PORT:-9014}"
READER_HTTP_PORT="${MG_CASSANDRA_READER_HTTP_PORT:-9015}"
READER_GRPC_PORT="${MG_CASSANDRA_READER_GRPC_PORT:-7015}"
CASSANDRA_CQL_PORT=9042

# --- create add-on files ---------------------------------------------------
mkdir -p "${ADDON_DIR}"

# If a writer config exists for Postgres, reuse it; else emit a minimal default.
if [ -f "docker/addons/postgres-writer/config.toml" ] && [ ! -f "${ADDON_DIR}/config.toml" ]; then
  cp "docker/addons/postgres-writer/config.toml" "${ADDON_DIR}/config.toml"
fi
if [ ! -f "${ADDON_DIR}/config.toml" ]; then
  cat > "${ADDON_DIR}/config.toml" <<'TOML'
# Minimal writer config (SenML)
["subscriber"]
subjects = ["writers.>"]

[transformer]
format = "senml" # or "json"
content_type = "application/senml+json"
TOML
fi

# Write the Cassandra add-on compose file
cat > "${ADDON_DIR}/docker-compose.yaml" <<YAML
networks:
  magistrala-base-net:

volumes:
  magistrala-cassandra-data:

services:
  cassandra:
    image: ${CASSANDRA_IMG}
    container_name: magistrala-cassandra
    restart: on-failure
    environment:
      CASSANDRA_CLUSTER_NAME: magistrala
      CASSANDRA_START_RPC: "true"
    ports:
      - "${CASSANDRA_CQL_PORT}:${CASSANDRA_CQL_PORT}"
    networks:
      - magistrala-base-net
    volumes:
      - magistrala-cassandra-data:/var/lib/cassandra

  cassandra-writer:
    image: ${MG_CASS_WRITER_IMG}
    container_name: magistrala-cassandra-writer
    depends_on:
      - cassandra
    restart: on-failure
    environment:
      MG_CASSANDRA_WRITER_LOG_LEVEL: \${MG_CASSANDRA_WRITER_LOG_LEVEL}
      MG_CASSANDRA_WRITER_CONFIG_PATH: \${MG_CASSANDRA_WRITER_CONFIG_PATH}
      MG_CASSANDRA_WRITER_HTTP_HOST: \${MG_CASSANDRA_WRITER_HTTP_HOST}
      MG_CASSANDRA_WRITER_HTTP_PORT: \${MG_CASSANDRA_WRITER_HTTP_PORT}
      MG_CASSANDRA_WRITER_HTTP_SERVER_CERT: \${MG_CASSANDRA_WRITER_HTTP_SERVER_CERT}
      MG_CASSANDRA_WRITER_HTTP_SERVER_KEY: \${MG_CASSANDRA_WRITER_HTTP_SERVER_KEY}
      MG_CASSANDRA_WRITER_INSTANCE_ID: \${MG_CASSANDRA_WRITER_INSTANCE_ID}

      MG_CASSANDRA_HOSTS: \${MG_CASSANDRA_HOSTS}
      MG_CASSANDRA_PORT: \${MG_CASSANDRA_PORT}
      MG_CASSANDRA_KEYSPACE: \${MG_CASSANDRA_KEYSPACE}
      MG_CASSANDRA_USERNAME: \${MG_CASSANDRA_USERNAME}
      MG_CASSANDRA_PASSWORD: \${MG_CASSANDRA_PASSWORD}

      SMQ_MESSAGE_BROKER_URL: \${SMQ_MESSAGE_BROKER_URL}
      SMQ_JAEGER_URL: \${SMQ_JAEGER_URL}
      SMQ_JAEGER_TRACE_RATIO: \${SMQ_JAEGER_TRACE_RATIO}
      SMQ_SEND_TELEMETRY: \${SMQ_SEND_TELEMETRY}
    ports:
      - "${WRITER_HTTP_PORT}:${WRITER_HTTP_PORT}"
    networks:
      - magistrala-base-net
    volumes:
      - ./config.toml:/config.toml

  cassandra-reader:
    image: ${MG_CASS_READER_IMG}
    container_name: magistrala-cassandra-reader
    depends_on:
      - cassandra
    restart: on-failure
    environment:
      MG_CASSANDRA_READER_LOG_LEVEL: \${MG_CASSANDRA_READER_LOG_LEVEL}
      MG_CASSANDRA_READER_HTTP_HOST: \${MG_CASSANDRA_READER_HTTP_HOST}
      MG_CASSANDRA_READER_HTTP_PORT: \${MG_CASSANDRA_READER_HTTP_PORT}
      MG_CASSANDRA_READER_GRPC_HOST: \${MG_CASSANDRA_READER_GRPC_HOST}
      MG_CASSANDRA_READER_GRPC_PORT: \${MG_CASSANDRA_READER_GRPC_PORT}
      MG_CASSANDRA_READER_HTTP_SERVER_CERT: \${MG_CASSANDRA_READER_HTTP_SERVER_CERT}
      MG_CASSANDRA_READER_HTTP_SERVER_KEY: \${MG_CASSANDRA_READER_HTTP_SERVER_KEY}
      MG_CASSANDRA_READER_INSTANCE_ID: \${MG_CASSANDRA_READER_INSTANCE_ID}

      MG_CASSANDRA_HOSTS: \${MG_CASSANDRA_HOSTS}
      MG_CASSANDRA_PORT: \${MG_CASSANDRA_PORT}
      MG_CASSANDRA_KEYSPACE: \${MG_CASSANDRA_KEYSPACE}
      MG_CASSANDRA_USERNAME: \${MG_CASSANDRA_USERNAME}
      MG_CASSANDRA_PASSWORD: \${MG_CASSANDRA_PASSWORD}

      SMQ_JAEGER_URL: \${SMQ_JAEGER_URL}
      SMQ_JAEGER_TRACE_RATIO: \${SMQ_JAEGER_TRACE_RATIO}
      SMQ_SEND_TELEMETRY: \${SMQ_SEND_TELEMETRY}
    ports:
      - "${READER_HTTP_PORT}:${READER_HTTP_PORT}"
      - "${READER_GRPC_PORT}:${READER_GRPC_PORT}"
    networks:
      - magistrala-base-net
YAML

# --- patch docker/.env -----------------------------------------------------
ENV_FILE="docker/.env"

# Append the Cassandra block only once
if ! grep -q '^# Cassandra connection' "$ENV_FILE" ; then
  cat >> "$ENV_FILE" <<'ENVVARS'

# Cassandra connection
MG_CASSANDRA_HOSTS=cassandra
MG_CASSANDRA_PORT=9042
MG_CASSANDRA_KEYSPACE=magistrala
MG_CASSANDRA_USERNAME=
MG_CASSANDRA_PASSWORD=

# Cassandra Writer
MG_CASSANDRA_WRITER_LOG_LEVEL=debug
MG_CASSANDRA_WRITER_CONFIG_PATH=/config.toml
MG_CASSANDRA_WRITER_HTTP_HOST=cassandra-writer
MG_CASSANDRA_WRITER_HTTP_PORT=9014
MG_CASSANDRA_WRITER_HTTP_SERVER_CERT=
MG_CASSANDRA_WRITER_HTTP_SERVER_KEY=
MG_CASSANDRA_WRITER_INSTANCE_ID=

# Cassandra Reader
MG_CASSANDRA_READER_LOG_LEVEL=debug
MG_CASSANDRA_READER_HTTP_HOST=cassandra-reader
MG_CASSANDRA_READER_HTTP_PORT=9015
MG_CASSANDRA_READER_GRPC_HOST=cassandra-reader
MG_CASSANDRA_READER_GRPC_PORT=7015
MG_CASSANDRA_READER_HTTP_SERVER_CERT=
MG_CASSANDRA_READER_HTTP_SERVER_KEY=
MG_CASSANDRA_READER_INSTANCE_ID=
ENVVARS
fi

# Point the default reader URL to Cassandra Reader
if grep -q '^MG_READER_URL=' "$ENV_FILE"; then
  sed -i.bak 's|^MG_READER_URL=.*|MG_READER_URL=http://cassandra-reader:9015|' "$ENV_FILE"
else
  echo 'MG_READER_URL=http://cassandra-reader:9015' >> "$ENV_FILE"
fi

# --- bring services up -----------------------------------------------------
echo "Bringing up base stack (if not already running)..."
compose -f docker/docker-compose.yaml --env-file docker/.env up -d

echo "Starting Cassandra only to initialize keyspace..."
compose -f docker/docker-compose.yaml -f "${ADDON_DIR}/docker-compose.yaml" --env-file docker/.env up -d cassandra

# Wait for Cassandra to be ready
echo -n "Waiting for Cassandra to accept CQL (this may take ~30-60s)"
for i in {1..40}; do
  if docker exec magistrala-cassandra cqlsh -e "SHOW VERSION;" >/dev/null 2>&1; then
    echo " - ready."
    break
  fi
  echo -n "."
  sleep 3
  if [ "$i" -eq 40 ]; then
    echo
    die "Cassandra did not become ready in time."
  fi
done

# Create keyspace if missing (SimpleStrategy for local/dev)
echo "Ensuring keyspace 'magistrala' exists..."
docker exec magistrala-cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS magistrala WITH replication = {'class':'SimpleStrategy','replication_factor':1};" >/dev/null

echo "Starting Cassandra writer & reader..."
compose -f docker/docker-compose.yaml -f "${ADDON_DIR}/docker-compose.yaml" --env-file docker/.env up -d cassandra-writer cassandra-reader

cat <<'DONE'

✅ Cassandra writer/reader enabled.

• Docker images (default):
    - DB:            cassandra:4.1
    - Writer:        ghcr.io/absmach/magistrala/cassandra-writer:${MG_CASSANDRA_WRITER_TAG:-0.9.0}
    - Reader:        ghcr.io/absmach/magistrala/cassandra-reader:${MG_CASSANDRA_READER_TAG:-0.9.0}

• Config files created:
    - docker/addons/cassandra/docker-compose.yaml
    - docker/addons/cassandra/config.toml  (copied or minimal default)

• Environment updated in ./docker/.env:
    - Added MG_CASSANDRA_* variables
    - MG_READER_URL=http://cassandra-reader:9015

To switch back to Timescale/Postgres Reader, revert MG_READER_URL in docker/.env and (optionally) stop the Cassandra add-on:
  compose -f docker/docker-compose.yaml -f docker/addons/cassandra/docker-compose.yaml --env-file docker/.env down cassandra-reader cassandra-writer cassandra

DONE
