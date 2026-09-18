# SmartAdapter (SMA)

SmartAdapter consumes sensor messages over MQTT, keeps the existing sensor
parsers, normalizes their measurements to SenML JSON, and publishes them to
Magistrala through the current Atom-backed FluxMQ HTTP API.

The v2 runtime also manages persistent device mappings and lifecycle state,
Atom profiles/policies, retry/DLQ, data quality, presence, MQTT alerts, and
provider-neutral LLM context with optional Timescale history/trends.

## Magistrala / Atom integration

The adapter no longer uses the removed `users`, `domains`, `clients` or
`magistrala-cli` management APIs. Atom is accessed through GraphQL at
`POST <ATOM_URL>/graphql` with a bearer token or configured service account.
The adapter treats a sensor as an Atom entity with `kind = "device"` and its
normalized identity as `externalId`.

Telemetry is published with:

```text
POST /<tenant_id>/channels/<channel_id>/messages
Authorization: Bearer <ATOM token>
Content-Type: application/json
```

The JSON body is the FluxMQ HTTP envelope:

```json
{
  "device_id": "<atom-device-id>",
  "subtopic": "",
  "payload": [{"bn":"DEVICE:","bt":0,"n":"air.temperature","u":"Cel","v":25}]
}
```

On first observation, SMA can create/reconcile the Atom device, typed profile,
and direct `publish` policy for the configured channel. Local SQLite state
keeps durable mappings, lifecycle/retry/DLQ/quality metadata and does not store
a per-device secret.

## Quick start / deployment configuration

Create the Python environment and install the project dependencies as usual,
then create a local runtime configuration from the tracked template:

```bash
cp .env.example .env
chmod 600 .env
$EDITOR .env
```

Run the `cp` step only for a new installation. On an existing deployment, do
not overwrite `.env`: it contains the machine-local credentials and settings.

`.env.example` is safe to commit and documents the supported runtime variables.
The real `.env` is intentionally ignored by Git and must contain deployment
credentials only on the target machine.

The recommended v2 entry point is:

```bash
source .venv/bin/activate
python scripts/run_v2.py
```

`run_v2.py` loads the repository-root `.env` automatically. Configuration
precedence is:

```text
shell / systemd environment > .env > code defaults
```

This makes local development convenient while still allowing production
service managers, containers, or secret injection to override individual
values without modifying the file.

Important groups in `.env.example` include:

- Magistrala/Atom connection and authentication
- sensor MQTT input
- Management API bearer token
- persistent retry/DLQ policy
- MQTT alert side-channel
- LLM-context side-channel
- Timescale history/trend settings
- presence thresholds

For another deployment, replace the Irrigap broker/topic/catalog values rather
than committing site-specific credentials.

## Management API

The v2 Management API listens on `127.0.0.1:8082` by default. Main endpoints
include:

```text
GET  /health
GET  /ready
GET  /metrics
GET  /api/v2/status
GET  /api/v2/devices
GET  /api/v2/devices/{external_id}/messages
GET  /api/v2/devices/{external_id}/context
GET  /api/v2/retry
GET  /api/v2/dlq
POST /api/v2/reconcile
GET  /api/v2/catalog/devices
```

`GET /api/v2/devices/{external_id}/context` builds provider-neutral context on
demand. It combines durable SMA identity/presence/quality/lifecycle state with
the latest persisted Timescale observation and recent trend summaries. This
allows an agent or LLM integration to inspect a managed sensor without waiting
for the next MQTT event. The endpoint keeps persisted measurements distinct
from newer adapter state; a delayed Timescale writer therefore cannot replace
the adapter's latest quality snapshot. If Timescale is temporarily unavailable,
the endpoint still returns durable device state and marks observation/history
as unavailable.

Additional catalog lifecycle endpoints are available under `/api/v2`. Set
`SMA_API_TOKEN` to require `Authorization: Bearer ...` for protected routes.

## Run tests

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```
