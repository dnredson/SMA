# SmartAdapter (SMA)

SmartAdapter consumes sensor messages over MQTT, keeps the existing sensor
parsers, normalizes their measurements to SenML JSON, and publishes them to
Magistrala through the current Atom-backed FluxMQ HTTP API.

The v2 runtime also manages persistent device mappings and lifecycle state,
Atom profiles/policies, retry/DLQ, data quality, presence, LoRaWAN gateway
presence/topology, MQTT alerts, and provider-neutral LLM context with optional
Timescale history/trends.

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

LoRaWAN gateways are also represented as Atom entities under the dedicated
`smarter-adapter-lorawan-gateway` profile. Gateway entities are observational
infrastructure objects and deliberately do not receive the sensor channel
`publish` permission.

## ChirpStack message roles and provenance

A single physical ChirpStack device can use different LoRaWAN fPorts for
different logical payloads. SMA keeps the transport metadata (`mqtt_topic`,
`f_port`, application id and a configurable port-role hint) but interprets the
payload from its actual keys rather than from field position.

For the current Irrigap traffic this distinguishes, for example:

```text
fPort 1   + VB/BT -> battery.voltage / battery.level
fPort 31  + M/T/C -> soil telemetry
```

The parser no longer treats a battery frame such as `VB|4.2|BT|100` as if
`4.2` were soil moisture and `100` were temperature. Greenstick calibration is
applied only to Greenstick nodes; Teros12 raw moisture is preserved until a
Teros-specific calibration is explicitly validated.

ChirpStack `rxInfo` is normalized as gateway reception metadata (gateway id,
RSSI, SNR, channel, RF chain and CRC status). After successful telemetry
publication SMA persists the observed sensor-to-gateway relationship.

Data-quality snapshots are also maintained per logical message role. A valid
battery frame therefore cannot hide a still-invalid soil snapshot for the same
physical sensor.

## LoRaWAN gateway presence

The gateway monitor is a separate read-only MQTT side input. It does not widen
the main sensor pipeline subscription, so gateway traffic cannot fall through
the sensor parser into the retry/DLQ path.

By default it observes:

```text
+/gateway/+/event/stats
gateway/+/state/conn
+/gateway/+/state/conn
```

Periodic `event/stats` traffic and sensor `rxInfo` both provide operational
activity. Retained `state/conn` packets can discover a gateway but do not by
themselves mark it online, avoiding a false-positive liveness state when SMA
subscribes after a broker reconnect.

The Irrigap defaults reflect the measured ~30 second stats interval:

```text
expected interval = 30 s
stale after       = 90 s
offline after     = 180 s
```

All thresholds and gateway MQTT settings are configurable in `.env.example`.
Gateway presence and topology are persisted in the same SQLite state database.

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
- LoRaWAN gateway monitoring and presence thresholds
- Management API bearer token
- persistent retry/DLQ policy
- MQTT alert side-channel
- LLM-context side-channel
- Timescale history/trend settings
- sensor presence thresholds

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
GET  /api/v2/gateways
GET  /api/v2/gateways/{gateway_id}
GET  /api/v2/retry
GET  /api/v2/dlq
POST /api/v2/reconcile
GET  /api/v2/catalog/devices
```

`GET /api/v2/devices/{external_id}/context` builds provider-neutral context on
demand. It combines durable SMA identity/presence/quality/lifecycle state with
the latest persisted Timescale observation and recent trend summaries. When
available, it also includes the LoRaWAN gateways that recently received the
sensor, including last RSSI/SNR information. This allows an agent or LLM
integration to inspect a managed sensor without waiting for the next MQTT
event.

The endpoint keeps persisted measurements distinct from newer adapter state; a
delayed Timescale writer therefore cannot replace the adapter's latest quality
snapshot. If Timescale is temporarily unavailable, the endpoint still returns
durable device state and marks observation/history as unavailable.

`GET /api/v2/gateways` exposes derived online/stale/offline presence plus last
stats/uplink/connection timestamps, Atom entity id, counters and topic root.

Additional catalog lifecycle endpoints are available under `/api/v2`. Set
`SMA_API_TOKEN` to require `Authorization: Bearer ...` for protected routes.

## Passive field sample capture

For parser/calibration work, `scripts/capture_irrigap_samples.py` can passively
collect real MQTT packets without sending anything back to the broker. By
default it reuses broker settings from the local `.env`, subscribes only to the
configured Irrigap application telemetry topic, decodes the base64 payload, and
writes one self-contained JSON record per MQTT message.

```bash
source .venv/bin/activate
python scripts/capture_irrigap_samples.py
```

Captured JSONL files are written under `captures/` and are ignored by Git
because they can contain deployment/device identifiers. Each record keeps the
original topic/envelope plus convenient fields such as device name, DevEUI,
fPort, decoded UTF-8/hex payload and parsed ultralight key/value pairs.

Useful filters/examples:

```bash
# One specific device, unlimited duration
python scripts/capture_irrigap_samples.py \
  --device teros12-sector1.3

# Inventory the complete broker passively, including gateway/direct-sensor topics
python scripts/capture_irrigap_samples.py --all-topics

# Two devices for one hour
python scripts/capture_irrigap_samples.py \
  --device teros12-sector1.3 \
  --device TEST-GREENSTICK-3303 \
  --duration 3600

# Stop after 100 matching packets
python scripts/capture_irrigap_samples.py \
  --max-messages 100
```

The capture tool is intentionally observational only. The SMA write-enabled
sensor runtime should continue using the narrow telemetry topic rather than a
broad `#` subscription; gateway liveness uses its own dedicated read-only
subscriptions.

## Run tests

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```
