# Smarter Adapter (SMA) 2.0

Smarter Adapter (SMA) is an IoT adaptation and management layer that receives heterogeneous sensor traffic, identifies the correct parser, normalizes telemetry to SenML, reconciles devices and permissions in Magistrala/Atom, publishes telemetry, and keeps durable operational state for lifecycle, quality, presence, retries, LoRaWAN gateways, RF health, alerts, and AI/LLM context.

SMA 2.0 is designed to sit between field protocols/brokers and the current Atom-backed Magistrala stack without forcing sensor-specific logic into the platform itself.

The current implementation has been validated with real Irrigap MQTT/ChirpStack traffic, including Teros12 soil/battery messages, Greenstick sensors, ChirpStack gateway statistics, persistent retry/ingress state, sensor-to-gateway topology, RSSI/SNR history, and LoRa radio parameters.

> Status: active field validation. The core v2 architecture is implemented; remaining work is mainly long-running validation and incremental hardening rather than a redesign of the pipeline.

---

## What SMA does

At a high level SMA performs the following tasks:

1. subscribes to one or more MQTT inputs;
2. persists the raw message before initial processing when the production SQLite store is enabled;
3. selects a compatible parser plugin;
4. converts the source payload into a transport-neutral `ParsedEvent`;
5. applies data-quality rules;
6. resolves or creates the corresponding Atom entity and typed profile;
7. ensures the direct publish permission required by the target Magistrala channel;
8. converts measurements to SenML;
9. publishes the SenML payload through the current Magistrala/FluxMQ HTTP path;
10. records device lifecycle, presence, quality and topology state;
11. retries transient failures and sends exhausted/permanent failures to a DLQ;
12. optionally publishes alerts and provider-neutral LLM context;
13. observes ChirpStack gateways separately from the sensor input;
14. persists RF observations for each sensor-to-gateway link.

The adapter intentionally separates transport, parsing, device semantics and control-plane behavior so that new sensors can be added without changing the core runtime.

---

## Architecture

```text
                             +----------------------+
                             |   MQTT sensor input  |
                             +----------+-----------+
                                        |
                                        v
                              +-------------------+
                              | Durable raw ingress|
                              | SQLite ingress_queue|
                              +---------+---------+
                                        |
                                        v
+----------------+          +----------+-----------+          +------------------+
| Parser plugins | -------> |   Parse / Quality    | -------> | Device Registry  |
| protocol aware |          | transport-neutral   |          | + Atom control   |
+----------------+          +----------+-----------+          +---------+--------+
                                        |                                |
                                        v                                v
                               +-----------------+               +---------------+
                               |      SenML      |               | typed profile |
                               +--------+--------+               | publish policy|
                                        |                        +---------------+
                                        v
                               +-----------------+
                               | Magistrala HTTP |
                               | /channels/...   |
                               +--------+--------+
                                        |
                   +--------------------+--------------------+
                   |                    |                    |
                   v                    v                    v
             +-----------+        +-----------+        +------------+
             | Timescale |        | Retry/DLQ |        | alerts/LLM |
             +-----------+        +-----------+        +------------+

Separate LoRaWAN observation path:

ChirpStack gateway stats ---------------------> Gateway monitor/state
ChirpStack uplink rxInfo/txInfo --------------> RF link history
                                               sensor <-> gateway
```

### Architectural rules

- Input plugins do not know about Atom, SenML or publish policies.
- Parser plugins convert protocol-specific input into a common `ParsedEvent`.
- A physical sensor keeps one stable identity even when different fPorts carry different logical message roles.
- Transport metadata such as MQTT topic, fPort and LoRa radio settings are observations, not permanent device identity.
- Sensor telemetry and gateway monitoring use separate MQTT subscriptions.
- Protocol or firmware variants that intentionally change payload meaning should normally be implemented as separate parser plugins rather than silently reinterpreting old data.

---

## Magistrala / Atom model

SMA targets the current Atom-backed Magistrala architecture and no longer depends on the removed legacy `users`, `domains`, `clients` or `magistrala-cli` management APIs.

The current mapping is:

| SMA concept | Magistrala / Atom representation |
| --- | --- |
| workspace / deployment | Atom tenant/workspace scope |
| physical sensor | Atom entity with `kind=device` |
| device family | Atom profile + active profile version |
| telemetry channel | Magistrala channel/resource |
| sensor publish authorization | direct Atom policy/permission |
| normalized measurements | SenML JSON |
| persistence | Magistrala rule -> Timescale writer |
| LoRaWAN gateway | Atom entity using the dedicated gateway profile |

On first observation SMA can create or reconcile the device and bind it to the correct profile without changing the external physical identity.

Telemetry is sent to the current FluxMQ HTTP route with an Atom bearer token:

```text
POST /<workspace_id>/channels/<channel_id>/messages
Authorization: Bearer <atom-token>
Content-Type: application/json
```

Example body:

```json
{
  "device_id": "<atom-device-id>",
  "subtopic": "",
  "payload": [
    {
      "bn": "teros12-sector5",
      "bt": 1789817273.0,
      "n": "soil.temperature",
      "u": "Cel",
      "v": 22.1
    }
  ]
}
```

---

## Reliability model

### Durable ingress

With the production SQLite store, the raw MQTT message is inserted into `ingress_queue` before parser/runtime processing.

```text
MQTT callback
    |
    v
SQLite ingress_queue
    |
    v
parser -> quality -> Atom -> SenML -> publish
    |                                  |
    | success                          | transient failure
    v                                  v
delete ingress                    ingress -> retry
                                         |
                                  exhausted/permanent
                                         v
                                        DLQ
```

Pending ingress records survive process restarts. Transfers from ingress to retry or DLQ are transactional.

SMA currently does **not** claim exactly-once end-to-end delivery. In particular:

- MQTT QoS 0 can lose a packet before the process receives it;
- a crash after Magistrala accepts a publish but before SMA removes the ingress row can cause a duplicate on recovery.

End-to-end idempotency is considered an additional hardening layer rather than a property of the current implementation.

### Retry and DLQ

Typical behavior:

- connection errors / timeouts / retryable 5xx / 429 -> persistent retry with backoff;
- expired authentication -> token refresh/re-login where supported;
- 403/404 during publish -> one reconciliation attempt of local/remote device state and policy;
- parser errors or permanently invalid input -> no infinite retry;
- exhausted retry -> DLQ.

---

## Device lifecycle and presence

SMA keeps local persistent device bindings and lifecycle information in SQLite. The current lifecycle supports planned/observed/managed behavior plus explicit decommission/reactivation flows.

Presence is derived from persisted `last_seen` timestamps at read time. Reporting cadence can be configured per sensor family:

```text
expected_interval_seconds
stale_after_seconds
offline_after_seconds
```

The current Irrigap Teros12 defaults are:

```text
expected interval = 600 s
stale after       = 900 s
offline after     = 1800 s
```

Families without an explicit cadence profile use the global fallback thresholds.

---

## LoRaWAN gateway monitoring

Gateway monitoring is intentionally a separate read-only MQTT side input. It does not widen the main sensor runtime subscription.

Default ChirpStack gateway topics:

```text
+/gateway/+/event/stats
gateway/+/state/conn
+/gateway/+/state/conn
```

For the current deployment, measured stats cadence is approximately 30 seconds, so the default presence policy is:

```text
expected interval = 30 s
stale after       = 90 s
offline after     = 180 s
```

`event/stats` payloads are decoded from ChirpStack `gw.GatewayStats` in protobuf or JSON form. SMA currently extracts operational fields such as:

- gateway id;
- gateway timestamp;
- config version;
- location when present;
- RX/TX counters;
- per-frequency/status counters when present;
- gateway metadata;
- normalized model/forwarder version/concentrator temperature when those metadata keys are present.

A retained opaque `state/conn` packet may discover a gateway but is not treated as proof that it is currently online. Periodic stats and observed uplinks are authoritative activity sources.

LoRaWAN gateways are represented as Atom entities using the dedicated `smarter-adapter-lorawan-gateway` profile. They are infrastructure observations and do not receive the sensor telemetry publish permission.

---

## RF health and sensor-to-gateway topology

For every ChirpStack uplink reception, SMA can persist the observed relation between one managed sensor and one gateway.

Each RF sample may contain:

```text
gateway_id
observed_at
RSSI
SNR
channel
RF chain
CRC status
fPort
message role
MQTT topic
frequency_hz
modulation
spreading_factor
bandwidth_hz
code_rate
bitrate_bps
```

The current ChirpStack integration has been field-validated with LoRa parameters such as:

```text
frequency      917600000 Hz
modulation     lora
spreading      SF7
bandwidth      125000 Hz
code rate      CR_4_5
```

RF samples are retained in SQLite independently of sensor measurements. The default retention is 90 days.

The RF endpoint reports:

- latest observation;
- min/max/average RSSI and SNR;
- regression slope and raw trend;
- channels observed;
- logical message roles observed;
- grouping by radio profile;
- grouping by concentrator channel;
- a conservative comparability assessment.

SMA deliberately avoids turning an aggregate RSSI value into an absolute `good`/`bad` verdict when samples were collected under different radio profiles. When radio-profile coverage is incomplete, the endpoint reports `mixed_conditions` instead.

Endpoint:

```text
GET /api/v2/devices/{external_id}/rf?hours=24&limit=500
```

Optional gateway filter:

```text
?gateway_id=<gateway-id>
```

---

## Data quality and logical message roles

One physical sensor can transmit different logical message roles on different fPorts.

For the current Irrigap ChirpStack traffic:

```text
fPort 1  + VB/BT keys -> battery
fPort 31 + M/T/C keys -> soil
```

The parser uses the actual payload keys as semantic authority. fPort is preserved as transport metadata and can provide a role hint, but it does not create a second device identity.

Quality state is kept per role so that a later valid battery packet cannot hide an invalid soil packet.

Example context state:

```json
{
  "quality_by_role": {
    "battery": {"data_quality": "valid"},
    "soil": {"data_quality": "valid"}
  }
}
```

---

# Parser and input plugins

The production v2 registry currently loads the native Irrigap ChirpStack parser first, then the legacy compatibility parser. The compatibility parser delegates to the already existing v1 sensor decoders.

## 1. MQTT input plugin

Implementation:

```text
src/smarter_adapter/inputs/mqtt.py
```

Expected input: arbitrary MQTT topic + raw bytes. The plugin produces a transport-neutral `RawEvent` containing source, topic, payload, receive timestamp and MQTT metadata.

Single-input configuration uses `SMA_MQTT_*` variables. Multiple MQTT inputs can be provided with `SMA_MQTT_INPUTS_JSON`.

Example multi-input configuration:

```json
[
  {
    "name": "field-a",
    "host": "mqtt.example.net",
    "port": 1883,
    "topic": "sensors/#",
    "qos": 0,
    "client_id": "sma-field-a"
  }
]
```

## 2. Native Irrigap ChirpStack parser

Implementation:

```text
src/smarter_adapter/parsers/chirpstack_irrigap.py
```

Plugin name:

```text
chirpstack-irrigap-v2
```

Expected MQTT payload: ChirpStack application uplink JSON with at least:

```json
{
  "time": "2026-09-19T11:00:00Z",
  "deviceInfo": {
    "deviceName": "teros12-sector5",
    "applicationId": "..."
  },
  "fPort": 31,
  "data": "<base64 encoded ultralight payload>",
  "rxInfo": [
    {
      "gatewayId": "000000ffff001002",
      "rssi": -23,
      "snr": 9.8,
      "channel": 4,
      "rfChain": 1,
      "crcStatus": "CRC_OK"
    }
  ],
  "txInfo": {
    "frequency": 917600000,
    "modulation": {
      "lora": {
        "bandwidth": 125000,
        "spreadingFactor": 7,
        "codeRate": "CR_4_5"
      }
    }
  }
}
```

Decoded payload examples:

Soil frame:

```text
S|2609191100|I|2305|M|2342.7|T|18.2|C|65
```

Battery frame:

```text
S|2609191110|I|2305|VB|4.2|BT|100
```

Current behavior:

- `VB` -> `battery.voltage`;
- `BT` -> `battery.level`;
- `M/M1` -> raw soil moisture field;
- `T/T1` -> soil temperature;
- `C/C1` -> electrical conductivity/raw EC;
- negative source sentinels are marked invalid instead of being interpreted as physical zero;
- Greenstick calibration is applied only to Greenstick nodes;
- Teros12 ChirpStack raw moisture is intentionally preserved until a Teros-specific calibration is explicitly validated;
- `rxInfo` produces gateway/RF observations;
- `txInfo` produces radio-profile metadata.

The deployment catalog maps node ids to sensor family/location/depth. Example:

```json
{
  "nodes": [
    {
      "id": "2305",
      "device": "teros12",
      "location": "Sector_5",
      "sub_location": "mz_1",
      "depths": {"31": "15cm"}
    }
  ]
}
```

## 3. Legacy compatibility parser

Implementation:

```text
src/smarter_adapter/legacy_parser.py
src/parsers/
```

Plugin name:

```text
legacy-sensor-parsers
```

The legacy adapter accepts the existing v1 sensor formats and converts their normalized entries into v2 `ParsedEvent` objects.

### WXT520 CSV

Expected topic prefix:

```text
WXT520_
```

Expected payload starts with `0R0,` or `0R3,`.

Example:

```text
0R0,Dm=081D,Sm=1.9M,Ta=25.4C,Ua=73.1P,Pa=1008.2H,Rc=1.2M,Rd=45S,Ri=0.5M,Vs=12.1V
```

Normalized fields include wind direction/speed, air temperature, relative humidity, pressure, precipitation and battery voltage.

### WXT520 SDI-12

Expected topic prefix:

```text
WXT520_
```

Expected payload starts with a numeric address followed by `+` separated values.

Logical format:

```text
0+<wind_direction>+<wind_speed>+<temperature>+<rh_percent>+<pressure>+<rain>+<battery>
```

### ATMOS41 SDI-12

Expected topic prefix:

```text
ATMOS41_
```

Expected payload: numeric SDI address followed by `+` separated numeric values.

The current positional decoder maps the first values to:

```text
solar irradiance
precipitation accumulation
lightning strike count
average lightning distance
wind speed
wind direction
maximum gust
air temperature
vapor pressure
barometric pressure
relative humidity
```

Additional supported positions include humidity sensor temperature, X/Y orientation and north/east wind components.

### TEROS12 SDI-12 (legacy direct format)

Expected topic prefix:

```text
TEROS12_
```

Logical format:

```text
<address>+<VWC counts>+<temperature C>+<bulk EC dS/m>
```

Normalized fields:

```text
soil.vwc.counts
soil.temperature
soil.ec.bulk
```

This legacy direct SDI format is distinct from the current role-aware Irrigap ChirpStack Teros12 payload.

### Greenstick direct Ultralight

Accepted topic prefixes include common Greenstick spelling variants such as:

```text
GREENSTICK...
GREENSTICKS...
GEENSTICK...
GEENSTICKS...
```

Expected payload:

```text
S|yymmddhhmm|I|<node-id>|M1|1261|T1|22.1|C1|640|VB|3.9|BT|70
```

Supported keys include:

```text
M# -> soil.raw.moisture_m#
T# -> soil.raw.temperature_t#
C# -> soil.raw.ec_c#
VB -> battery.voltage
BT -> battery.level
I  -> node/device id metadata
S  -> timestamp marker in the typed direct parser
```

### Greenstick TTN / ChirpStack JSON compatibility

The legacy router also recognizes TTN-style JSON envelopes and Greenstick JSON payloads containing:

```text
end_device_ids
uplink_message
```

It first looks for:

```text
uplink_message.decoded_payload.ultralight
```

and can fall back to base64 `frm_payload` or another decoded string containing the Ultralight `|` representation.

Typical external identity is derived from `end_device_ids.device_id` and normalized to a stable uppercase/underscore form.

---

## Typed Atom profiles

Native typed profiles currently exist for:

```text
teros12
  key: smarter-adapter-teros12

greenstick
  key: smarter-adapter-greenstick

LoRaWAN gateway
  key: smarter-adapter-lorawan-gateway
```

Unknown families use the generic device profile as a safe fallback.

The profile schema describes identity/deployment metadata. Telemetry itself remains SenML rather than being copied into Atom attributes.

---

## Alerts plugin

The optional MQTT alert side channel evaluates processed events against configured rules/thresholds and publishes JSON alerts.

Configure:

```env
SMA_ALERTS_MQTT_ADDRESS=tcp://mqtt.example.net:1883
SMA_ALERTS_TOPIC_BASE=adapter/alerts
SMA_ALERTS_QOS=1
```

Leave `SMA_ALERTS_MQTT_ADDRESS` empty to disable publication.

---

## LLM context plugin

SMA can publish provider-neutral context without depending on a specific model runtime.

The context combines:

- device identity/profile;
- lifecycle and presence;
- quality state and quality by message role;
- latest persisted Timescale observation;
- recent historical trends;
- observed gateway(s);
- last RF values and RF-health summary.

MQTT configuration:

```env
SMA_LLM_CONTEXT_MQTT_ADDRESS=tcp://mqtt.example.net:1883
SMA_LLM_CONTEXT_TOPIC_BASE=adapter/llm-context
SMA_LLM_CONTEXT_QOS=1
```

On-demand context is also available through:

```text
GET /api/v2/devices/{external_id}/context
```

---

# Installation

## Requirements

- Linux is the primary tested environment;
- Python 3.10+;
- access to an MQTT broker;
- current Magistrala with Atom and the required reader/writer services;
- network access from SMA to Atom, Magistrala HTTP ingress and optional Timescale Reader;
- SQLite support from the Python standard library.

Python dependency currently tracked by the project:

```text
paho-mqtt>=2.0,<3
```

## 1. Clone SMA

```bash
git clone https://github.com/dnredson/SMA.git
cd SMA
```

## 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Prepare Magistrala

SMA assumes a current Atom-backed Magistrala deployment. For a standard upstream checkout:

```bash
git clone https://github.com/absmach/magistrala.git
cd magistrala
make run_latest
```

Exact service topology/credentials are deployment-specific; verify Atom, publishing ingress, Rules and Timescale Reader before starting SMA.

## 4. Create the local environment file

For a **new installation only**:

```bash
cp .env.example .env
chmod 600 .env
$EDITOR .env
```

Do not copy `.env.example` over an existing `.env`; the real file contains machine-local credentials and settings and is intentionally ignored by Git.

Configuration precedence is:

```text
shell / systemd environment > .env > code defaults
```

## 5. Run SMA

Recommended v2 entry point:

```bash
source .venv/bin/activate
python scripts/run_v2.py
```

The launcher loads `.env`, installs the current runtime extensions and then starts the canonical MQTT service.

Expected startup output includes the resolved environment, state DB, workspace/channel, enabled plugins, API address, gateway monitor, retry configuration and MQTT input(s).

Stop with `Ctrl+C`.

---

# `.env` configuration

`.env.example` is the canonical list of supported deployment settings. Important groups are summarized below.

## Deployment

```env
SMA_ENVIRONMENT=irrigap
```

Recognized environment aliases currently include test/dev and irrigap/field/production variants.

## Atom / Magistrala

```env
ATOM_URL=http://127.0.0.1
ATOM_USERNAME=admin
ATOM_PASSWORD=CHANGE_ME

# Alternatively:
# ATOM_SERVICE_TOKEN=CHANGE_ME
# ATOM_ADMIN_TOKEN=CHANGE_ME

MAGISTRALA_PUBLISH_URL=http://127.0.0.1
MAGISTRALA_RULES_URL=http://127.0.0.1
MAGISTRALA_READER_URL=http://127.0.0.1:9011
```

Never commit real credentials.

## Management API

```env
SMA_API_TOKEN=CHANGE_ME
# SMA_API_HOST=127.0.0.1
# SMA_API_PORT=8082
```

Protected routes require:

```text
Authorization: Bearer <SMA_API_TOKEN>
```

## Sensor MQTT input

```env
SMA_MQTT_HOST=mqtt.example.net
SMA_MQTT_PORT=1883
SMA_MQTT_TOPIC=application/<app-id>/device/+/event/up
SMA_MQTT_QOS=0
SMA_MQTT_CLIENT_ID=smarter-adapter-v2
SMA_MQTT_SOURCE=mqtt:mqtt.example.net:1883
# SMA_MQTT_USERNAME=CHANGE_ME
# SMA_MQTT_PASSWORD=CHANGE_ME
```

For write-enabled field runtime, prefer a narrow telemetry subscription instead of `#` so unrelated gateway/log events cannot enter the sensor parser/retry path.

## Gateway monitor

```env
SMA_GATEWAY_MONITOR_ENABLED=true
SMA_GATEWAY_EXPECTED_INTERVAL=30
SMA_GATEWAY_STALE_AFTER=90
SMA_GATEWAY_OFFLINE_AFTER=180
```

Gateway MQTT settings normally inherit the main broker and can be overridden independently when required.

## RF history

```env
SMA_RF_RETENTION_DAYS=90
SMA_RF_SUMMARY_WINDOW_HOURS=24
SMA_RF_STABLE_SLOPE_DB_PER_HOUR=0.5
```

## Retry

```env
SMA_RETRY_MAX_ATTEMPTS=20
SMA_RETRY_BASE_DELAY=1
SMA_RETRY_MAX_DELAY=5
SMA_RETRY_POLL_INTERVAL=0.5
SMA_RETRY_BATCH_SIZE=50
```

## Presence

Global fallback:

```env
SMA_DEVICE_STALE_AFTER=300
SMA_DEVICE_OFFLINE_AFTER=1800
```

Optional per-family JSON:

```env
SMA_DEVICE_PRESENCE_PROFILES_JSON={"teros12":{"expected_interval_seconds":600,"stale_after_seconds":900,"offline_after_seconds":1800}}
```

## Irrigap catalog

Default file:

```text
config/irrigap.nodes.json
```

Override:

```env
# SMA_IRRIGAP_NODES_FILE=/absolute/path/to/nodes.json
```

An inline JSON catalog is also supported by the canonical runner.

---

# Management API

Default bind:

```text
http://127.0.0.1:8082
```

Core routes:

```text
GET  /health
GET  /ready
GET  /metrics
GET  /api/v2/status
GET  /api/v2/devices
GET  /api/v2/devices/{external_id}/messages
GET  /api/v2/devices/{external_id}/context
GET  /api/v2/devices/{external_id}/rf
GET  /api/v2/gateways
GET  /api/v2/gateways/{gateway_id}
GET  /api/v2/retry
GET  /api/v2/dlq
POST /api/v2/reconcile
POST /api/v2/dlq/{id}/retry
GET  /api/v2/catalog/devices
GET  /api/v2/catalog/devices/{node_id}
POST /api/v2/catalog/devices
PUT  /api/v2/catalog/devices/{node_id}
DELETE /api/v2/catalog/devices/{node_id}
POST /api/v2/catalog/devices/{node_id}/decommission
POST /api/v2/catalog/devices/{node_id}/reactivate
```

Example:

```bash
curl -s \
  -H 'Authorization: Bearer <SMA_API_TOKEN>' \
  http://127.0.0.1:8082/api/v2/status | jq
```

---

# Observability

Prometheus metrics are exposed at `/metrics` when the API server is running.

Examples include pipeline/retry/DLQ/presence/quality/cache metrics plus v2 reliability/RF indicators such as:

```text
sma_events_ingressed_total
sma_ingress_queue_size
sma_durable_ingress
```

Gateway status, decoded GatewayStats and RF history are also available through the management API.

---

# Passive field capture

For protocol investigation without modifying remote state:

```bash
source .venv/bin/activate
python scripts/capture_irrigap_samples.py
```

Useful examples:

```bash
python scripts/capture_irrigap_samples.py --device teros12-sector1.3
python scripts/capture_irrigap_samples.py --all-topics
python scripts/capture_irrigap_samples.py --duration 3600
python scripts/capture_irrigap_samples.py --max-messages 100
```

Capture files are stored as JSONL under `captures/` and should remain outside Git because they can contain deployment/device identifiers.

---

# Tests

Run the complete unit/integration-style local suite with:

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```

Field validation should additionally verify:

```text
sensor packet received
-> ingressed
-> processed
-> ingress queue returns to zero
-> SenML published
-> Timescale row visible
-> device state updated
-> gateway/RF observation updated when applicable
```

For reliability testing, deliberately exercise broker/Atom/Reader outages and process restarts before treating a deployment as production hardened.

---

# Persistent state

The current v2 local state is SQLite. It stores operational metadata such as:

- device mappings;
- lifecycle/catalog bindings;
- observation metadata;
- quality snapshots and quality by role;
- ingress queue;
- retry queue;
- dead letters;
- gateway state;
- gateway decoded stats snapshots;
- sensor-to-gateway topology;
- RF link samples.

The state DB path is configurable with `SMA_STATE_DB`.

Telemetry history itself belongs in Magistrala/Timescale; SQLite is not intended to replace the telemetry database.

---

# Adding a new parser plugin

A new native parser should implement the same conceptual contract used by the current v2 parser registry:

```text
supports(RawEvent) -> bool
parse(RawEvent) -> ParsedEvent | None
```

A `ParsedEvent` should provide:

```text
external_device_id
measurements[]
metadata{}
```

Recommended metadata when available:

```text
sensor
node_id
location
sub_location
depth
application_id
f_port
message_role
port_role
transport
source
topic
gateway_rx
radio_tx
```

Keep protocol interpretation inside the parser. Do not make parser code create Atom entities, publish SenML or manipulate policies.

When a firmware/protocol revision changes the meaning of the source fields, prefer a new/versioned parser instead of changing old semantics invisibly.

---

# Known limitations / current hardening backlog

The current version is intentionally conservative about claims of delivery and RF health.

Known items that can be improved incrementally:

- end-to-end idempotency after an accepted publish;
- richer DLQ diagnostic context;
- stronger dependency/readiness breakdown;
- service-manager packaging such as systemd/container deployment templates;
- TLS/authentication hardening according to each deployment;
- additional native parsers replacing the legacy compatibility bridge over time;
- validated Teros-specific moisture calibration for the current ChirpStack raw representation;
- more advanced RF link-margin/headroom analysis once enough comparable radio-profile history is accumulated.

None of these require changing the central plugin -> quality -> control-plane -> SenML -> publish architecture.

---

# Repository history

Before SMA 2.0 was merged into `main`, the previous main-line implementation was preserved in:

```text
legacy/pre-smarter-adapter-v2
```

This branch is intended as a historical recovery/reference point while `main` tracks the current SMA 2.x implementation.

---

## License / project policy

Follow the repository license and deployment policies applicable to your environment. Do not commit broker passwords, Atom credentials, production captures or other field secrets.
