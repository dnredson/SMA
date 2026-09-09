# SmartAdapter (SMA)

SmartAdapter is the ingestion and normalization component of the IoT testbed. It receives sensor messages in the formats already supported by the project, identifies the corresponding device in Magistrala Atom, converts measurements to canonical SenML JSON, and publishes telemetry through the current FluxMQ HTTP route.

The adapter also owns the device lifecycle required by the ingestion path and exposes an HTTP API for explicit device CRUD operations.

## Architecture and message flow

The runtime follows this pipeline:

1. MQTT receives a sensor message.
2. The parser dispatcher normalizes the topic and selects the parser for the sensor family.
3. The selected parser converts the payload into normalized measurement entries and metadata.
4. The Atom-backed registry finds or creates the device using the normalized external ID.
5. When enabled, the registry ensures that the Atom device has permission to publish to the configured channel.
6. The SenML boundary removes parser-specific base fields and creates one canonical SenML batch.
7. The publisher sends the batch to FluxMQ through the modern HTTP endpoint.
8. The local state stores the Atom device ID and lifecycle metadata, including the last-seen timestamp.

~~~text
sensor MQTT message
        |
        v
topic normalization and parser dispatch
        |
        v
normalized measurements and metadata
        |
        v
Atom device lookup/create + publish policy
        |
        v
canonical SenML JSON
        |
        v
FluxMQ HTTP API -> Magistrala channel
~~~

The adapter does not store device secrets. Its local state is only a cache and lifecycle index that maps an external sensor identifier to the corresponding Atom device.

## Supported input formats

The existing parser families are preserved:

| Sensor family | Accepted input |
| --- | --- |
| WXT520 | CSV and SDI-12 |
| ATMOS41 | SDI-12 |
| TEROS12 | SDI-12 |
| GreenStick | direct Ultra-Light payload |
| GreenStick | ChirpStack/TTN JSON envelope containing Ultra-Light |
| TTN | JSON envelope containing the supported Ultra-Light payload |

The dispatcher normalizes topics before routing. It accepts the existing sensor prefixes, including the historical GreenStick spelling variants, and uses the normalized topic as the default external device identifier.

Parsers describe measurements only. They do not own the SenML batch header.

## Canonical SenML output

All accepted measurements are converted to a SenML JSON array. The common base name and base time are assigned at one boundary so that parser-specific fields cannot leak into the transport layer.

Example:

~~~json
[
  {
    "bn": "ATMOS41_WS_TEST:",
    "bt": 1725882000,
    "n": "air.temperature",
    "u": "Cel",
    "v": 25
  },
  {
    "n": "rel.humidity",
    "u": "1",
    "v": 0.5
  }
]
~~~

The first record owns the base name and base time. Subsequent records contain only their individual measurement fields. If a parser produces no valid measurement, the adapter still emits a valid base SenML record.

## Magistrala Atom integration

The previous users/domains/clients management flow and the legacy Magistrala CLI are no longer used.

The adapter uses Atom through:

- GraphQL for device/entity lifecycle operations;
- Atom bearer authentication;
- the FluxMQ HTTP publish route for telemetry.

The adapter represents a sensor as an Atom entity with:

- kind: device;
- externalId: the normalized sensor identifier;
- name: the configured or inferred device name;
- tenantId: the configured Atom tenant;
- attributes: sensor metadata and application-specific properties.

### Device discovery and automatic provisioning

For every accepted message, the registry:

1. checks the local mapping;
2. validates the mapped Atom device when a local mapping exists;
3. searches Atom by tenant and externalId when necessary;
4. creates the Atom device when it does not exist;
5. ensures the publish policy when policy management is enabled;
6. persists the Atom ID and lifecycle metadata locally.

This makes the first observation of a new sensor sufficient to provision its Atom device and prepare it for telemetry publication.

### Telemetry publication

Telemetry is sent to FluxMQ using:

~~~text
POST /<tenant_id>/channels/<channel_id>/messages
Authorization: Bearer <ATOM_SERVICE_TOKEN>
Content-Type: application/json
~~~

The request body is:

~~~json
{
  "device_id": "<atom-device-id>",
  "subtopic": "",
  "payload": [
    {
      "bn": "DEVICE:",
      "bt": 1725882000,
      "n": "air.temperature",
      "u": "Cel",
      "v": 25
    }
  ]
}
~~~

The device identity is carried in device_id, while the normalized sensor data remains in the SenML payload.

## Device lifecycle and CRUD API

The registry exposes Atom-backed operations for:

- listing devices;
- retrieving a device;
- creating a device;
- updating a device;
- deleting a device.

The HTTP API is enabled by the adapter process and listens on 127.0.0.1:8081 by default.

~~~text
GET    /health
GET    /api/v1/devices
POST   /api/v1/devices
GET    /api/v1/devices/<atom-device-id>
PUT    /api/v1/devices/<atom-device-id>
PATCH  /api/v1/devices/<atom-device-id>
DELETE /api/v1/devices/<atom-device-id>
~~~

The health endpoint does not require authentication. CRUD endpoints require the configured bearer token when api_token or ADAPTER_API_TOKEN is set.

### List devices

~~~bash
curl -fsS \
  -H "Authorization: Bearer $ADAPTER_API_TOKEN" \
  "http://127.0.0.1:8081/api/v1/devices?tenant_id=$ATOM_TENANT_ID"
~~~

### Create a device

~~~bash
curl -fsS -X POST \
  -H "Authorization: Bearer $ADAPTER_API_TOKEN" \
  -H "Content-Type: application/json" \
  http://127.0.0.1:8081/api/v1/devices \
  --data-raw '{
    "external_id": "GREENSTICK_1_TEST",
    "name": "GreenStick 1",
    "tenant_id": "replace-with-atom-tenant-id",
    "attributes": {
      "sensor": "greenstick"
    },
    "ensure_publish": true
  }'
~~~

### Update a device

PUT and PATCH accept the lifecycle fields name, external_id, status, and attributes.

By default, attributes are merged with the current Atom attributes. Set merge_attributes to false when a complete replacement is required.

~~~bash
curl -fsS -X PATCH \
  -H "Authorization: Bearer $ADAPTER_API_TOKEN" \
  -H "Content-Type: application/json" \
  http://127.0.0.1:8081/api/v1/devices/<atom-device-id> \
  --data-raw '{
    "status": "active",
    "attributes": {
      "location": "field-a"
    },
    "merge_attributes": true
  }'
~~~

### Delete a device

~~~bash
curl -fsS -X DELETE \
  -H "Authorization: Bearer $ADAPTER_API_TOKEN" \
  http://127.0.0.1:8081/api/v1/devices/<atom-device-id>
~~~

Deleting a device removes it from Atom and removes the corresponding local external-ID mapping.

## Configuration

Start from config.example.toml and keep credentials in environment variables whenever possible.

The most important settings are:

~~~toml
atom_url = "http://localhost:8080"
atom_graphql_url = "http://localhost:8080/graphql"
atom_tenant_id = "replace-with-atom-tenant-id"
atom_channel_id = "replace-with-atom-channel-id"
atom_manage_policies = true
atom_timeout_ms = 10000

http_adapter_url = "http://localhost:8008"
publish_timeout_ms = 5000

mqtt_address = "tcp://127.0.0.1:1883"
mqtt_topics = ["#"]

api_host = "127.0.0.1"
api_port = 8081
api_token = ""
~~~

Supported credential environment variables include:

- ATOM_SERVICE_TOKEN;
- ATOM_ADMIN_TOKEN;
- ATOM_TOKEN;
- ATOM_USERNAME and ATOM_PASSWORD for the configured Atom login flow;
- ATOM_URL and ATOM_GRAPHQL_URL;
- ATOM_TENANT_ID and ATOM_CHANNEL_ID;
- ADAPTER_API_TOKEN for protecting the CRUD API;
- ADAPTER_CONFIG for selecting a configuration file other than ./config.toml.

When atom_manage_policies is true, the adapter attempts to ensure that each device can publish to the configured channel. Set it to false when policy provisioning is managed externally.

The quality subsystem can validate numeric measurements against quality_thresholds_json and publish threshold violation alerts through MQTT. The quality and alert settings can be reloaded partially with SIGHUP.

## Running the adapter

Install the Python dependency:

~~~bash
python -m pip install -r requirements.txt
~~~

Configure the Atom and MQTT values, then start the adapter:

~~~bash
python src/main.py
~~~

The adapter starts the CRUD API, connects to the configured MQTT broker, and begins processing accepted messages.

## Tests

Run the complete unit-test suite with:

~~~bash
python -m unittest discover -s tests -v
~~~

The tests cover:

- compatibility of the existing parser families;
- GreenStick and TTN normalization;
- canonical SenML base name and time handling;
- Atom GraphQL authentication and entity fields;
- the modern FluxMQ HTTP route and JSON envelope;
- authenticated CRUD API behavior.

## Repository structure

~~~text
src/
  main.py                    MQTT runtime and end-to-end pipeline
  core/atom_client.py        Atom GraphQL/login client
  core/registry.py           Atom device lifecycle and local mapping
  core/publisher.py          FluxMQ HTTP publisher
  core/senml.py              canonical SenML boundary
  core/api.py                authenticated device CRUD API
  parsers/                   sensor-family parsers
tests/                       parser, transport, SenML, and API tests
config.example.toml          configuration template
~~~

## Scope

This repository contains the SmartAdapter only. The current migration does not modify the cloud deployment or DATUM components. Those components can consume the adapter's canonical SenML output and device lifecycle behavior independently.
