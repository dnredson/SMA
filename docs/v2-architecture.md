# Smarter Adapter 2.0 architecture

Smarter Adapter 2.0 is the next major version of SmartAdapter. The repository remains `SMA`; the new implementation is developed alongside the current implementation until the v2 end-to-end path is proven.

## Goal

The adapter must operate as an autonomous integration layer between heterogeneous IoT sources and Magistrala/Atom. A human should not need to open the Magistrala UI to provision routine resources or verify normal operation.

## Core responsibilities

1. Run one or more input plugins (MQTT first; HTTP/serial/other transports can follow).
2. Detect and parse multiple payload formats through parser plugins.
3. Normalize accepted measurements into a transport-independent internal event and then SenML.
4. Preserve transport provenance (for example MQTT topic, ChirpStack fPort and application id) without turning transport metadata into permanent sensor identity.
5. Support multiple logical message roles from the same physical sensor (for example soil telemetry and battery telemetry) and keep data-quality state per role.
6. Ensure the Magistrala control plane exists: workspace, channel, sensor device, policies and rules.
7. Discover LoRaWAN gateways, represent them as Atom entities, and derive gateway liveness from periodic gateway traffic.
8. Persist observed sensor-to-gateway reception edges (including RSSI/SNR/channel where available) without assuming a sensor belongs permanently to one gateway.
9. Create an Atom sensor device only when it is first discovered; later observations reconcile/update the existing device.
10. Centralize authentication/token acquisition, renewal and retry after authentication failures.
11. Publish telemetry through the current FluxMQ HTTP ingress.
12. Persist device mappings and reconciliation state so normal packets do not require GraphQL lookups.
13. Retry transient delivery failures and move terminal/exhausted failures to a persistent DLQ while retaining the original raw event.
14. Expose health, status, CRUD, gateway presence/topology and manual reconciliation through the adapter API.

## Architectural boundaries

```text
Sensor MQTT input plugins
    |
    v
RawEvent
    |
    v
Parser plugins ----> ParsedEvent
                         |
                +--------+---------+
                |                  |
                v                  v
        Device Registry       Transport metadata
          /         \          topic/fPort/rxInfo
         v           v                |
   State Store    Atom control plane   |
         |        workspace/channel/   |
         |        sensor/policies      |
         |                             |
         +-------------+---------------+
                       |
                       v
                 SenML Normalizer
                       |
                       v
                    Publisher ------> Magistrala / FluxMQ
                       |
                +------+------+
                |             |
              success      transient failure
                |             |
               ACK        Retry Queue
                              |
                        exhausted/permanent
                              |
                              v
                             DLQ

Gateway MQTT side input (read-only)
    |
    +--> event/stats --------> Gateway Registry ----> Gateway presence
    |                              |                      |
    |                              +----> Atom gateway entity
    |
    +--> state/conn ---------> discovery/connection evidence

ChirpStack ParsedEvent.rxInfo
    |
    +--> sensor --observed_by--> gateway
         (RSSI/SNR/channel/last_seen/packet_count)
```

## Invariants

- Input plugins know transports, not Magistrala.
- Parser plugins interpret payloads and do not perform network I/O.
- Payload keys are authoritative for message semantics; fPort is preserved as provenance/role hint and must not be used as a positional decoder shortcut.
- Transport metadata such as MQTT topic/fPort belongs to observations, not permanent device identity.
- A physical device may have multiple logical message roles; a valid role must not erase an invalid quality snapshot from another role.
- The device registry is the only component that decides whether a sensor device must be created, recovered, updated or reconciled.
- Gateway entities are observational infrastructure objects and do not receive sensor channel publish permission.
- Gateway liveness is derived from recent non-retained activity; a retained connection-state message can discover a gateway but cannot by itself prove it is currently online.
- Sensor-to-gateway relationships are observations, not ownership. Multiple gateways may hear the same uplink.
- Authentication state belongs to a TokenManager, not to individual plugins or devices.
- Device creation is idempotent from the adapter point of view: resolve local mapping, recover from Atom when possible, create only when absent.
- Normal telemetry on a known sensor follows the fast path from local state to publish without a GraphQL request per packet.
- The CLI is a diagnostic/reference tool. Runtime integration uses the public HTTP/GraphQL APIs directly.
- DLQ records retain the original event and failure metadata; richer parsed/normalized context may be added without weakening raw-event retention.
- Existing v1 sensor parsers are preserved during migration through an explicit compatibility plugin.

## ChirpStack role-aware parsing

The current field deployment uses one physical sensor entity with multiple frame roles. SMA therefore keeps both the semantic role inferred from payload keys and the transport role hint inferred from the fPort/deployment metadata.

Example:

```text
fPort 1
  VB -> battery.voltage
  BT -> battery.level

fPort 31 (and deployment depth ports such as 32/33)
  M  -> soil raw moisture
  T  -> soil temperature
  C  -> soil electrical conductivity/raw EC
```

A role/port mismatch is diagnostic metadata rather than an automatic positional reinterpretation. This keeps the adapter resilient if firmware changes the port assignment in the future.

Greenstick-specific calibration is only applied to Greenstick nodes. Teros12 moisture is retained as raw telemetry until a Teros-specific conversion is explicitly validated.

## Gateway model

A discovered LoRaWAN gateway is represented locally and in Atom using a dedicated `smarter-adapter-lorawan-gateway` profile. The local state includes:

- gateway id
- Atom entity id
- topic root/region prefix
- first/last observation
- last stats/uplink/connection observation
- message counters
- derived online/stale/offline status
- last reconciliation error

For the current Irrigap deployment, observed gateway stats traffic is approximately every 30 seconds. The initial policy is therefore:

```text
expected interval: 30 s
stale after:       90 s
offline after:    180 s
```

All thresholds remain runtime-configurable.

ChirpStack application uplinks can contain `rxInfo` for one or more gateways. SMA records each reception as an edge between the sensor and gateway with last RSSI/SNR/channel/RF-chain/CRC state and packet count. This allows operational reasoning such as distinguishing a sensor outage from a gateway outage and, later, detecting progressive link degradation.

## Magistrala/Atom model

The v2 control plane treats the former Magistrala domain concept as an Atom-backed workspace/tenant. Sensors and observed gateways are Atom entities, channels are Atom resources, and sensor publish permission is reconciled as policy state. Gateway entities deliberately remain outside the sensor publish policy because they are topology/health objects rather than telemetry publishers into the normalized sensor channel.

The adapter should eventually be able to start against an empty test deployment and converge the required workspace/channel/sensor/gateway state without UI interaction.

## Migration strategy

The v1 code remains available while the v2 package grows under `src/smarter_adapter`. Migration is contract-first: existing parser behavior is wrapped rather than immediately rewritten. Once the new pipeline, control plane, reliability layer and E2E test are complete, v2 becomes the default entry point and the compatibility layer can be reduced deliberately.

## Milestones

- **2.0-A Foundation:** event models, plugin contracts, parser registry, compatibility wrapper and tests.
- **2.0-B Control plane:** TokenManager, Atom client, workspace/channel/profile/device/policy reconciliation.
- **2.0-C Inputs:** multi-source MQTT plugin and input lifecycle.
- **2.0-D Pipeline:** RawEvent -> ParsedEvent -> SenML -> publish.
- **2.0-E Reliability:** persistent retry queue, DLQ, restart recovery, 401/403-aware repair.
- **2.0-F Management:** CRUD/status/reconcile API, rules management and metrics.
- **2.0-G E2E:** empty Magistrala -> auto-bootstrap -> auto-device -> SenML -> FluxMQ -> Timescale verification.
- **2.0-H Topology:** message-role provenance, gateway entities/presence, sensor-to-gateway radio observations and topology-aware context.
