# Smarter Adapter 2.0 architecture

Smarter Adapter 2.0 is the next major version of SmartAdapter. The repository remains `SMA`; the new implementation is developed alongside the current implementation until the v2 end-to-end path is proven.

## Goal

The adapter must operate as an autonomous integration layer between heterogeneous IoT sources and Magistrala/Atom. A human should not need to open the Magistrala UI to provision routine resources or verify normal operation.

## Core responsibilities

1. Run one or more input plugins (MQTT first; HTTP/serial/other transports can follow).
2. Detect and parse multiple payload formats through parser plugins.
3. Normalize accepted measurements into a transport-independent internal event and then SenML.
4. Ensure the Magistrala control plane exists: workspace, channel, device, policies and eventually rules.
5. Create an Atom device only when it is first discovered; later observations reconcile/update the existing device.
6. Centralize authentication/token acquisition, renewal and retry after authentication failures.
7. Publish telemetry through the current FluxMQ HTTP ingress.
8. Persist device mappings and reconciliation state so normal packets do not require GraphQL lookups.
9. Retry transient delivery failures and move terminal/exhausted failures to a persistent DLQ while retaining the original raw event.
10. Expose health, status, CRUD and manual reconciliation through the adapter API.

## Architectural boundaries

```text
Input plugins
    |
    v
RawEvent
    |
    v
Parser plugins ----> ParsedEvent
                         |
                         v
                  Device Registry
                    /         \
                   v           v
             State Store    Atom control plane
                   |        workspace/channel/
                   |        device/policies
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
```

## Invariants

- Input plugins know transports, not Magistrala.
- Parser plugins interpret payloads and do not perform network I/O.
- The device registry is the only component that decides whether a device must be created, recovered, updated or reconciled.
- Authentication state belongs to a TokenManager, not to individual plugins or devices.
- Device creation is idempotent from the adapter point of view: resolve local mapping, recover from Atom when possible, create only when absent.
- Normal telemetry on a known device follows the fast path from local state to publish without a GraphQL request per packet.
- The CLI is a diagnostic/reference tool. Runtime integration uses the public HTTP/GraphQL APIs directly.
- DLQ records retain the original event plus parsed/normalized context and failure metadata.
- Existing v1 sensor parsers are preserved during migration through an explicit compatibility plugin.

## Magistrala/Atom model

The v2 control plane treats the former Magistrala domain concept as an Atom-backed workspace/tenant. Devices are Atom entities, channels are Atom resources, and publish permission is reconciled as policy state. The adapter should eventually be able to start against an empty test deployment and converge the required workspace/channel/device state without UI interaction.

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
