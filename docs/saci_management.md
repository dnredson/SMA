# SACI management layer (separate from SMA)

The SMA Pitaya parser forwards numeric S1–S6 and T1–T3 independently, except protocol sentinel 99. It does not infer physical connectivity, farm sectors, NSP, calibration, or which T is currently used. Values of 0 and negative T values remain raw. BAT/relay are not part of this parser version.

`saci_management.sql` creates a gateway inventory, time-effective board assignments, channel usage, and a read-only labeled view over the existing Timescale messages. It never deletes/re-writes telemetry. No farm-specific rules are embedded in the adapter.

Apply (after reviewing schema and taking normal database backups):
```bash
sudo docker exec -i magistrala-timescale psql -v ON_ERROR_STOP=1 -U magistrala -d magistrala < docs/saci_management.sql
```

Example, only after verifying physical installation dates: ID01 slot s01 has S3 and T3 relevant. Do not invent dates; substitute the true effective time:

```sql
INSERT INTO saci_management.gateways(gateway_key,device_id,farm_code)
VALUES ('NSAAB','PITAYA_NSAAB','NSAAB')
ON CONFLICT (gateway_key) DO NOTHING;

WITH added AS (
 INSERT INTO saci_management.board_assignments
 (gateway_key,slot,reported_board_id,sector_label,valid_from,notes)
 VALUES ('NSAAB',1,1,NULL,TIMESTAMPTZ '2026-10-09 00:00:00+00','Example only: confirm installation date')
 RETURNING assignment_id
)
INSERT INTO saci_management.channel_assignments(assignment_id,field_name,usage)
SELECT added.assignment_id, f.field_name, 'agronomic'
FROM added CROSS JOIN (VALUES ('s3'),('t3')) AS f(field_name);
```

Never run that example without confirming the installation start timestamp and association. To display configured agronomic channels in Grafana:

```sql
SELECT measured_at AS "time",
       COALESCE(sector_label, 'ID' || lpad(reported_board_id::text,2,'0')) || ' · ' || upper(field_name) AS metric,
       value
FROM saci_management.telemetry_labeled
WHERE device_id = 'PITAYA_NSAAB'
  AND usage = 'agronomic'
  AND time >= $__unixEpochFrom() * 1000000000::bigint
  AND time <= $__unixEpochTo() * 1000000000::bigint
ORDER BY "time";
```

The view is intentionally not a history backfill: missing T in prior MQTT ingestion remains absent. The dashboard should use source T only and label any future derived value separately.
