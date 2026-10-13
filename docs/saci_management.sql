-- SACI management layer (PostgreSQL/TimescaleDB).
-- Independent of SMA data ingestion. Run in magistrala database.
-- No deletion/update of messages or automated classification of unknown channels.
BEGIN;
CREATE SCHEMA IF NOT EXISTS saci_management;
CREATE TABLE IF NOT EXISTS saci_management.gateways (
    gateway_key text PRIMARY KEY,
    device_id text NOT NULL UNIQUE,
    description text,
    farm_code text,
    central_label text
);
CREATE TABLE IF NOT EXISTS saci_management.board_assignments (
    assignment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gateway_key text NOT NULL REFERENCES saci_management.gateways(gateway_key),
    slot integer NOT NULL CHECK (slot BETWEEN 1 AND 50),
    reported_board_id integer NOT NULL CHECK (reported_board_id BETWEEN 0 AND 99),
    sector_label text,
    nsp integer CHECK (nsp BETWEEN 0 AND 6),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    notes text,
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE TABLE IF NOT EXISTS saci_management.channel_assignments (
    assignment_id bigint NOT NULL REFERENCES saci_management.board_assignments(assignment_id) ON DELETE CASCADE,
    field_name text NOT NULL CHECK (field_name ~ '^(s[1-6]|t[1-3])$'),
    usage text NOT NULL CHECK (usage IN ('agronomic','diagnostic','ignore')),
    description text,
    PRIMARY KEY (assignment_id, field_name)
);
CREATE INDEX IF NOT EXISTS saci_board_time_idx
    ON saci_management.board_assignments(gateway_key, slot, valid_from, valid_to);

-- A view only: data stays raw in public.messages.
-- Slot AND reported board ID are required to avoid accidental ID collisions.
-- Null usage = not yet mapped; dashboard can select explicitly usage='agronomic'.
CREATE OR REPLACE VIEW saci_management.telemetry_labeled AS
SELECT m.time,
       to_timestamp(m.time / 1000000000.0) AS measured_at,
       m.device_id,
       m.name,
       m.value,
       substring(m.name from 'sensorboard[.]s([0-9]{2})[.]')::integer AS slot,
       substring(m.name from '[.]id([0-9]{2})[.]')::integer AS reported_board_id,
       substring(m.name from '[.]([st][1-6])[.]raw$') AS field_name,
       a.assignment_id,
       a.sector_label,
       a.nsp,
       c.usage,
       c.description AS channel_description
FROM public.messages m
LEFT JOIN saci_management.gateways g ON g.device_id = m.device_id
LEFT JOIN LATERAL (
  SELECT b.*
  FROM saci_management.board_assignments b
  WHERE b.gateway_key = g.gateway_key
    AND b.slot = substring(m.name from 'sensorboard[.]s([0-9]{2})[.]')::integer
    AND b.reported_board_id = substring(m.name from '[.]id([0-9]{2})[.]')::integer
    AND to_timestamp(m.time / 1000000000.0) >= b.valid_from
    AND (b.valid_to IS NULL OR to_timestamp(m.time / 1000000000.0) < b.valid_to)
  ORDER BY b.valid_from DESC, b.assignment_id DESC
  LIMIT 1
) a ON true
LEFT JOIN saci_management.channel_assignments c
  ON c.assignment_id = a.assignment_id
 AND c.field_name = substring(m.name from '[.]([st][1-6])[.]raw$')
WHERE m.name ~ '^.+:sensorboard[.]s[0-9]{2}[.]id[0-9]{2}[.](s[1-6]|t[1-3])[.]raw$';
COMMIT;
