-- Repair unsigned.redacted_because rows stored in the old internal-wrapper shape.
--
-- Before the fix in c517ca22, MaybeRedactEvent stored the internal *types.Event
-- wrapper, so redacted_because looked like
--     {"PDU": {...redaction event...}, "EventNID": 123, "Rejected": false}
-- with no top-level `content`. Clients that read
-- unsigned.redacted_because.content.reason (e.g. Cinny) crash on those events.
-- Already-redacted rows are not rewritten by the code, so they need this repair.
--
-- This script rewrites every affected row in roomserver_event_json and
-- syncapi_output_room_events into the client-event shape the code now stores:
--     {type, sender, content, origin_server_ts, redacts, event_id[, unsigned]}
-- The redaction's event_id is taken from unsigned.redacted_by (the wrapper's
-- EventNID is the redaction's NID, not the redacted event's, so it is not used).
--
-- Usage: connect interactively to the dendrite database, then run:
--     \set ON_ERROR_STOP on
--     \i scripts/repair_redacted_because_wrapper.sql
-- The script leaves the transaction OPEN. Keep this psql session open, inspect
-- the counts and verify rows, then type COMMIT; or ROLLBACK;. Restart Dendrite
-- afterwards so caches are dropped. Drop the zz_bak_* tables once satisfied.
--
-- Idempotent: only rows that still contain the wrapper are touched. The backup
-- tables are created WITHOUT "IF NOT EXISTS" on purpose, so a stale backup from
-- an earlier run makes the script fail instead of being silently reused.
-- Drop or rename zz_bak_* tables from a previous run first.

\set ON_ERROR_STOP on

BEGIN;

\echo '--- rows to repair (before) ---'
SELECT 'roomserver' AS src, count(*) AS wrapped
FROM roomserver_event_json
WHERE event_json LIKE '%redacted_because%'
  AND (event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
UNION ALL
SELECT 'syncapi', count(*)
FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU';

-- Backups of exactly the rows that will change.
CREATE TABLE zz_bak_rs_redacted_because AS
SELECT j.event_nid, e.event_id, j.event_json
FROM roomserver_event_json j
JOIN roomserver_events e ON e.event_nid = j.event_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU';

CREATE TABLE zz_bak_sync_redacted_because AS
SELECT id, event_id, headered_event_json
FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU';

-- roomserver_event_json
WITH src AS (
  SELECT j.event_nid,
         j.event_json::jsonb AS doc,
         j.event_json::jsonb #> '{unsigned,redacted_because,PDU}' AS rb
  FROM roomserver_event_json j
  WHERE j.event_json LIKE '%redacted_because%'
    AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
    AND j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
)
UPDATE roomserver_event_json j
SET event_json = jsonb_set(
      src.doc,
      '{unsigned,redacted_because}',
      jsonb_strip_nulls(jsonb_build_object(
        'type',             src.rb -> 'type',
        'sender',           src.rb -> 'sender',
        'content',          src.rb -> 'content',
        'origin_server_ts', src.rb -> 'origin_server_ts',
        'redacts',          src.rb -> 'redacts',
        'unsigned',         src.rb -> 'unsigned',
        'event_id',         src.doc #>> '{unsigned,redacted_by}'
      ))
    )::text
FROM src
WHERE j.event_nid = src.event_nid;

-- syncapi_output_room_events
WITH src AS (
  SELECT s.id,
         s.headered_event_json::jsonb AS doc,
         s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU}' AS rb
  FROM syncapi_output_room_events s
  WHERE s.headered_event_json LIKE '%redacted_because%'
    AND (s.headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
    AND s.headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
)
UPDATE syncapi_output_room_events s
SET headered_event_json = jsonb_set(
      src.doc,
      '{unsigned,redacted_because}',
      jsonb_strip_nulls(jsonb_build_object(
        'type',             src.rb -> 'type',
        'sender',           src.rb -> 'sender',
        'content',          src.rb -> 'content',
        'origin_server_ts', src.rb -> 'origin_server_ts',
        'redacts',          src.rb -> 'redacts',
        'unsigned',         src.rb -> 'unsigned',
        'event_id',         src.doc #>> '{unsigned,redacted_by}'
      ))
    )::text
FROM src
WHERE s.id = src.id;

\echo '--- verify: expect no wrapped rows and no id mismatches ---'
SELECT 'roomserver' AS src,
       count(*) FILTER (WHERE (event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU') AS still_wrapped,
       count(*) FILTER (WHERE event_json::jsonb #>> '{unsigned,redacted_because,event_id}'
                              IS DISTINCT FROM event_json::jsonb #>> '{unsigned,redacted_by}') AS id_mismatch
FROM roomserver_event_json
WHERE event_nid IN (SELECT event_nid FROM zz_bak_rs_redacted_because)
UNION ALL
SELECT 'syncapi',
       count(*) FILTER (WHERE (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'),
       count(*) FILTER (WHERE headered_event_json::jsonb #>> '{unsigned,redacted_because,event_id}'
                              IS DISTINCT FROM headered_event_json::jsonb #>> '{unsigned,redacted_by}')
FROM syncapi_output_room_events
WHERE id IN (SELECT id FROM zz_bak_sync_redacted_because);

\echo 'If both rows show still_wrapped = 0 and id_mismatch = 0: COMMIT;  otherwise: ROLLBACK;'
-- The transaction is intentionally left open here.
