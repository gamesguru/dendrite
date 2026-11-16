-- Repair unsigned.redacted_because rows stored in the old internal-wrapper shape.
--
-- Before the fix in c517ca22, MaybeRedactEvent stored the internal *types.Event
-- wrapper, so redacted_because looked like
--     {"PDU": {...redaction event...}, "EventNID": 123, "Rejected": false}
-- with no top-level `content`. Clients that read
-- unsigned.redacted_because.content.reason (e.g. Cinny) crash on those events.
-- Already-redacted rows are not rewritten by the code, so they need this repair.
--
-- This script rewrites every affected row in roomserver_event_json,
-- syncapi_output_room_events and syncapi_current_room_state into the client-event shape the code now stores
-- in rooms with ordinary sender IDs:
--     {type, sender, content, origin_server_ts, redacts, event_id[, unsigned]}
-- Limitation: the redaction's sender is copied as stored. In pseudo-ID rooms the
-- client format maps it to a user ID and adds sender_key. This SQL script cannot
-- perform that roomserver mapping. Pseudo-ID rows, syncapi rows whose rooms are
-- absent from roomserver_rooms, and rows without unsigned.redacted_by remain
-- unrepaired and require a separate follow-up.
-- The script requires the roomserver and syncapi tables in one PostgreSQL
-- database: it reads roomserver_rooms to find pseudo-ID rooms for the syncapi
-- rows. Split-database deployments are not supported by this script.
-- The redaction's event_id is taken from unsigned.redacted_by (the wrapper's
-- EventNID is the redaction's NID, not the redacted event's, so it is not used).
--
-- Usage: connect interactively to the dendrite database, then run:
--     \set ON_ERROR_STOP on
--     \i scripts/repair_redacted_because_wrapper.sql
-- Run it with \i from that interactive session, not with psql -f (which exits and
-- rolls back at the end instead of leaving the transaction open for inspection),
-- and not by pasting statements. The ON_ERROR_STOP setting below is required for
-- the DO-block checks to stop execution.
-- The script leaves the transaction OPEN. Keep this psql session open, inspect
-- the counts and verify rows, then type COMMIT; or ROLLBACK;. Restart Dendrite
-- afterwards so caches are dropped. Drop the zz_bak_* tables once satisfied;
-- zz_pseudo_rooms and zz_repairable_rooms are temporary and are dropped at
-- COMMIT or ROLLBACK.
--
-- Idempotent: only rows that still contain the wrapper are touched. The backup
-- tables are created WITHOUT "IF NOT EXISTS" on purpose, so a stale backup from
-- an earlier run makes the script fail instead of being silently reused.
-- Drop or rename zz_bak_* tables from a previous run first.

\set ON_ERROR_STOP on
-- An interactive session with ON_ERROR_ROLLBACK on would wrap each statement in
-- an implicit savepoint, so a failed check would undo only that statement and
-- leave the transaction open to COMMIT. Keep it off so a failure aborts it all.
\set ON_ERROR_ROLLBACK off

-- REPEATABLE READ: every backup and UPDATE sees the same snapshot, and a row the
-- live server changes meanwhile makes the UPDATE fail ("could not serialize")
-- instead of being overwritten; ROLLBACK and re-run in that case. It takes no
-- table lock, but each UPDATE holds a row lock until COMMIT or ROLLBACK, so a
-- live redaction or event rewrite of a repaired row waits while this session
-- stays open. Stop Dendrite for the run, or keep the inspection window short.
BEGIN ISOLATION LEVEL REPEATABLE READ;

CREATE TEMP TABLE zz_pseudo_rooms ON COMMIT DROP AS
SELECT room_id FROM roomserver_rooms WHERE room_version = 'org.matrix.msc4014';

-- Room versions 11 and later carry the redacted event ID in content.redacts;
-- earlier versions carry it at the top level of the redaction.
CREATE TEMP TABLE zz_v11_rooms ON COMMIT DROP AS
SELECT room_id FROM roomserver_rooms
WHERE CASE WHEN room_version ~ '^[0-9]+$' THEN room_version::int >= 11 ELSE false END;

-- The rooms whose rows this script may rewrite: known to the roomserver and
-- not pseudo-ID. Split-database deployments are not supported.
CREATE TEMP TABLE zz_repairable_rooms ON COMMIT DROP AS
SELECT room_id FROM roomserver_rooms
WHERE room_id NOT IN (SELECT room_id FROM zz_pseudo_rooms);

-- The "before" count plus every "skipped" count below should equal all wrapped
-- rows for each table; the skip categories are disjoint.
\echo '--- rows to repair (before) ---'
SELECT 'roomserver' AS src, count(*) AS wrapped
FROM roomserver_event_json j
JOIN roomserver_events e ON e.event_nid = j.event_nid
JOIN roomserver_rooms r ON r.room_nid = e.room_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND r.room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
UNION ALL
SELECT 'syncapi', count(*)
FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
UNION ALL
SELECT 'syncapi_current_room_state', count(*)
FROM syncapi_current_room_state
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL;

\echo '--- pseudo-ID rows skipped (left unchanged; need a separate follow-up) ---'
SELECT 'roomserver' AS src, count(*) AS wrapped
FROM roomserver_event_json j
JOIN roomserver_events e ON e.event_nid = j.event_nid
JOIN roomserver_rooms r ON r.room_nid = e.room_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND r.room_id IN (SELECT room_id FROM zz_pseudo_rooms)
UNION ALL
SELECT 'syncapi', count(*) FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_pseudo_rooms)
UNION ALL
SELECT 'syncapi_current_room_state', count(*) FROM syncapi_current_room_state
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_pseudo_rooms);

\echo '--- syncapi rows skipped: room absent from roomserver_rooms (left unchanged) ---'
SELECT 'syncapi' AS src, count(*) AS wrapped
FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id NOT IN (SELECT room_id FROM roomserver_rooms)
UNION ALL
SELECT 'syncapi_current_room_state', count(*)
FROM syncapi_current_room_state
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id NOT IN (SELECT room_id FROM roomserver_rooms);

\echo '--- roomserver wrapper rows without a room mapping (left unchanged) ---'
SELECT 'roomserver' AS src, count(*) AS wrapped
FROM roomserver_event_json j
LEFT JOIN roomserver_events e ON e.event_nid = j.event_nid
LEFT JOIN roomserver_rooms r ON r.room_nid = e.room_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND (e.event_nid IS NULL OR r.room_nid IS NULL);

-- The no-redacted_by count below excludes pseudo-ID and unmapped rooms, so all
-- of the skip categories are disjoint.
\echo '--- rows skipped: no unsigned.redacted_by (left unchanged) ---'
SELECT 'roomserver' AS src, count(*) AS wrapped
FROM roomserver_event_json j
JOIN roomserver_events e ON e.event_nid = j.event_nid
JOIN roomserver_rooms r ON r.room_nid = e.room_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND r.room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NULL
UNION ALL
SELECT 'syncapi', count(*) FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NULL
UNION ALL
SELECT 'syncapi_current_room_state', count(*) FROM syncapi_current_room_state
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NULL;

-- Backups of exactly the rows that will change.
CREATE TABLE zz_bak_rs_redacted_because AS
SELECT j.event_nid, e.event_id, j.event_json
FROM roomserver_event_json j
JOIN roomserver_events e ON e.event_nid = j.event_nid
JOIN roomserver_rooms r ON r.room_nid = e.room_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND r.room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL;

CREATE TABLE zz_bak_sync_redacted_because AS
SELECT id, event_id, headered_event_json
FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL;

CREATE TABLE zz_bak_sync_current_redacted_because AS
SELECT room_id, event_id, headered_event_json
FROM syncapi_current_room_state
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL;

\echo '--- reconciliation: unexplained should be 0, and backup should equal repairable ---'
-- total_wrapped = repairable + pseudo + no_redacted_by + unmapped, where
-- unexplained is the difference. backup is the number of rows written to the
-- backup table and should equal repairable.
CREATE TEMP TABLE zz_reconciliation ON COMMIT DROP AS
WITH rs_counts AS (
  SELECT
    count(*) AS total_wrapped,
    count(*) FILTER (WHERE repairable AND has_redacted_by) AS repairable,
    count(*) FILTER (WHERE pseudo) AS pseudo,
    count(*) FILTER (WHERE repairable AND NOT has_redacted_by) AS no_redacted_by,
    count(*) FILTER (WHERE NOT mapped) AS unmapped
  FROM (
    SELECT
      e.event_nid IS NOT NULL AND r.room_nid IS NOT NULL AS mapped,
      r.room_id IN (SELECT room_id FROM zz_repairable_rooms) AS repairable,
      r.room_id IN (SELECT room_id FROM zz_pseudo_rooms) AS pseudo,
      j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL AS has_redacted_by
    FROM roomserver_event_json j
    LEFT JOIN roomserver_events e ON e.event_nid = j.event_nid
    LEFT JOIN roomserver_rooms r ON r.room_nid = e.room_nid
    WHERE j.event_json LIKE '%redacted_because%'
      AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  ) x
),
sync_counts AS (
  SELECT
    count(*) AS total_wrapped,
    count(*) FILTER (WHERE repairable AND has_redacted_by) AS repairable,
    count(*) FILTER (WHERE pseudo) AS pseudo,
    count(*) FILTER (WHERE repairable AND NOT has_redacted_by) AS no_redacted_by,
    count(*) FILTER (WHERE NOT known) AS unmapped
  FROM (
    SELECT
      room_id IN (SELECT room_id FROM roomserver_rooms) AS known,
      room_id IN (SELECT room_id FROM zz_repairable_rooms) AS repairable,
      room_id IN (SELECT room_id FROM zz_pseudo_rooms) AS pseudo,
      headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL AS has_redacted_by
    FROM syncapi_output_room_events
    WHERE headered_event_json LIKE '%redacted_because%'
      AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  ) x
),
current_counts AS (
  SELECT
    count(*) AS total_wrapped,
    count(*) FILTER (WHERE repairable AND has_redacted_by) AS repairable,
    count(*) FILTER (WHERE pseudo) AS pseudo,
    count(*) FILTER (WHERE repairable AND NOT has_redacted_by) AS no_redacted_by,
    count(*) FILTER (WHERE NOT known) AS unmapped
  FROM (
    SELECT
      room_id IN (SELECT room_id FROM roomserver_rooms) AS known,
      room_id IN (SELECT room_id FROM zz_repairable_rooms) AS repairable,
      room_id IN (SELECT room_id FROM zz_pseudo_rooms) AS pseudo,
      headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL AS has_redacted_by
    FROM syncapi_current_room_state
    WHERE headered_event_json LIKE '%redacted_because%'
      AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  ) x
)
SELECT 'roomserver' AS src, total_wrapped, repairable, pseudo, no_redacted_by, unmapped,
       total_wrapped - (repairable + pseudo + no_redacted_by + unmapped) AS unexplained,
       (SELECT count(*) FROM zz_bak_rs_redacted_because) AS backup
FROM rs_counts
UNION ALL
SELECT 'syncapi', total_wrapped, repairable, pseudo, no_redacted_by, unmapped,
       total_wrapped - (repairable + pseudo + no_redacted_by + unmapped),
       (SELECT count(*) FROM zz_bak_sync_redacted_because)
FROM sync_counts
UNION ALL
SELECT 'syncapi_current_room_state', total_wrapped, repairable, pseudo, no_redacted_by, unmapped,
       total_wrapped - (repairable + pseudo + no_redacted_by + unmapped),
       (SELECT count(*) FROM zz_bak_sync_current_redacted_because)
FROM current_counts;

SELECT * FROM zz_reconciliation;

DO $$
DECLARE
  failed_tables text;
BEGIN
  SELECT string_agg(src, ', ' ORDER BY src) INTO failed_tables
  FROM zz_reconciliation
  WHERE unexplained <> 0 OR backup <> repairable;
  IF failed_tables IS NOT NULL THEN
    RAISE EXCEPTION 'wrapper reconciliation failed for %; transaction aborted; ROLLBACK and fix the cause', failed_tables;
  END IF;
END
$$;

-- roomserver_event_json
WITH src AS (
  SELECT j.event_nid,
         j.event_json::jsonb AS doc,
         j.event_json::jsonb #> '{unsigned,redacted_because,PDU}' AS rb,
         CASE WHEN r.room_id IN (SELECT room_id FROM zz_v11_rooms)
              THEN j.event_json::jsonb #> '{unsigned,redacted_because,PDU,content,redacts}'
              ELSE j.event_json::jsonb #> '{unsigned,redacted_because,PDU,redacts}' END AS target
  FROM roomserver_event_json j
  JOIN roomserver_events e ON e.event_nid = j.event_nid
  JOIN roomserver_rooms r ON r.room_nid = e.room_nid
  WHERE j.event_json LIKE '%redacted_because%'
    AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
    AND j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
    AND r.room_id IN (SELECT room_id FROM zz_repairable_rooms)
)
UPDATE roomserver_event_json j
SET event_json = jsonb_set(
      src.doc,
      '{unsigned,redacted_because}',
      -- Drop only absent outer fields. jsonb_strip_nulls would also strip null
      -- members nested inside content and unsigned.
      (SELECT jsonb_object_agg(f.key, f.value)
       FROM jsonb_each(jsonb_build_object(
        'type',             src.rb -> 'type',
        'sender',           src.rb -> 'sender',
        -- MSC2174: also serve the redacted ID in content, as the client format does.
        'content',          CASE WHEN jsonb_typeof(src.rb -> 'content') = 'object'
                                  AND NOT (src.rb -> 'content') ? 'redacts'
                                  AND src.target IS NOT NULL
                             THEN jsonb_set(src.rb -> 'content', '{redacts}', src.target)
                             ELSE src.rb -> 'content' END,
        'origin_server_ts', src.rb -> 'origin_server_ts',
        'redacts',          src.target,
        'unsigned',         src.rb -> 'unsigned',
        'event_id',         src.doc #>> '{unsigned,redacted_by}'
       )) AS f
       WHERE jsonb_typeof(f.value) <> 'null')
    )::text
FROM src
WHERE j.event_nid = src.event_nid;

-- syncapi_output_room_events
WITH src AS (
  SELECT s.id,
         s.headered_event_json::jsonb AS doc,
         s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU}' AS rb,
         CASE WHEN s.room_id IN (SELECT room_id FROM zz_v11_rooms)
              THEN s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU,content,redacts}'
              ELSE s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU,redacts}' END AS target
  FROM syncapi_output_room_events s
  WHERE s.headered_event_json LIKE '%redacted_because%'
    AND (s.headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
    AND s.headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
    AND s.room_id IN (SELECT room_id FROM zz_repairable_rooms)
)
UPDATE syncapi_output_room_events s
SET headered_event_json = jsonb_set(
      src.doc,
      '{unsigned,redacted_because}',
      -- Drop only absent outer fields. jsonb_strip_nulls would also strip null
      -- members nested inside content and unsigned.
      (SELECT jsonb_object_agg(f.key, f.value)
       FROM jsonb_each(jsonb_build_object(
        'type',             src.rb -> 'type',
        'sender',           src.rb -> 'sender',
        -- MSC2174: also serve the redacted ID in content, as the client format does.
        'content',          CASE WHEN jsonb_typeof(src.rb -> 'content') = 'object'
                                  AND NOT (src.rb -> 'content') ? 'redacts'
                                  AND src.target IS NOT NULL
                             THEN jsonb_set(src.rb -> 'content', '{redacts}', src.target)
                             ELSE src.rb -> 'content' END,
        'origin_server_ts', src.rb -> 'origin_server_ts',
        'redacts',          src.target,
        'unsigned',         src.rb -> 'unsigned',
        'event_id',         src.doc #>> '{unsigned,redacted_by}'
       )) AS f
       WHERE jsonb_typeof(f.value) <> 'null')
    )::text
FROM src
WHERE s.id = src.id;

-- syncapi_current_room_state
WITH src AS (
  SELECT s.room_id, s.event_id,
         s.headered_event_json::jsonb AS doc,
         s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU}' AS rb,
         CASE WHEN s.room_id IN (SELECT room_id FROM zz_v11_rooms)
              THEN s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU,content,redacts}'
              ELSE s.headered_event_json::jsonb #> '{unsigned,redacted_because,PDU,redacts}' END AS target
  FROM syncapi_current_room_state s
  WHERE s.headered_event_json LIKE '%redacted_because%'
    AND (s.headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
    AND s.headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
    AND s.room_id IN (SELECT room_id FROM zz_repairable_rooms)
)
UPDATE syncapi_current_room_state s
SET headered_event_json = jsonb_set(
      src.doc,
      '{unsigned,redacted_because}',
      -- Drop only absent outer fields. jsonb_strip_nulls would also strip null
      -- members nested inside content and unsigned.
      (SELECT jsonb_object_agg(f.key, f.value)
       FROM jsonb_each(jsonb_build_object(
        'type',             src.rb -> 'type',
        'sender',           src.rb -> 'sender',
        -- MSC2174: also serve the redacted ID in content, as the client format does.
        'content',          CASE WHEN jsonb_typeof(src.rb -> 'content') = 'object'
                                  AND NOT (src.rb -> 'content') ? 'redacts'
                                  AND src.target IS NOT NULL
                             THEN jsonb_set(src.rb -> 'content', '{redacts}', src.target)
                             ELSE src.rb -> 'content' END,
        'origin_server_ts', src.rb -> 'origin_server_ts',
        'redacts',          src.target,
        'unsigned',         src.rb -> 'unsigned',
        'event_id',         src.doc #>> '{unsigned,redacted_by}'
       )) AS f
       WHERE jsonb_typeof(f.value) <> 'null')
    )::text
FROM src
WHERE s.room_id = src.room_id AND s.event_id = src.event_id;

\echo '--- post-update: eligible repairable rows still wrapped (expect 0) ---'
CREATE TEMP TABLE zz_post_update ON COMMIT DROP AS
SELECT 'roomserver' AS src, count(*) AS post_update_still_wrapped
FROM roomserver_event_json j
JOIN roomserver_events e ON e.event_nid = j.event_nid
JOIN roomserver_rooms r ON r.room_nid = e.room_nid
WHERE j.event_json LIKE '%redacted_because%'
  AND (j.event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND r.room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND j.event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
UNION ALL
SELECT 'syncapi', count(*)
FROM syncapi_output_room_events
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL
UNION ALL
SELECT 'syncapi_current_room_state', count(*)
FROM syncapi_current_room_state
WHERE headered_event_json LIKE '%redacted_because%'
  AND (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'
  AND room_id IN (SELECT room_id FROM zz_repairable_rooms)
  AND headered_event_json::jsonb #>> '{unsigned,redacted_by}' IS NOT NULL;

SELECT * FROM zz_post_update;

\echo '--- verify: expect all three counts to be 0 ---'
-- still_wrapped: the PDU wrapper is still there.
-- id_mismatch:   redacted_because.event_id is not unsigned.redacted_by.
-- bad_shape:     content is not an object, or type/sender/origin_server_ts/event_id is missing.
CREATE TEMP TABLE zz_verify_counts ON COMMIT DROP AS
SELECT 'roomserver' AS src,
       count(*) FILTER (WHERE (event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU') AS still_wrapped,
       count(*) FILTER (WHERE event_json::jsonb #>> '{unsigned,redacted_because,event_id}'
                              IS DISTINCT FROM event_json::jsonb #>> '{unsigned,redacted_by}') AS id_mismatch,
       count(*) FILTER (WHERE jsonb_typeof(event_json::jsonb #> '{unsigned,redacted_because,content}') IS DISTINCT FROM 'object'
                              OR COALESCE(NOT ((event_json::jsonb #> '{unsigned,redacted_because}')
                                               ?& ARRAY['type','sender','origin_server_ts','event_id']), true)) AS bad_shape
FROM roomserver_event_json
WHERE event_nid IN (SELECT event_nid FROM zz_bak_rs_redacted_because)
UNION ALL
SELECT 'syncapi',
       count(*) FILTER (WHERE (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'),
       count(*) FILTER (WHERE headered_event_json::jsonb #>> '{unsigned,redacted_because,event_id}'
                              IS DISTINCT FROM headered_event_json::jsonb #>> '{unsigned,redacted_by}'),
       count(*) FILTER (WHERE jsonb_typeof(headered_event_json::jsonb #> '{unsigned,redacted_because,content}') IS DISTINCT FROM 'object'
                              OR COALESCE(NOT ((headered_event_json::jsonb #> '{unsigned,redacted_because}')
                                               ?& ARRAY['type','sender','origin_server_ts','event_id']), true))
FROM syncapi_output_room_events
WHERE id IN (SELECT id FROM zz_bak_sync_redacted_because)
UNION ALL
SELECT 'syncapi_current_room_state',
       count(*) FILTER (WHERE (headered_event_json::jsonb #> '{unsigned,redacted_because}') ? 'PDU'),
       count(*) FILTER (WHERE headered_event_json::jsonb #>> '{unsigned,redacted_because,event_id}'
                              IS DISTINCT FROM headered_event_json::jsonb #>> '{unsigned,redacted_by}'),
       count(*) FILTER (WHERE jsonb_typeof(headered_event_json::jsonb #> '{unsigned,redacted_because,content}') IS DISTINCT FROM 'object'
                              OR COALESCE(NOT ((headered_event_json::jsonb #> '{unsigned,redacted_because}')
                                               ?& ARRAY['type','sender','origin_server_ts','event_id']), true))
FROM syncapi_current_room_state
WHERE (room_id, event_id) IN (SELECT room_id, event_id FROM zz_bak_sync_current_redacted_because);

SELECT * FROM zz_verify_counts;

-- Turn failed verification and post-update checks into a hard stop. With
-- ON_ERROR_STOP set, this aborts before the operator can COMMIT.
DO $$
DECLARE
  failed_tables text;
BEGIN
  SELECT string_agg(
           src || ' (' || concat_ws(', ',
             CASE WHEN still_wrapped <> 0 THEN 'still_wrapped' END,
             CASE WHEN id_mismatch <> 0 THEN 'id_mismatch' END,
             CASE WHEN bad_shape <> 0 THEN 'bad_shape' END
           ) || ')', ', ' ORDER BY src)
    INTO failed_tables
  FROM zz_verify_counts
  WHERE still_wrapped <> 0 OR id_mismatch <> 0 OR bad_shape <> 0;
  IF failed_tables IS NOT NULL THEN
    RAISE EXCEPTION 'verification failed for %; transaction aborted; ROLLBACK and fix the cause', failed_tables;
  END IF;

  SELECT string_agg(src, ', ' ORDER BY src) INTO failed_tables
  FROM zz_post_update
  WHERE post_update_still_wrapped <> 0;
  IF failed_tables IS NOT NULL THEN
    RAISE EXCEPTION 'post-update check failed for %; repairable rows are still wrapped; transaction aborted; ROLLBACK and fix the cause', failed_tables;
  END IF;
END $$;

\echo 'COMMIT only if verification passed, post-update counts are 0,'
\echo 'and reconciliation has unexplained = 0 and backup = repairable.'
\echo 'Otherwise ROLLBACK.'
-- The transaction is intentionally left open here.
