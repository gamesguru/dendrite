-- Repair roomserver_event_json rows that hold a hash-pruned copy of an event
-- whose complete copy survived in syncapi_current_room_state.
--
-- A parse that fails gomatrixserverlib's content hash check yields the pruned
-- form of the event (only the fields that survive redaction are kept), and the
-- old unconditional upsert in EventJSONTable could replace a stored full event
-- with such a copy. Such rows have no unsigned.redacted_because: they are not
-- the result of a redaction, they are the remains of a failed hash check. The
-- sync API kept the full copy, which still matches the event's hashes.sha256.
--
-- This script rewrites roomserver_event_json for the affected state events from
-- the sync API copy. The sync copy is headered: _room_version and _event_id are
-- appended after the canonical payload. They are removed textually, not with
-- jsonb, because jsonb reorders keys and would break the canonical form that
-- the content hash is computed over. For the same reason the rest of the text
-- is copied byte for byte.
--
-- Limitation: SQL cannot recompute hashes.sha256. The sync copies are the
-- hash-verified ones, and only the two non-hashed header fields are removed, so
-- the result hashes to the same value; this script cannot prove that itself.
--
-- Re-running: it only ever writes the allowlisted rows, and only while all
-- four are still outstanding. After a successful repair the second run finds
-- zero and aborts; that abort means there is nothing left to repair, not that
-- something broke. A partial set (fewer than four outstanding) also aborts,
-- so a row that disappeared is never silently skipped. Backup tables are
-- created WITHOUT "IF NOT EXISTS" on purpose, so a stale backup from an earlier
-- run makes the script fail instead of being silently reused.
--
-- Two guards restrict what may be overwritten:
--   1. Containment. The sync copy's content must contain the stored content
--      (the pruned copy is a subset of the full one). This refuses to replace
--      a row with a sync copy that is not strictly richer, so a stale or
--      divergent sync row cannot win.
--   2. Allowlist. The event_nids are pinned to the rows audited for this
--      deployment. A different deployment must review and update this list.
--      If any audited row has disappeared, the script aborts instead of
--      repairing a partial set.
--
-- Order of operations: deploy the storage fix first. This script repairs the
-- existing rows; without the code fix, the same full-to-pruned overwrite can
-- happen again and undo the repair.
--
-- Usage: connect interactively to the dendrite database, then run:
--     \set ON_ERROR_STOP on
--     \i scripts/repair_pruned_event_json.sql
-- Run it with \i from that interactive session, not with psql -f (which exits
-- and rolls back at the end instead of leaving the transaction open for
-- inspection). The script leaves the transaction OPEN. Inspect the counts,
-- then type COMMIT; or ROLLBACK;.
--
-- Stop Dendrite for the run, or restart it afterwards: the repair writes the
-- table directly, bypassing the cache invalidation the event store performs.
-- Drop zz_bak_pruned_event_json once satisfied.

\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK off

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- Rows where the roomserver copy is pruned and the sync API has the full one.
-- A real redaction is excluded: it carries unsigned.redacted_because and must
-- stay authoritative. The content containment test (sync content @> stored
-- content) ensures the sync copy is the richer one. The allowlist pins the
-- exact audited rows; see the header.
CREATE TEMP TABLE zz_pruned_targets ON COMMIT DROP AS
SELECT s.event_id,
       e.event_nid,
       s.headered_event_json AS sync_json,
       j.event_json          AS stored_json
FROM syncapi_current_room_state s
JOIN roomserver_events e ON e.event_id = s.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE e.event_nid IN (9770, 19216, 19219, 19240)
  AND (j.event_json::jsonb -> 'content') IS DISTINCT FROM (s.headered_event_json::jsonb -> 'content')
  AND (s.headered_event_json::jsonb -> 'content') @> (j.event_json::jsonb -> 'content')
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND s.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL;

-- The allowlist and the audit must agree: all four rows must still qualify.
DO $$
DECLARE
  repaired bigint;
BEGIN
  SELECT count(*) INTO repaired FROM zz_pruned_targets;
  IF repaired <> 4 THEN
    RAISE EXCEPTION 'expected 4 allowlisted rows to repair, found %; transaction aborted; ROLLBACK', repaired;
  END IF;
END
$$;

\echo '--- rows to repair ---'
SELECT event_id, event_nid FROM zz_pruned_targets ORDER BY event_nid;

-- The header keys are appended in this order: _room_version, then _event_id.
CREATE TEMP TABLE zz_pruned_fixed ON COMMIT DROP AS
SELECT event_id,
       event_nid,
       regexp_replace(
         regexp_replace(sync_json, ',"_event_id":"[^"]*"}$', '}'),
         ',"_room_version":"[^"]*"}$', '}'
       ) AS fixed_json
FROM zz_pruned_targets;

-- Every row must change, must still parse, and must no longer carry headers.
DO $$
DECLARE
  problems text;
BEGIN
  SELECT string_agg(event_id, ', ' ORDER BY event_nid) INTO problems
  FROM zz_pruned_fixed
  WHERE fixed_json = (SELECT t.stored_json FROM zz_pruned_targets t WHERE t.event_id = zz_pruned_fixed.event_id)
     OR fixed_json LIKE '%"_event_id"%'
     OR fixed_json LIKE '%"_room_version"%'
     OR fixed_json::jsonb IS NULL
     OR (fixed_json::jsonb -> 'content') IS NULL;
  IF problems IS NOT NULL THEN
    RAISE EXCEPTION 'header strip did not produce a clean event for %; transaction aborted; ROLLBACK', problems;
  END IF;
END
$$;

-- Exact backups of the rows that will change.
CREATE TABLE zz_bak_pruned_event_json AS
SELECT event_nid, event_id, stored_json AS event_json FROM zz_pruned_targets;

\echo '--- reconciliation: backup should equal targets ---'
SELECT (SELECT count(*) FROM zz_pruned_targets) AS targets,
       (SELECT count(*) FROM zz_pruned_fixed)   AS fixed,
       (SELECT count(*) FROM zz_bak_pruned_event_json) AS backup;

UPDATE roomserver_event_json j
SET event_json = f.fixed_json
FROM zz_pruned_fixed f
WHERE j.event_nid = f.event_nid;

\echo '--- post-update: repairable rows still mismatched (expect 0) ---'
SELECT count(*) AS remaining
FROM syncapi_current_room_state s
JOIN roomserver_events e ON e.event_id = s.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (s.headered_event_json::jsonb -> 'content')
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND s.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL;

DO $$
DECLARE
  remaining bigint;
BEGIN
  SELECT count(*) INTO remaining
  FROM syncapi_current_room_state s
  JOIN roomserver_events e ON e.event_id = s.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
  WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (s.headered_event_json::jsonb -> 'content')
    AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
    AND s.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL;
  IF remaining <> 0 THEN
    RAISE EXCEPTION 'post-update check failed: % rows still mismatched; transaction aborted; ROLLBACK', remaining;
  END IF;
END
$$;

\echo 'The target count has already been checked (must be exactly 4) and the'
\echo 'backup/fixed counts must agree. COMMIT only if remaining is 0.'
\echo 'Otherwise ROLLBACK.'
-- The transaction is intentionally left open here.
