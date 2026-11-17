-- Un-hide live state events that the old exclusion rule is still hiding.
--
-- This is the data half of the fix. The code half is the change to
-- excludeOldEventFromSync; run the code fix first (or at least deploy it in
-- the same window) so the same rows are not hidden again by a later resync.
--
-- What counts as a candidate, generically (no allowlist, no event IDs):
--
--   * syncapi_output_room_events.exclude_from_sync = TRUE
--   * the event is still the current state for its (room, type, state_key)
--     in syncapi_current_room_state -- otherwise it is history and belongs in
--     /messages only, not in the live timeline
--   * its topological position is at or after the room's live start: the
--     lowest topological position among that room's already-delivered
--     (exclude_from_sync = FALSE) events
--   * not m.room.create
--
-- The boundary is topological, not origin_server_ts: remote clocks are not
-- causal, and a late parent can lift an event's stored position above its raw
-- depth. This uses the stored position, which is what /messages would sort by.
--
-- It changes one boolean per row. It does not touch headered_event_json,
-- added_at, the topology table, relations, or stream positions. It is
-- idempotent: re-running finds nothing once the candidates are flipped.
--
-- It does NOT move a stream position, so a client whose sync token is already
-- past an event will not be notified by incremental /sync. Such a client still
-- sees the event in /messages scrollback, and a fresh client sees it in the
-- recent window. That is the accepted limitation; a replay path is not part of
-- this repair.
--
-- Usage, interactively, so the transaction can be inspected before COMMIT:
--     \set ON_ERROR_STOP on
--     \i scripts/repair_unhide_live_state.sql
-- The transaction is left OPEN at the end. Review the counts, then COMMIT; or
-- ROLLBACK;. Drop zz_bak_unhide_live_state once satisfied.

\pset pager off
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK off

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- The live start per room. Rooms with nothing delivered are absent, so no
-- candidate is produced for them: there is no live part to place an event in.
CREATE TEMP TABLE zz_live_start ON COMMIT DROP AS
SELECT t.room_id, min(t.topological_position) AS live_start
FROM syncapi_output_room_events_topology t
JOIN syncapi_output_room_events e ON e.event_id = t.event_id
WHERE e.exclude_from_sync = FALSE
GROUP BY t.room_id;

-- Every row the repair would change. Current state is required, so a row that
-- is only scrollback history is never flipped.
CREATE TEMP TABLE zz_unhide_candidates ON COMMIT DROP AS
SELECT o.id,
       o.room_id,
       o.type,
       o.event_id,
       t.topological_position,
       ls.live_start
FROM syncapi_output_room_events o
JOIN syncapi_output_room_events_topology t ON t.event_id = o.event_id
JOIN zz_live_start ls ON ls.room_id = o.room_id
JOIN syncapi_current_room_state s
  ON s.room_id = o.room_id AND s.event_id = o.event_id
WHERE o.exclude_from_sync = TRUE
  AND o.type <> 'm.room.create'
  AND t.topological_position >= ls.live_start;

\echo '--- candidates by room and type (review this) ---'
SELECT room_id, type, count(*) AS rows, min(topological_position) AS first_pos, max(topological_position) AS last_pos
FROM zz_unhide_candidates
GROUP BY room_id, type
ORDER BY rows DESC, room_id;

\echo '--- candidates (every row that will change) ---'
SELECT id, room_id, type, event_id, topological_position, live_start
FROM zz_unhide_candidates
ORDER BY room_id, topological_position, id;

-- Refuse to run on an empty or implausible set. An empty set is not an error
-- worth aborting on; it means the repair is already done. This RAISE only
-- guards the apply below, so comment it out for a read-only check.
DO $$
DECLARE
  n bigint;
BEGIN
  SELECT count(*) INTO n FROM zz_unhide_candidates;
  IF n = 0 THEN
    RAISE NOTICE 'no candidates: nothing to repair';
  END IF;
END
$$;

-- Exact backup of the rows that will change, in their pre-repair state.
CREATE TABLE zz_bak_unhide_live_state AS
SELECT o.id, o.room_id, o.event_id, o.exclude_from_sync
FROM syncapi_output_room_events o
JOIN zz_unhide_candidates c ON c.id = o.id;

\echo '--- reconciliation: candidates should equal backup ---'
SELECT (SELECT count(*) FROM zz_unhide_candidates) AS candidates,
       (SELECT count(*) FROM zz_bak_unhide_live_state) AS backup;

UPDATE syncapi_output_room_events o
SET exclude_from_sync = FALSE
FROM zz_unhide_candidates c
WHERE o.id = c.id
  AND o.exclude_from_sync = TRUE;

\echo '--- post-update: candidates still excluded (expect 0) ---'
SELECT count(*) AS remaining
FROM syncapi_output_room_events o
JOIN zz_unhide_candidates c ON c.id = o.id
WHERE o.exclude_from_sync = TRUE;

DO $$
DECLARE
  remaining bigint;
BEGIN
  SELECT count(*) INTO remaining
  FROM syncapi_output_room_events o
  JOIN zz_unhide_candidates c ON c.id = o.id
  WHERE o.exclude_from_sync = TRUE;
  IF remaining <> 0 THEN
    RAISE EXCEPTION 'post-update check failed: % candidates still excluded; ROLLBACK', remaining;
  END IF;
END
$$;

\echo 'The transaction is still open. COMMIT if the counts look right;'
\echo 'otherwise ROLLBACK. Drop zz_bak_unhide_live_state once satisfied.'
