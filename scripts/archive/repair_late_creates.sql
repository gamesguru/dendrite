-- Hide late-stored m.room.create rows that sync delivered as new activity.
--
-- How a row gets here: a room is discovered through an invite or join, and
-- its create event is fetched later. The create gets a new stream position,
-- so forward /sync presents the start of the room as the newest event.
--
-- The code half is excludeNewEventFromSync in syncapi/consumers/roomserver.go:
-- a state event that arrives as new but is not a forward extremity is stored
-- with exclude_from_sync = TRUE. Deploy that first so the same rows are not
-- stored visible again.
--
-- Candidate: type = m.room.create, still visible to sync, and stored after
-- some other event in the same room. A create stored first for its room is
-- left alone; it was not late.
--
-- This changes one boolean per row. It does not touch JSON, stream positions,
-- the topology table, current state, relations, or any non-create row. The
-- create remains in the state block and in /messages history.
--
-- Usage, interactively, so the transaction can be inspected before COMMIT:
--     \set ON_ERROR_STOP on
--     \i scripts/archive/repair_late_creates.sql
-- The transaction is left OPEN at the end. Review the candidates, then COMMIT;
-- or ROLLBACK;. Drop zz_bak_late_creates once satisfied.

\pset pager off
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK off

BEGIN ISOLATION LEVEL READ COMMITTED;

-- Keep sync writers out while the candidate set and the UPDATE run, so a
-- create cannot be stored between the two steps and missed.
LOCK TABLE syncapi_output_room_events IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE zz_first_event ON COMMIT DROP AS
SELECT room_id, min(id) AS first_id
FROM syncapi_output_room_events
GROUP BY room_id;

CREATE TEMP TABLE zz_late_creates ON COMMIT DROP AS
SELECT o.id, o.room_id, o.event_id
FROM syncapi_output_room_events o
JOIN zz_first_event f ON f.room_id = o.room_id
WHERE o.type = 'm.room.create'
  AND o.exclude_from_sync = FALSE
  AND o.id > f.first_id;

\echo '--- late creates (every row that will change) ---'
SELECT id, room_id, event_id
FROM zz_late_creates
ORDER BY id;

-- Exact backup of the rows that will change, in their pre-repair state.
CREATE TABLE zz_bak_late_creates AS
SELECT o.id, o.room_id, o.event_id, o.exclude_from_sync
FROM syncapi_output_room_events o
JOIN zz_late_creates c ON c.id = o.id;

\echo '--- reconciliation: candidates should equal backup ---'
SELECT (SELECT count(*) FROM zz_late_creates) AS candidates,
       (SELECT count(*) FROM zz_bak_late_creates) AS backup;

UPDATE syncapi_output_room_events o
SET exclude_from_sync = TRUE
FROM zz_late_creates c
WHERE o.id = c.id
  AND o.exclude_from_sync = FALSE;

\echo '--- post-update: candidates still excluded (expect 0) ---'
SELECT count(*) AS remaining
FROM syncapi_output_room_events o
JOIN zz_late_creates c ON c.id = o.id
WHERE o.exclude_from_sync = FALSE;

DO $$
DECLARE
  remaining bigint;
BEGIN
  SELECT count(*) INTO remaining
  FROM syncapi_output_room_events o
  JOIN zz_late_creates c ON c.id = o.id
  WHERE o.exclude_from_sync = FALSE;
  IF remaining <> 0 THEN
    RAISE EXCEPTION 'post-update check failed: % candidates still visible; ROLLBACK', remaining;
  END IF;
END
$$;

\echo 'The transaction is still open. COMMIT if the candidates look right;'
\echo 'otherwise ROLLBACK. Drop zz_bak_late_creates once satisfied.'
