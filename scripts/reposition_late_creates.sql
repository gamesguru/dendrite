-- Move late-stored m.room.create rows back to the start of their room's
-- stream, instead of hiding them from /sync.
--
-- How a row gets here: a room's create event was stored after other events in
-- the room (late state install or join), so it received a later stream
-- position than the room's first event. Forward /sync then delivered the start
-- of the room as new activity.
--
-- This script reassigns the create's stream position to an UNUSED id just
-- below the room's first event. Three places carry the position and all move
-- together, per event_id:
--   syncapi_output_room_events.id                       (PK, /sync order)
--   syncapi_output_room_events_topology.stream_position (tiebreaker only)
--   syncapi_current_room_state.added_at                 (initial-sync state order)
-- Nothing else changes: no JSON, no other row, no other room, no sequence.
-- /messages order is unaffected; it sorts by topological position.
--
-- Limits:
--   * A free id must exist within FREE_WINDOW below the room's first id. A room
--     with none is listed as SKIPPED and left alone. Hide it instead with
--     scripts/repair_late_creates.sql.
--   * Clients that already received the create keep their misplaced copy until
--     they clear their cache. Only fresh and initial syncs change.
--   * A client whose token lies between the new and old id and never saw the
--     create will not receive it as a delta. It is still in the state block.
--   * Postgres only.
--
-- Usage, interactively. Review the output, then COMMIT or ROLLBACK:
--     \set ON_ERROR_STOP on
--     \i scripts/reposition_late_creates.sql
-- Dry run: after reviewing the first listing, type ROLLBACK; at the prompt.
-- The transaction is left OPEN at the end. No table lock is taken; the UPDATE
-- re-checks each row's old id and visibility, and a count assertion aborts the
-- transaction if any candidate changed in between.
\pset pager off
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK off
\set FREE_WINDOW 1000
BEGIN ISOLATION LEVEL READ COMMITTED;
CREATE TEMP TABLE zz_first_event ON COMMIT DROP AS
SELECT
    room_id, min(id) AS first_id
FROM
    syncapi_output_room_events
GROUP BY
    room_id;
-- Late creates stored after the room's first event, visible or already
-- hidden. exclude_from_sync is left exactly as it is; only the position moves.
CREATE TEMP TABLE zz_late_creates ON COMMIT DROP AS
SELECT
    o.id AS old_id, o.room_id, o.event_id, f.first_id, row_number() OVER (PARTITION BY o.room_id ORDER BY o.id) AS rn
FROM
    syncapi_output_room_events o
    JOIN zz_first_event f ON f.room_id = o.room_id
WHERE
    o.type = 'm.room.create'
    AND o.id > f.first_id;
-- Unused ids below each affected room's first id, nearest first. The sequence
-- has already passed them, so they are never handed out again.
CREATE TEMP TABLE zz_free_ids ON COMMIT DROP AS
SELECT
    r.room_id, g AS free_id, row_number() OVER (PARTITION BY r.room_id ORDER BY g DESC) AS rn
FROM ( SELECT DISTINCT
        room_id, first_id
    FROM
        zz_late_creates) r, generate_series(greatest (r.first_id - :FREE_WINDOW, 1), r.first_id - 1) AS g
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            syncapi_output_room_events x
        WHERE
            x.id = g);
CREATE TEMP TABLE zz_plan ON COMMIT DROP AS
SELECT
    c.old_id, f.free_id AS new_id, c.room_id, c.event_id, c.first_id
FROM
    zz_late_creates c
    LEFT JOIN zz_free_ids f ON f.room_id = c.room_id
        AND f.rn = c.rn;
\echo '--- plan: every row considered (new_id NULL = SKIPPED, no free id) ---'
SELECT
    p.old_id,
    p.new_id,
    p.first_id,
    p.room_id,
    p.event_id,
    CASE WHEN p.new_id IS NULL THEN
        'SKIPPED'
    ELSE
        'move'
    END AS action,
    (
        SELECT
            count(*)
        FROM
            syncapi_output_room_events_topology t
        WHERE
            t.event_id = p.event_id) AS topology_rows,
    (
        SELECT
            count(*)
        FROM
            syncapi_current_room_state s
        WHERE
            s.event_id = p.event_id) AS current_state_rows
FROM
    zz_plan p
ORDER BY
    p.old_id;
-- Exact pre-repair values, for restoring by hand.
CREATE TABLE zz_bak_reposition_creates AS
SELECT
    p.old_id,
    p.new_id,
    p.room_id,
    p.event_id,
    (
        SELECT
            t.stream_position
        FROM
            syncapi_output_room_events_topology t
        WHERE
            t.event_id = p.event_id) AS old_topology_stream_position,
    (
        SELECT
            s.added_at
        FROM
            syncapi_current_room_state s
        WHERE
            s.event_id = p.event_id
            AND s.room_id = p.room_id) AS old_added_at
FROM
    zz_plan p
WHERE
    p.new_id IS NOT NULL;
\echo '--- reconciliation ---'
SELECT
    (
        SELECT
            count(*)
        FROM
            zz_plan) AS considered,
    (
        SELECT
            count(*)
        FROM
            zz_plan
        WHERE
            new_id IS NULL) AS skipped,
    (
        SELECT
            count(*)
        FROM
            zz_bak_reposition_creates) AS to_move;
-- Move all three position columns. The events row goes first; the others key
-- on event_id so order does not matter for them.
CREATE TEMP TABLE zz_moved ON COMMIT DROP AS
WITH moved AS (
    UPDATE
        syncapi_output_room_events o
    SET
        id = p.new_id
    FROM
        zz_plan p
    WHERE
        p.new_id IS NOT NULL
        AND o.id = p.old_id
        AND o.event_id = p.event_id
    RETURNING
        o.event_id,
        o.id AS new_id
)
SELECT
    *
FROM
    moved;
UPDATE
    syncapi_output_room_events_topology t
SET
    stream_position = m.new_id
FROM
    zz_moved m
WHERE
    t.event_id = m.event_id;
UPDATE
    syncapi_current_room_state s
SET
    added_at = m.new_id
FROM
    zz_moved m
WHERE
    s.event_id = m.event_id;
DO $$
DECLARE
    planned bigint;
    moved bigint;
    stale bigint;
BEGIN
    SELECT
        count(*)
    INTO
        planned
    FROM
        zz_bak_reposition_creates;
    SELECT
        count(*)
    INTO
        moved
    FROM
        zz_moved;
    IF moved <> planned THEN
        RAISE EXCEPTION 'moved % rows but planned %; a candidate changed concurrently. ROLLBACK', moved, planned;
    END IF;
    -- Nothing may still sit at an old id, and every moved row must be before
    -- its room's first remaining event.
    SELECT
        count(*)
    INTO
        stale
    FROM
        zz_bak_reposition_creates b
        JOIN syncapi_output_room_events o ON o.event_id = b.event_id
    WHERE
        o.id <> b.new_id
        OR o.id >= (
            SELECT
                min(x.id)
            FROM
                syncapi_output_room_events x
            WHERE
                x.room_id = b.room_id
                AND x.event_id <> b.event_id);
    IF stale <> 0 THEN
        RAISE EXCEPTION 'post-update check failed for % rows. ROLLBACK', stale;
    END IF;
END
$$;
\echo '--- post-update: moved rows ---'
SELECT
    b.event_id,
    b.old_id,
    o.id AS now_id
FROM
    zz_bak_reposition_creates b
    JOIN syncapi_output_room_events o ON o.event_id = b.event_id
ORDER BY
    b.old_id;
\echo 'The transaction is still open. COMMIT if the plan looks right;'
\echo 'otherwise ROLLBACK. SKIPPED rooms were not touched.'
\echo 'Drop zz_bak_reposition_creates once satisfied.'
