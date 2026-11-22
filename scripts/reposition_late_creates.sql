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
--     scripts/archive/repair_late_creates.sql.
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
-- The transaction is left OPEN at the end. Locks on the PDU-position and
-- occupancy tables block writers until COMMIT or ROLLBACK, keeping the plan
-- valid throughout the repair.
\pset pager off
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK off
\set FREE_WINDOW 1000
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '300s';
-- SHARE ROW EXCLUSIVE conflicts with ordinary write locks and is held through
-- commit. These are the tables used to read or write occupied stream IDs.
LOCK TABLE syncapi_output_room_events IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE syncapi_output_room_events_topology IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE syncapi_current_room_state IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE syncapi_unpartialstated_rooms IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE syncapi_peeks IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE syncapi_sliding_sync_joined_rooms IN SHARE ROW EXCLUSIVE MODE;
CREATE TEMP TABLE zz_first_event ON COMMIT DROP AS
SELECT
    room_id, min(id) AS first_id
FROM
    syncapi_output_room_events
GROUP BY
    room_id;
-- Late creates stored after the room's first event, visible or already
-- hidden. a hidden one is unhidden after it moves (see below).
CREATE TEMP TABLE zz_late_creates ON COMMIT DROP AS
SELECT
    o.id AS old_id, o.room_id, o.event_id, f.first_id, row_number() OVER (PARTITION BY o.room_id ORDER BY o.id) AS rn
FROM
    syncapi_output_room_events o
    JOIN zz_first_event f ON f.room_id = o.room_id
WHERE
    o.type = 'm.room.create'
    AND o.id > f.first_id
    -- !YRsw's create is stored under an orphaned roomserver incarnation (room
    -- NID 3, live room NID 11). Left in place until that is settled.
    AND o.room_id <> '!YRswYZcqh4FWoo1LLk:nutra.tk';
-- Unused PDU positions below each affected room's first id, nearest first.
-- Un-partial-state markers and peeks share the PDU token coordinate, so their
-- IDs are occupied too even though they are in other tables.
CREATE TEMP TABLE zz_free_ids ON COMMIT DROP AS
SELECT
    r.room_id, g AS free_id,
    row_number() OVER (PARTITION BY r.room_id ORDER BY g DESC) AS rn
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
            x.id = g)
    AND NOT EXISTS (
        SELECT
            1
        FROM
            syncapi_unpartialstated_rooms u
        WHERE
            u.id = g)
    AND NOT EXISTS (
        SELECT
            1
        FROM
            syncapi_peeks p
        WHERE
            p.id = g);
-- Allocate one ID at a time so an ID chosen for one room is not reused by
-- another room whose free window overlaps it. Each room keeps its own nearest
-- candidate and moves to the next available ID when that one is taken.
CREATE TEMP TABLE zz_plan ON COMMIT DROP AS
SELECT
    old_id, NULL::bigint AS new_id, room_id, event_id, first_id, rn
FROM
    zz_late_creates;
DO $$
DECLARE
    candidate record;
    chosen bigint;
    used bigint[] := ARRAY[]::bigint[];
BEGIN
    FOR candidate IN
    SELECT
        *
    FROM
        zz_plan
    ORDER BY
        room_id, rn
    LOOP
        chosen := NULL;
        SELECT
            f.free_id
        INTO
            chosen
        FROM
            zz_free_ids f
        WHERE
            f.room_id = candidate.room_id
            AND NOT (f.free_id = ANY (used))
        ORDER BY
            f.rn
        LIMIT 1;
        IF chosen IS NOT NULL THEN
            UPDATE
                zz_plan
            SET
                new_id = chosen
            WHERE
                old_id = candidate.old_id;
            used := used || chosen;
        END IF;
    END LOOP;
END
$$;
-- GUARDS: refuse ambiguous plans, stream-position collisions, or moving the
-- known !YRsw create if it is not attached to the live room incarnation.
DO $$
DECLARE
    duplicate_ids bigint;
    marker_collisions bigint;
    bad_rows bigint;
    live_room_nid bigint;
    create_room_nid bigint;
BEGIN
    SELECT
        count(*) - count(DISTINCT new_id)
    INTO
        duplicate_ids
    FROM
        zz_plan
    WHERE
        new_id IS NOT NULL;
    IF duplicate_ids <> 0 THEN
        RAISE EXCEPTION 'plan reuses % new stream IDs across rooms; ROLLBACK and review', duplicate_ids;
    END IF;
    SELECT
        count(*)
    INTO
        marker_collisions
    FROM
        zz_plan p
        JOIN syncapi_unpartialstated_rooms u ON u.id = p.new_id
    WHERE
        p.new_id IS NOT NULL;
    IF marker_collisions <> 0 THEN
        RAISE EXCEPTION 'plan collides with % un-partial-state PDU markers; ROLLBACK and review', marker_collisions;
    END IF;
    -- Moving a carrier row can move unique state deltas out of the range
    -- visited by an incremental sync. Leave such a row in place (new_id NULL)
    -- unless another row between its new and old IDs carries each delta.
    UPDATE
        zz_plan p
    SET
        new_id = NULL
    FROM
        syncapi_output_room_events moved
    WHERE
        moved.id = p.old_id
        AND moved.event_id = p.event_id
        AND p.new_id IS NOT NULL
        AND EXISTS (
            SELECT
                1
            FROM
                unnest(coalesce(moved.add_state_ids, ARRAY[]::text[]) ||
                    coalesce(moved.remove_state_ids, ARRAY[]::text[])) delta(event_id)
            WHERE
                delta.event_id <> p.event_id
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        syncapi_output_room_events other
                    WHERE
                        other.id BETWEEN p.new_id AND p.old_id
                        AND other.id <> p.old_id
                        AND (delta.event_id = ANY (coalesce(other.add_state_ids, ARRAY[]::text[]))
                            OR delta.event_id = ANY (coalesce(other.remove_state_ids, ARRAY[]::text[])))));
    SELECT
        count(*)
    INTO
        bad_rows
    FROM
        zz_plan p
    WHERE
        p.new_id IS NOT NULL
        AND ((
                SELECT
                    count(*)
                FROM
                    syncapi_output_room_events_topology t
                WHERE
                    t.event_id = p.event_id) <> 1
                OR (
                    SELECT
                        count(*)
                    FROM
                        syncapi_current_room_state s
                    WHERE
                        s.event_id = p.event_id
                        AND s.room_id = p.room_id) <> 1
                    OR p.new_id >= p.first_id);
    IF bad_rows <> 0 THEN
        RAISE EXCEPTION 'plan has % rows without exactly one topology/current-state row or not before first_id; ROLLBACK', bad_rows;
    END IF;
    IF EXISTS (
        SELECT
            1
        FROM
            zz_plan
        WHERE
            event_id = '$-DqZ28jVQgUybnxxq35vUXqAiaJpAfXO0eXzv9LQu9s') THEN
    SELECT
        room_nid
    INTO
        live_room_nid
    FROM
        roomserver_rooms
    WHERE
        room_id = '!YRswYZcqh4FWoo1LLk:nutra.tk';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'live !YRsw room is absent from roomserver_rooms; ROLLBACK';
    END IF;
    SELECT
        room_nid
    INTO
        create_room_nid
    FROM
        roomserver_events
    WHERE
        event_id = '$-DqZ28jVQgUybnxxq35vUXqAiaJpAfXO0eXzv9LQu9s';
    IF NOT FOUND OR create_room_nid <> live_room_nid THEN
        RAISE EXCEPTION '!YRsw create does not belong to its live room NID; ROLLBACK';
    END IF;
END IF;
END
$$;
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
            AND s.room_id = p.room_id) AS old_added_at,
    (
        SELECT
            o.exclude_from_sync
        FROM
            syncapi_output_room_events o
        WHERE
            o.id = p.old_id) AS old_exclude_from_sync,
    (
        SELECT
            s.event_stream_ordering
        FROM
            syncapi_sliding_sync_joined_rooms s
        WHERE
            s.room_id = p.room_id) AS old_sliding_event_stream_ordering,
    (
        SELECT
            s.bump_stamp
        FROM
            syncapi_sliding_sync_joined_rooms s
        WHERE
            s.room_id = p.room_id) AS old_sliding_bump_stamp
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
        o.id AS new_id,
        p.old_id
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
    added_at = CASE WHEN s.added_at = m.old_id THEN
        m.new_id
    ELSE
        s.added_at
    END
FROM
    zz_moved m
WHERE
    s.event_id = m.event_id;
-- Keep sliding-sync room ordering metadata coherent if it points at a moved
-- create. The metadata worker writes BOTH columns from the room's newest
-- bump-type event (extractRoomMetadata via MaxStreamPositionsForRooms), hidden
-- rows included, so recompute that one value after the move and use it for
-- both. Only a column whose cached value was exactly an old create position
-- is touched; other values may be timestamps.
UPDATE
    syncapi_sliding_sync_joined_rooms s
SET
    event_stream_ordering = CASE WHEN s.event_stream_ordering = ANY (r.old_ids) THEN
        r.bump_max
    ELSE
        s.event_stream_ordering
    END,
    bump_stamp = CASE WHEN s.bump_stamp = ANY (r.old_ids) THEN
        r.bump_max
    ELSE
        s.bump_stamp
    END
FROM (
    SELECT
        b.room_id,
        array_agg(b.old_id) AS old_ids,
        (
            SELECT
                max(e.id)
            FROM
                syncapi_output_room_events e
            WHERE
                e.room_id = b.room_id
                AND e.type = ANY (ARRAY['m.room.create', 'm.room.message', 'm.room.encrypted', 'm.sticker', 'm.call.invite', 'm.poll.start', 'm.beacon_info'])) AS bump_max
        FROM
            zz_bak_reposition_creates b
        GROUP BY
            b.room_id) r
WHERE
    s.room_id = r.room_id
    AND (s.event_stream_ordering = ANY (r.old_ids)
        OR s.bump_stamp = ANY (r.old_ids));
-- A moved create now sits before the room's first event, so it is history,
-- not new activity: make it visible to sync at its corrected position. This
-- unhides creates the ingest classifier had hidden (old_exclude_from_sync in
-- the backup records which).
UPDATE
    syncapi_output_room_events o
SET
    exclude_from_sync = FALSE
FROM
    zz_moved m
WHERE
    o.event_id = m.event_id
    AND o.exclude_from_sync IS DISTINCT FROM FALSE;
DO $$
DECLARE
    planned bigint;
    moved bigint;
    stale bigint;
    visible bigint;
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
    SELECT
        count(*)
    INTO
        visible
    FROM
        zz_bak_reposition_creates b
        JOIN syncapi_output_room_events o ON o.event_id = b.event_id
    WHERE
        o.exclude_from_sync IS DISTINCT FROM FALSE;
    IF visible <> 0 THEN
        RAISE EXCEPTION 'post-update check failed: % moved creates remain hidden. ROLLBACK', visible;
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
