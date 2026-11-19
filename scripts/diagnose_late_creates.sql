-- READ-ONLY diagnosis: list every room whose m.room.create was stored after
-- the room's first sync event, and whether scripts/reposition_late_creates.sql
-- can fix it. Changes nothing; the transaction is READ ONLY and rolled back.
--
--     psql -X -f scripts/diagnose_late_creates.sql <dsn>
--
-- Columns of the room listing:
--   create_id / first_id  stream positions; create_id > first_id means late
--   visible               exclude_from_sync = FALSE (delivered by /sync)
--   create_ts / first_ts  origin_server_ts; create_ts < first_ts is the
--                         symptom (an old create shown as new activity)
--   free_id               nearest unused id below first_id within the window
--                         (NULL = reposition cannot fix this room; hide it)
--   topo_rows / state_rows rows that must move with it (expect 1 / 1)
\pset pager off
\set ON_ERROR_STOP on
\set FREE_WINDOW 1000
BEGIN
    READ ONLY;

CREATE TEMP TABLE zz_first_event ON COMMIT DROP AS
SELECT
    room_id, min(id) AS first_id
FROM
    syncapi_output_room_events
GROUP BY
    room_id;

CREATE TEMP TABLE zz_diag ON COMMIT DROP AS
SELECT
    o.room_id, o.event_id, o.id AS create_id, f.first_id, o.exclude_from_sync = FALSE AS visible, o.origin_server_ts AS create_ts, (
        SELECT
            x.origin_server_ts
        FROM
            syncapi_output_room_events x
        WHERE
            x.id = f.first_id) AS first_ts, row_number() OVER (PARTITION BY o.room_id ORDER BY o.id) AS rn
FROM
    syncapi_output_room_events o
    JOIN zz_first_event f ON f.room_id = o.room_id
WHERE
    o.type = 'm.room.create'
    AND o.id > f.first_id;

\echo '--- summary ---'
SELECT
    count(*) AS late_creates,
    count(*) FILTER (WHERE visible) AS visible_to_sync,
    count(*) FILTER (WHERE NOT visible) AS already_hidden,
    count(DISTINCT room_id) AS rooms,
    count(*) FILTER (WHERE rn > 1) AS extra_creates_in_same_room
FROM
    zz_diag;

\echo '--- rooms (visible late creates, the ones to fix) ---'
SELECT
    d.room_id,
    d.event_id,
    d.create_id,
    d.first_id,
    d.create_id - d.first_id AS id_gap,
    to_timestamp(d.create_ts / 1000.0) AT TIME ZONE 'UTC' AS create_ts,
    to_timestamp(d.first_ts / 1000.0) AT TIME ZONE 'UTC' AS first_ts,
    (
        SELECT
            max(g)
        FROM
            generate_series(greatest (d.first_id - :FREE_WINDOW, 1), d.first_id - 1) g
        WHERE
            NOT EXISTS (
                SELECT
                    1
                FROM
                    syncapi_output_room_events x
                WHERE
                    x.id = g)) AS free_id,
        (
            SELECT
                count(*)
            FROM
                syncapi_output_room_events_topology t
            WHERE
                t.event_id = d.event_id) AS topo_rows,
        (
            SELECT
                count(*)
            FROM
                syncapi_current_room_state s
            WHERE
                s.event_id = d.event_id) AS state_rows,
        -- Context to tell a real late create from an edge case: the event
        -- stored immediately before it, and the room's newest stream id.
        (
            SELECT
                p.type || ' @' || p.id
            FROM
                syncapi_output_room_events p
            WHERE
                p.room_id = d.room_id
                AND p.id < d.create_id
            ORDER BY
                p.id DESC
            LIMIT 1) AS preceding_event,
    (
        SELECT
            max(id)
        FROM
            syncapi_output_room_events m
        WHERE
            m.room_id = d.room_id) AS room_last_id,
    d.rn
FROM
    zz_diag d
WHERE
    d.visible
ORDER BY
    d.create_id;

\echo '--- visible late creates that reposition cannot fix (no free id) ---'
SELECT
    d.room_id,
    d.event_id,
    d.first_id
FROM
    zz_diag d
WHERE
    d.visible
    AND NOT EXISTS (
        SELECT
            1
        FROM
            generate_series(greatest (d.first_id - :FREE_WINDOW, 1), d.first_id - 1) g
        WHERE
            NOT EXISTS (
                SELECT
                    1
                FROM
                    syncapi_output_room_events x
                WHERE
                    x.id = g))
    ORDER BY
        d.room_id;

\echo '--- rooms with more than one late create (need one free id each) ---'
SELECT
    room_id,
    count(*) AS late_creates
FROM
    zz_diag
WHERE
    visible
GROUP BY
    room_id
HAVING
    count(*) > 1;

ROLLBACK;
