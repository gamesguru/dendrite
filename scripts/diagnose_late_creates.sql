-- READ-ONLY diagnosis of visible m.room.create rows stored after the room's
-- first sync event. This script uses SELECTs only; it is safe in a read-only
-- transaction and rolls back at the end.
--
--     psql -X -f scripts/diagnose_late_creates.sql <dsn>
--
-- free_id is the nearest unused output-event id below first_id, within the
-- reposition script's search window. topo_rows and state_rows should each be
-- one for the current incident candidates.
\pset pager off
\set ON_ERROR_STOP on
\set FREE_WINDOW 1000
BEGIN
    READ ONLY;

\echo '--- summary: all late creates, including already-hidden rows ---'
WITH room_first AS (
    SELECT
        room_id,
        min(id) AS first_id
    FROM
        syncapi_output_room_events
    GROUP BY
        room_id
),
diag AS (
    SELECT
        o.room_id,
        o.id,
        o.exclude_from_sync,
        row_number() OVER (PARTITION BY o.room_id ORDER BY o.id) AS rn
    FROM
        syncapi_output_room_events o
        JOIN room_first f ON f.room_id = o.room_id
    WHERE
        o.type = 'm.room.create'
        AND o.id > f.first_id
)
SELECT
    count(*) AS late_creates,
    count(*) FILTER (WHERE exclude_from_sync = FALSE) AS visible_to_sync,
    count(*) FILTER (WHERE exclude_from_sync = TRUE) AS already_hidden,
    count(DISTINCT room_id) AS rooms,
    count(*) FILTER (WHERE rn > 1) AS extra_creates_in_same_room
FROM
    diag;

\echo '--- visible late creates with room context and repair indicators ---'
WITH room_first AS (
    SELECT
        room_id,
        min(id) AS first_id
    FROM
        syncapi_output_room_events
    GROUP BY
        room_id
),
diag AS (
    SELECT
        o.room_id,
        o.event_id,
        o.id AS create_id,
        f.first_id,
        o.origin_server_ts AS create_ts,
        (
            SELECT
                x.origin_server_ts
            FROM
                syncapi_output_room_events x
            WHERE
                x.id = f.first_id) AS first_ts,
        row_number() OVER (PARTITION BY o.room_id ORDER BY o.id) AS rn
    FROM
        syncapi_output_room_events o
        JOIN room_first f ON f.room_id = o.room_id
    WHERE
        o.type = 'm.room.create'
        AND o.exclude_from_sync = FALSE
        AND o.id > f.first_id
)
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
            generate_series(greatest (d.first_id - :FREE_WINDOW, 1), d.first_id - 1) AS ids (g)
        WHERE
            NOT EXISTS (
                SELECT
                    1
                FROM
                    syncapi_output_room_events x
                WHERE
                    x.id = ids.g)) AS free_id,
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
            max(m.id)
        FROM
            syncapi_output_room_events m
        WHERE
            m.room_id = d.room_id) AS room_last_id,
    d.rn
FROM
    diag d
ORDER BY
    d.create_id;

\echo '--- visible late creates with no free ID in the search window ---'
WITH room_first AS (
    SELECT
        room_id,
        min(id) AS first_id
    FROM
        syncapi_output_room_events
    GROUP BY
        room_id
),
diag AS (
    SELECT
        o.room_id,
        o.event_id,
        f.first_id
    FROM
        syncapi_output_room_events o
        JOIN room_first f ON f.room_id = o.room_id
    WHERE
        o.type = 'm.room.create'
        AND o.exclude_from_sync = FALSE
        AND o.id > f.first_id
)
SELECT
    d.room_id,
    d.event_id,
    d.first_id
FROM
    diag d
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            generate_series(greatest (d.first_id - :FREE_WINDOW, 1), d.first_id - 1) AS ids (g)
        WHERE
            NOT EXISTS (
                SELECT
                    1
                FROM
                    syncapi_output_room_events x
                WHERE
                    x.id = ids.g))
    ORDER BY
        d.room_id;

\echo '--- rooms with multiple visible late creates ---'
WITH room_first AS (
    SELECT
        room_id,
        min(id) AS first_id
    FROM
        syncapi_output_room_events
    GROUP BY
        room_id
)
SELECT
    o.room_id,
    count(*) AS late_creates
FROM
    syncapi_output_room_events o
    JOIN room_first f ON f.room_id = o.room_id
WHERE
    o.type = 'm.room.create'
    AND o.exclude_from_sync = FALSE
    AND o.id > f.first_id
GROUP BY
    o.room_id
HAVING
    count(*) > 1
ORDER BY
    o.room_id;

\echo '--- late creates already hidden from sync (reposition leaves these alone) ---'
WITH room_first AS (
    SELECT
        room_id,
        min(id) AS first_id
    FROM
        syncapi_output_room_events
    GROUP BY
        room_id
)
SELECT
    o.room_id,
    o.event_id,
    o.id AS create_id,
    f.first_id,
    to_timestamp(o.origin_server_ts / 1000.0) AT TIME ZONE 'UTC' AS create_ts
FROM
    syncapi_output_room_events o
    JOIN room_first f ON f.room_id = o.room_id
WHERE
    o.type = 'm.room.create'
    AND o.id > f.first_id
    AND o.exclude_from_sync = TRUE
ORDER BY
    o.id;

ROLLBACK;
