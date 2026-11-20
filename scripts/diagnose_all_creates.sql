-- READ-ONLY scan of every m.room.create row, not just late ones. It classifies
-- each create by the property that actually decides how clients render it.
--
--     psql -X -f scripts/diagnose_all_creates.sql <dsn>
--
-- It reads persistent tables only. The one thing it writes is a TEMP table, and
-- the transaction rolls back at the end, so nothing is changed.
--
-- Classes:
--   duplicate_create   more than one create in the room
--   no_topo_row        create has no syncapi_output_room_events_topology row
--   hidden_stream_late create hidden from sync and still after the room's first
--                      stream event (the reposition leaves hidden rows alone)
--   hidden_backfill    create hidden from sync; it reaches clients through
--                      /messages at the end of history (normally renders first)
--   stream_late        create visible to sync but stored after the room's first
--                      stream event (the reposition script targets this)
--   topo_late          create's topological position is not the room minimum
--   window_mismatch    create is visible AND hidden events sit above it in
--                      topology. The create is in the live /sync window while
--                      the hidden history is paginated in above it, so clients
--                      bury the create and drop the "created the room" tile.
--                      No stream reposition fixes this shape.
--   not_first_visible  a visible event is topologically before the create
--   clean              none of the above
\pset pager off
\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE zz_create_scan ON COMMIT DROP AS
WITH room_roll AS (
    SELECT
        o.room_id,
        count(*) AS n_events,
        count(*) FILTER (WHERE o.exclude_from_sync) AS n_hidden,
        count(*) FILTER (WHERE NOT o.exclude_from_sync) AS n_visible,
        min(o.id) AS first_id
    FROM
        syncapi_output_room_events o
    GROUP BY
        o.room_id
),
topo_roll AS (
    SELECT
        t.room_id,
        min(t.topological_position) AS min_topo
    FROM
        syncapi_output_room_events_topology t
    GROUP BY
        t.room_id
),
creates AS (
    SELECT
        o.room_id,
        o.event_id,
        o.id AS create_id,
        o.exclude_from_sync AS hidden,
        o.origin_server_ts AS create_ts,
        t.topological_position AS create_topo
    FROM
        syncapi_output_room_events o
        LEFT JOIN syncapi_output_room_events_topology t ON t.event_id = o.event_id
    WHERE
        o.type = 'm.room.create'
),
create_counts AS (
    SELECT
        room_id,
        count(*) AS creates_in_room
    FROM
        creates
    GROUP BY
        room_id
),
flags AS (
    SELECT
        c.room_id,
        c.event_id,
        c.create_id,
        c.hidden,
        c.create_ts,
        c.create_topo,
        cc.creates_in_room,
        r.first_id,
        r.n_events,
        r.n_visible,
        r.n_hidden,
        tr.min_topo,
        EXISTS (
            SELECT
                1
            FROM
                syncapi_output_room_events_topology t2
                JOIN syncapi_output_room_events e2 ON e2.event_id = t2.event_id
            WHERE
                t2.room_id = c.room_id
                AND c.create_topo IS NOT NULL
                AND t2.topological_position > c.create_topo
                AND e2.exclude_from_sync) AS has_hidden_after,
            EXISTS (
                SELECT
                    1
                FROM
                    syncapi_output_room_events_topology t3
                    JOIN syncapi_output_room_events e3 ON e3.event_id = t3.event_id
                WHERE
                    t3.room_id = c.room_id
                    AND c.create_topo IS NOT NULL
                    AND t3.topological_position < c.create_topo
                    AND NOT e3.exclude_from_sync) AS has_visible_before
            FROM
                creates c
                JOIN room_roll r USING (room_id)
                JOIN create_counts cc USING (room_id)
                LEFT JOIN topo_roll tr USING (room_id))
    SELECT
        f.*, CASE WHEN f.creates_in_room > 1 THEN
            'duplicate_create'
        WHEN f.create_topo IS NULL THEN
            'no_topo_row'
        WHEN f.hidden
            AND f.create_id > f.first_id THEN
            'hidden_stream_late'
        WHEN f.hidden THEN
            'hidden_backfill'
        WHEN f.create_id > f.first_id THEN
            'stream_late'
        WHEN f.create_topo > f.min_topo THEN
            'topo_late'
        WHEN f.has_hidden_after THEN
            'window_mismatch'
        WHEN f.has_visible_before THEN
            'not_first_visible'
        ELSE
            'clean'
        END AS class
    FROM
        flags f;
\echo '--- summary: every create in the database ---'
SELECT
    count(*) AS creates,
    count(DISTINCT room_id) AS rooms,
    count(*) FILTER (WHERE hidden) AS hidden_from_sync,
    count(*) FILTER (WHERE NOT hidden) AS visible_to_sync
FROM
    zz_create_scan;
\echo '--- summary by class ---'
SELECT
    class,
    count(*) AS creates,
    count(DISTINCT room_id) AS rooms
FROM
    zz_create_scan
GROUP BY
    class
ORDER BY
    creates DESC,
    class;
\echo '--- every create that is not clean ---'
SELECT
    class,
    room_id,
    event_id,
    create_id,
    first_id,
    create_topo,
    min_topo,
    hidden,
    n_visible,
    n_hidden,
    n_events,
    to_timestamp(create_ts / 1000.0) AT TIME ZONE 'UTC' AS create_ts
FROM
    zz_create_scan
WHERE
    class <> 'clean'
ORDER BY
    class,
    create_id;
\echo '--- window_mismatch detail: visible create with hidden history above it ---'
SELECT
    cs.room_id,
    cs.event_id,
    cs.create_id,
    cs.create_topo,
    cs.n_visible,
    cs.n_hidden,
    cs.n_events,
    (
        SELECT
            count(*)
        FROM
            syncapi_output_room_events_topology t
            JOIN syncapi_output_room_events e ON e.event_id = t.event_id
        WHERE
            t.room_id = cs.room_id
            AND t.topological_position > cs.create_topo
            AND e.exclude_from_sync) AS hidden_after
FROM
    zz_create_scan cs
WHERE
    cs.class = 'window_mismatch'
ORDER BY
    hidden_after DESC,
    cs.create_id;
\echo '--- stream_late detail (reposition candidates) ---'
SELECT
    room_id,
    event_id,
    create_id,
    first_id,
    create_id - first_id AS id_gap,
    n_visible,
    n_hidden,
    to_timestamp(create_ts / 1000.0) AT TIME ZONE 'UTC' AS create_ts
FROM
    zz_create_scan
WHERE
    class = 'stream_late'
ORDER BY
    create_id;
\echo '--- rooms with events but no create row ---'
SELECT
    e.room_id,
    count(*) AS n_events
FROM
    syncapi_output_room_events e
GROUP BY
    e.room_id
HAVING
    NOT bool_or(e.type = 'm.room.create')
ORDER BY
    n_events DESC;
ROLLBACK;
