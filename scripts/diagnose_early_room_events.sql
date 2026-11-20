-- READ-ONLY scan of the first EARLY_N events (by topological position) of every
-- room that has an m.room.create in syncapi. Reposition fixes the create only.
-- A room whose early config events are hidden from sync (exclude_from_sync) or
-- sit before the create in stream order shows them above the create in clients,
-- because they reach the client through /messages, not the sync timeline. The
-- create then is not the first event and clients drop the creation summary.
--
--     psql -X -f scripts/diagnose_early_room_events.sql <dsn>
--
-- Hidden early events can be legitimate. Review the list; this script changes
-- nothing.
\pset pager off
\set ON_ERROR_STOP on
\set EARLY_N 15
BEGIN
    READ ONLY;

\echo '--- summary ---'
WITH early AS (
    SELECT
        o.room_id,
        o.event_id,
        o.id,
        o.type,
        o.exclude_from_sync AS hidden,
        t.topological_position AS topo,
        row_number() OVER (PARTITION BY o.room_id ORDER BY t.topological_position,
            t.stream_position) AS rn
    FROM
        syncapi_output_room_events o
        JOIN syncapi_output_room_events_topology t USING (event_id)
),
cr AS (
    SELECT
        room_id,
        min(id) AS create_id
    FROM
        syncapi_output_room_events
    WHERE
        type = 'm.room.create'
    GROUP BY
        room_id
),
per_room AS (
    SELECT
        e.room_id,
        count(*) AS early_n,
        count(*) FILTER (WHERE e.hidden) AS hidden_early,
        count(*) FILTER (WHERE e.id < c.create_id) AS before_create
    FROM
        early e
        JOIN cr c USING (room_id)
    WHERE
        e.rn <= :EARLY_N
    GROUP BY
        e.room_id
)
SELECT
    count(*) AS rooms_scanned,
    count(*) FILTER (WHERE hidden_early > 0) AS rooms_with_hidden_early,
    count(*) FILTER (WHERE before_create > 0) AS rooms_with_event_before_create,
    count(*) FILTER (WHERE hidden_early > 0
        AND before_create > 0) AS rooms_with_both
FROM
    per_room;

\echo '--- rooms with hidden or pre-create early events ---'
WITH early AS (
    SELECT
        o.room_id,
        o.id,
        o.type,
        o.exclude_from_sync AS hidden,
        t.topological_position AS topo,
        row_number() OVER (PARTITION BY o.room_id ORDER BY t.topological_position,
            t.stream_position) AS rn
    FROM
        syncapi_output_room_events o
        JOIN syncapi_output_room_events_topology t USING (event_id)
),
cr AS (
    SELECT
        room_id,
        min(id) AS create_id
    FROM
        syncapi_output_room_events
    WHERE
        type = 'm.room.create'
    GROUP BY
        room_id
)
SELECT
    e.room_id,
    c.create_id,
    count(*) AS early_n,
    count(*) FILTER (WHERE e.hidden) AS hidden_early,
    count(*) FILTER (WHERE e.id < c.create_id) AS before_create,
    min(e.id) FILTER (WHERE e.id > c.create_id) AS first_id_after_create,
    max(e.id) AS max_early_id,
    string_agg(e.type, ',' ORDER BY e.topo) FILTER (WHERE e.hidden) AS hidden_types
FROM
    early e
    JOIN cr c USING (room_id)
WHERE
    e.rn <= :EARLY_N
GROUP BY
    e.room_id,
    c.create_id
HAVING
    count(*) FILTER (WHERE e.hidden) > 0
    OR count(*) FILTER (WHERE e.id < c.create_id) > 0
ORDER BY
    hidden_early DESC,
    e.room_id;

ROLLBACK;
