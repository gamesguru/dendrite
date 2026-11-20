-- READ-ONLY cross-store scan of room creation delivery for every room known to
-- roomserver or syncapi. It reports missing/misplaced creates, roomserver
-- incarnation/output mismatches, current-state gaps, and stored member display
-- name coverage. Per-client response behavior still requires a /sync capture.
--
--     psql -X -f scripts/diagnose_room_create_delivery.sql <dsn>
--
-- Persistent tables are only read. A temporary report table is dropped by the
-- final rollback.
\pset pager off
\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE zz_room_create_delivery_scan ON COMMIT DROP AS
WITH room_ids AS (
    SELECT
        room_id
    FROM
        roomserver_rooms
    UNION
    SELECT
        j.event_json::jsonb ->> 'room_id'
    FROM
        roomserver_events e
        JOIN roomserver_event_types et ON et.event_type_nid = e.event_type_nid
            AND et.event_type = 'm.room.create'
        JOIN roomserver_event_json j USING (event_nid)
    WHERE
        j.event_json::jsonb ->> 'room_id' IS NOT NULL
    UNION
    SELECT
        room_id
    FROM
        syncapi_output_room_events
    UNION
    SELECT
        room_id
    FROM
        syncapi_current_room_state
),
live_rooms AS (
    SELECT
        room_id,
        room_nid,
        state_snapshot_nid,
        last_event_sent_nid
    FROM
        roomserver_rooms
),
sync_room_roll AS (
    SELECT
        room_id,
        count(*) AS sync_events,
        count(*) FILTER (WHERE NOT exclude_from_sync) AS sync_visible_events,
    count(*) FILTER (WHERE exclude_from_sync) AS sync_hidden_events,
    min(id) AS first_sync_id
FROM
    syncapi_output_room_events
GROUP BY
    room_id
),
sync_topology_roll AS (
    SELECT
        room_id,
        min(topological_position) AS min_topology
    FROM
        syncapi_output_room_events_topology
    GROUP BY
        room_id
),
sync_create_rows AS (
    SELECT
        o.room_id,
        o.event_id,
        o.id AS create_id,
        o.exclude_from_sync AS hidden,
        o.origin_server_ts,
        t.topological_position AS create_topology,
        (s.event_id IS NOT NULL) AS in_sync_current_state,
        (rse.event_id IS NOT NULL) AS in_roomserver
    FROM
        syncapi_output_room_events o
        LEFT JOIN syncapi_output_room_events_topology t USING (event_id)
        LEFT JOIN syncapi_current_room_state s ON s.room_id = o.room_id
            AND s.event_id = o.event_id
            AND s.type = 'm.room.create'
            AND s.state_key = ''
        LEFT JOIN roomserver_events rse ON rse.event_id = o.event_id
    WHERE
        o.type = 'm.room.create'
),
sync_create_flags AS (
    SELECT
        c.*,
        rr.first_sync_id,
        tr.min_topology,
        EXISTS (
            SELECT
                1
            FROM
                syncapi_output_room_events_topology ht
                JOIN syncapi_output_room_events he ON he.event_id = ht.event_id
            WHERE
                ht.room_id = c.room_id
                AND c.create_topology IS NOT NULL
                AND ht.topological_position > c.create_topology
                AND he.exclude_from_sync) AS hidden_after_create,
        EXISTS (
            SELECT
                1
            FROM
                syncapi_output_room_events_topology vt
                JOIN syncapi_output_room_events ve ON ve.event_id = vt.event_id
            WHERE
                vt.room_id = c.room_id
                AND c.create_topology IS NOT NULL
                AND vt.topological_position < c.create_topology
                AND NOT ve.exclude_from_sync) AS visible_before_create
    FROM
        sync_create_rows c
        JOIN sync_room_roll rr USING (room_id)
        LEFT JOIN sync_topology_roll tr USING (room_id)
),
sync_create_roll AS (
    SELECT
        room_id,
        count(*) AS sync_create_rows,
        count(*) FILTER (WHERE hidden) AS hidden_sync_creates,
        count(*) FILTER (WHERE create_id > first_sync_id) AS stream_late_creates,
        count(*) FILTER (WHERE create_topology IS NOT NULL
                AND min_topology IS NOT NULL
                AND create_topology > min_topology) AS topology_late_creates,
            count(*) FILTER (WHERE create_topology IS NULL) AS creates_without_topology,
            count(*) FILTER (WHERE hidden_after_create) AS creates_with_hidden_history_after,
            count(*) FILTER (WHERE visible_before_create) AS creates_with_visible_history_before,
            count(*) FILTER (WHERE NOT in_sync_current_state) AS creates_missing_from_sync_state,
            count(*) FILTER (WHERE NOT in_roomserver) AS creates_missing_from_roomserver,
            array_agg(event_id ORDER BY create_id) AS sync_create_event_ids,
            array_agg(event_id ORDER BY create_id) FILTER (WHERE NOT hidden
                AND hidden_after_create) AS window_mismatch_event_ids,
            array_agg(event_id ORDER BY create_id) FILTER (WHERE create_id > first_sync_id) AS stream_late_event_ids
        FROM
            sync_create_flags
    GROUP BY
        room_id
),
roomserver_create_rows AS (
    SELECT
        j.event_json::jsonb ->> 'room_id' AS room_id,
        e.event_id,
        e.event_nid,
        e.room_nid,
        live.room_nid AS live_room_nid,
        live.room_id AS registered_live_room_id,
        e.depth,
        e.sent_to_output,
        e.is_rejected,
        e.state_snapshot_nid,
        sk.event_state_key,
        j.event_json::jsonb ->> 'state_key' AS json_state_key
    FROM
        roomserver_events e
        JOIN roomserver_event_types et ON et.event_type_nid = e.event_type_nid
            AND et.event_type = 'm.room.create'
        JOIN roomserver_event_json j USING (event_nid)
        LEFT JOIN roomserver_rooms live ON live.room_id = j.event_json::jsonb ->> 'room_id'
        LEFT JOIN roomserver_event_state_keys sk ON sk.event_state_key_nid = e.event_state_key_nid
),
roomserver_create_roll AS (
    SELECT
        room_id,
        count(*) AS roomserver_create_rows,
        count(*) FILTER (WHERE room_nid = live_room_nid) AS live_nid_create_rows,
        count(*) FILTER (WHERE live_room_nid IS NULL
                OR room_nid <> live_room_nid) AS orphan_nid_create_rows,
            count(*) FILTER (WHERE NOT sent_to_output) AS not_sent_to_output_rows,
            count(*) FILTER (WHERE is_rejected) AS rejected_create_rows,
            count(*) FILTER (WHERE event_state_key IS DISTINCT FROM ''
                OR json_state_key IS DISTINCT FROM '') AS create_state_key_mismatches,
            count(*) FILTER (WHERE room_nid = live_room_nid
                AND NOT sent_to_output) AS live_create_not_sent_rows,
            array_agg(event_id ORDER BY event_nid) FILTER (WHERE live_room_nid IS NULL
                OR room_nid <> live_room_nid) AS orphan_create_event_ids,
            array_agg(event_id ORDER BY event_nid) FILTER (WHERE room_nid = live_room_nid
                AND NOT sent_to_output) AS live_create_not_sent_event_ids
        FROM
            roomserver_create_rows
    GROUP BY
        room_id
),
sync_state_roll AS (
    SELECT
        room_id,
        count(*) FILTER (WHERE type = 'm.room.create'
            AND state_key = '') AS current_create_rows,
        count(*) FILTER (WHERE type = 'm.room.member'
            AND membership = 'join') AS joined_member_rows,
        count(*) FILTER (WHERE type = 'm.room.member'
            AND membership = 'join'
            AND coalesce(headered_event_json::jsonb -> 'content' ->> 'displayname', '') <> '') AS joined_members_with_displayname,
            count(*) FILTER (WHERE type = 'm.room.member'
                AND membership = 'join'
                AND coalesce(headered_event_json::jsonb -> 'content' ->> 'displayname', '') = '') AS joined_members_without_displayname FROM syncapi_current_room_state
            GROUP BY
                room_id
)
            SELECT
                ids.room_id,
                live.room_nid AS live_room_nid,
                live.state_snapshot_nid,
                live.last_event_sent_nid,
                coalesce(sr.sync_events, 0) AS sync_events,
                sr.first_sync_id,
                coalesce(sr.sync_visible_events, 0) AS sync_visible_events,
                coalesce(sr.sync_hidden_events, 0) AS sync_hidden_events,
                coalesce(sc.sync_create_rows, 0) AS sync_create_rows,
                coalesce(sc.hidden_sync_creates, 0) AS hidden_sync_creates,
                coalesce(sc.stream_late_creates, 0) AS stream_late_creates,
                coalesce(sc.topology_late_creates, 0) AS topology_late_creates,
                coalesce(sc.creates_without_topology, 0) AS creates_without_topology,
                coalesce(sc.creates_with_hidden_history_after, 0) AS creates_with_hidden_history_after,
                coalesce(sc.creates_with_visible_history_before, 0) AS creates_with_visible_history_before,
                coalesce(sc.creates_missing_from_sync_state, 0) AS creates_missing_from_sync_state,
                coalesce(sc.creates_missing_from_roomserver, 0) AS creates_missing_from_roomserver,
                (coalesce(sc.sync_create_rows, 0) = 1
                AND coalesce(sc.hidden_sync_creates, 0) = 0
                AND coalesce(sr.sync_visible_events, 0) BETWEEN 2 AND 10
                AND coalesce(sc.topology_late_creates, 0) = 0
                AND coalesce(sc.creates_with_visible_history_before, 0) = 0
                AND coalesce(sc.creates_with_hidden_history_after, 0) > 0) AS default_window_mismatch_candidate,
            coalesce(sc.sync_create_event_ids, ARRAY[]::text[]) AS sync_create_event_ids,
            coalesce(sc.window_mismatch_event_ids, ARRAY[]::text[]) AS window_mismatch_event_ids,
            coalesce(sc.stream_late_event_ids, ARRAY[]::text[]) AS stream_late_event_ids,
            coalesce(rc.roomserver_create_rows, 0) AS roomserver_create_rows,
            coalesce(rc.live_nid_create_rows, 0) AS live_nid_create_rows,
            coalesce(rc.orphan_nid_create_rows, 0) AS orphan_nid_create_rows,
            coalesce(rc.not_sent_to_output_rows, 0) AS not_sent_to_output_rows,
            coalesce(rc.live_create_not_sent_rows, 0) AS live_create_not_sent_rows,
            coalesce(rc.rejected_create_rows, 0) AS rejected_create_rows,
            coalesce(rc.create_state_key_mismatches, 0) AS create_state_key_mismatches,
            coalesce(rc.orphan_create_event_ids, ARRAY[]::text[]) AS orphan_create_event_ids,
            coalesce(rc.live_create_not_sent_event_ids, ARRAY[]::text[]) AS live_create_not_sent_event_ids,
            coalesce(ss.current_create_rows, 0) AS current_create_rows,
            coalesce(ss.joined_member_rows, 0) AS joined_member_rows,
            coalesce(ss.joined_members_with_displayname, 0) AS joined_members_with_displayname,
            coalesce(ss.joined_members_without_displayname, 0) AS joined_members_without_displayname FROM room_ids ids
        LEFT JOIN live_rooms live USING (room_id)
        LEFT JOIN sync_room_roll sr USING (room_id)
        LEFT JOIN sync_create_roll sc USING (room_id)
        LEFT JOIN roomserver_create_roll rc USING (room_id)
        LEFT JOIN sync_state_roll ss USING (room_id);
\echo '--- all-room coverage summary ---'
SELECT
    count(*) AS rooms_known_to_either_store,
    count(*) FILTER (WHERE live_room_nid IS NOT NULL) AS registered_roomserver_rooms,
    count(*) FILTER (WHERE sync_events > 0) AS rooms_with_sync_events,
    count(*) FILTER (WHERE sync_create_rows = 0) AS rooms_without_sync_create_row,
    count(*) FILTER (WHERE roomserver_create_rows = 0) AS rooms_without_roomserver_create_row,
    count(*) FILTER (WHERE sync_create_rows > 0
        AND roomserver_create_rows = 0) AS sync_creates_missing_from_roomserver,
    count(*) FILTER (WHERE orphan_nid_create_rows > 0) AS rooms_with_orphan_nid_create,
    count(*) FILTER (WHERE live_create_not_sent_rows > 0) AS rooms_with_live_create_not_emitted,
    count(*) FILTER (WHERE stream_late_creates > 0) AS rooms_with_stream_late_create,
    count(*) FILTER (WHERE topology_late_creates > 0) AS rooms_with_topology_late_create,
    count(*) FILTER (WHERE creates_with_hidden_history_after > 0) AS rooms_with_hidden_history_after_create,
    count(*) FILTER (WHERE default_window_mismatch_candidate) AS default_window_mismatch_candidates,
    count(*) FILTER (WHERE creates_missing_from_sync_state > 0) AS rooms_with_sync_create_missing_from_current_state
FROM
    zz_room_create_delivery_scan;
\echo '--- all rooms with a create-delivery, ordering, or member-state anomaly ---'
SELECT
    *
FROM
    zz_room_create_delivery_scan
WHERE
    sync_create_rows = 0
    OR roomserver_create_rows = 0
    OR orphan_nid_create_rows > 0
    OR live_create_not_sent_rows > 0
    OR rejected_create_rows > 0
    OR create_state_key_mismatches > 0
    OR sync_create_rows > 1
    OR hidden_sync_creates > 0
    OR stream_late_creates > 0
    OR topology_late_creates > 0
    OR creates_without_topology > 0
    OR creates_with_hidden_history_after > 0
    OR creates_with_visible_history_before > 0
    OR creates_missing_from_sync_state > 0
    OR creates_missing_from_roomserver > 0
    OR default_window_mismatch_candidate
ORDER BY
    CASE WHEN creates_with_hidden_history_after > 0 THEN
        0
    ELSE
        1
    END,
    CASE WHEN orphan_nid_create_rows > 0 THEN
        0
    ELSE
        1
    END,
    sync_events DESC,
    room_id;
\echo '--- joined-member display-name storage (unset names can be intentional) ---'
SELECT
    room_id,
    live_room_nid,
    joined_member_rows,
    joined_members_with_displayname,
    joined_members_without_displayname
FROM
    zz_room_create_delivery_scan
WHERE
    joined_members_without_displayname > 0
ORDER BY
    joined_members_without_displayname DESC,
    room_id;
\echo '--- sync creates in rooms with no registered roomserver room ---'
SELECT
    room_id,
    sync_create_event_ids,
    sync_events,
    stream_late_creates,
    topology_late_creates
FROM
    zz_room_create_delivery_scan
WHERE
    sync_create_rows > 0
    AND live_room_nid IS NULL
ORDER BY
    room_id;
ROLLBACK;
