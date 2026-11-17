-- Read-only inventory of the two different repair backlogs.
--
-- This answers "how many events are still affected", without touching a row.
-- There is no allowlist and no event ID in here: every count and listing is
-- derived from the tables.
--
-- Run interactively (or with psql -f; it never writes):
--     \set ON_ERROR_STOP on
--     \i scripts/audit_repair_backlog.sql
--
-- The two classes are not the same bug and must not be repaired the same way:
--
--   1. VISIBILITY. Rows in syncapi_output_room_events with
--      exclude_from_sync = TRUE. Only the ones whose event is still the
--      current state for its (type, state_key) are wrong: those are live
--      state events that /sync will never show. The rest are history that
--      was deliberately kept out of the live timeline and belongs in
--      /messages, not /sync. Un-hiding those would flood timelines.
--
--   2. CONTENT. Rows whose roomserver_event_json lost its content to a failed
--      content-hash check (a pruned copy with no unsigned.redacted_because).
--      Split again by whether the sync API still has a richer copy (locally
--      repairable) or not (needs refetchPruned from another server).
--
-- Neither repair moves a stream position, so an already-synced client will
-- not see either repair through incremental /sync. That is a separate,
-- per-client problem (full resync) and is not counted here.

\pset pager off
\pset footer on

\echo ''
\echo '=== 1a. hidden output rows: current state (wrongly hidden) vs history ==='
WITH hidden AS (
  SELECT o.id, o.room_id, o.event_id, o.type
  FROM syncapi_output_room_events o
  WHERE o.exclude_from_sync = TRUE
)
SELECT count(*) AS hidden_total,
       count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM syncapi_current_room_state s
         WHERE s.room_id = h.room_id AND s.event_id = h.event_id
       )) AS hidden_but_current_state,
       count(*) FILTER (WHERE NOT EXISTS (
         SELECT 1 FROM syncapi_current_room_state s
         WHERE s.room_id = h.room_id AND s.event_id = h.event_id
       )) AS hidden_history
FROM hidden h;

\echo ''
\echo '=== 1b. wrongly hidden current-state rows, by room (expect empty) ==='
SELECT o.room_id, o.type, count(*) AS wrongly_hidden, min(o.id) AS first_id, max(o.id) AS last_id
FROM syncapi_output_room_events o
JOIN syncapi_current_room_state s ON s.room_id = o.room_id AND s.event_id = o.event_id
WHERE o.exclude_from_sync = TRUE
GROUP BY o.room_id, o.type
ORDER BY wrongly_hidden DESC;

\echo ''
\echo '=== 1c. hidden state rows after each room''s latest join, by room ==='
-- These are the ones a gap-fill/resync could have created. Not proof; the
-- stream ids and the room's membership history decide, so review per room.
WITH last_join AS (
  SELECT room_id, max(id) AS join_id
  FROM syncapi_output_room_events
  WHERE type = 'm.room.member'
    AND headered_event_json::jsonb #>> '{content,membership}' = 'join'
  GROUP BY room_id
)
SELECT o.room_id, count(*) AS hidden_state_after_last_join,
       min(o.id) AS first_id, max(o.id) AS last_id
FROM syncapi_output_room_events o
JOIN last_join lj ON lj.room_id = o.room_id
WHERE o.exclude_from_sync = TRUE
  AND o.type <> 'm.room.member'
  AND o.id > lj.join_id
GROUP BY o.room_id
ORDER BY hidden_state_after_last_join DESC;

\echo ''
\echo '=== 2a. content backlogs: roomserver pruned vs sync richer vs both pruned ==='
WITH cmp AS (
  SELECT o.event_id,
         (j.event_json::jsonb -> 'content') AS rs_content,
         (o.headered_event_json::jsonb -> 'content') AS sync_content,
         j.event_json::jsonb #> '{unsigned,redacted_because}' AS rs_rb,
         o.headered_event_json::jsonb #> '{unsigned,redacted_because}' AS sync_rb
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
)
SELECT count(*) FILTER (
         WHERE rs_rb IS NULL AND sync_rb IS NULL
           AND rs_content IS DISTINCT FROM sync_content
           AND sync_content @> rs_content
       ) AS locally_repairable,
       count(*) FILTER (
         WHERE rs_rb IS NULL AND sync_rb IS NULL
           AND rs_content IS DISTINCT FROM sync_content
           AND NOT (sync_content @> rs_content)
       ) AS needs_review,
       count(*) FILTER (
         WHERE rs_rb IS NOT NULL OR sync_rb IS NOT NULL
           AND rs_content IS DISTINCT FROM sync_content
       ) AS genuine_redaction_mismatch
FROM cmp;

\echo ''
\echo '=== 2b. locally repairable rows, by event type ==='
SELECT o.type, count(*) AS locally_repairable
FROM syncapi_output_room_events o
JOIN roomserver_events e ON e.event_id = o.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (o.headered_event_json::jsonb -> 'content')
  AND (o.headered_event_json::jsonb -> 'content') @> (j.event_json::jsonb -> 'content')
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
GROUP BY o.type
ORDER BY locally_repairable DESC;

\echo ''
\echo '=== 2c. same, for current state (these change what /state returns) ==='
SELECT s.type, count(*) AS locally_repairable_current_state
FROM syncapi_current_room_state s
JOIN roomserver_events e ON e.event_id = s.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (s.headered_event_json::jsonb -> 'content')
  AND (s.headered_event_json::jsonb -> 'content') @> (j.event_json::jsonb -> 'content')
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND s.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
GROUP BY s.type
ORDER BY locally_repairable_current_state DESC;

\echo ''
\echo '=== 2d. per-room totals for the backlog (output rows) ==='
SELECT o.room_id, count(*) AS affected_output_rows
FROM syncapi_output_room_events o
JOIN roomserver_events e ON e.event_id = o.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (o.headered_event_json::jsonb -> 'content')
  AND o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
GROUP BY o.room_id
ORDER BY affected_output_rows DESC;

\echo ''
\echo '=== 2e. event IDs for POST /_zendrite/admin/refetchPruned (batches of 100) ==='
-- Output rows only. Drop the ::text if you want to pipe it.
SELECT o.event_id::text
FROM syncapi_output_room_events o
JOIN roomserver_events e ON e.event_id = o.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (o.headered_event_json::jsonb -> 'content')
  AND o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
ORDER BY o.id;
