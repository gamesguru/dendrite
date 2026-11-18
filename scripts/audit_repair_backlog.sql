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
\echo '=== 1a. hidden output rows: repair candidates vs intentionally hidden ==='
-- Top-level state_key is detected in the text, not through JSON/JSONB: both
-- PostgreSQL JSON types reject \u0000 and lone-surrogate escapes that valid
-- event content may contain. Canonical JSON sorts keys, so top-level
-- sender/signatures precede state_key, which immediately precedes type;
-- content is before sender and unsigned after type, so neither can match.
WITH live_start AS (
  SELECT t.room_id, min(t.topological_position) AS live_start
  FROM syncapi_output_room_events_topology t
  JOIN syncapi_output_room_events e ON e.event_id = t.event_id
  CROSS JOIN LATERAL (
    SELECT right(
             e.headered_event_json,
             LEAST(
               NULLIF(strpos(reverse(e.headered_event_json), reverse('"signatures":')), 0),
               NULLIF(strpos(reverse(e.headered_event_json), reverse('"sender":')), 0)
             )
           ) AS suffix
  ) sk
  WHERE e.exclude_from_sync = FALSE
    AND e.type <> 'm.room.create'
    AND substring(sk.suffix FROM 1 FOR NULLIF(strpos(sk.suffix, '"type":'), 0) - 1)
        ~ '"state_key":"([^"\\]|\\.)*",$'
  GROUP BY t.room_id
), hidden AS (
  SELECT o.id, o.room_id, o.event_id, o.type, t.topological_position, l.live_start,
         (s.event_id IS NOT NULL) AS is_current
  FROM syncapi_output_room_events o
  LEFT JOIN syncapi_current_room_state s
    ON s.room_id = o.room_id AND s.event_id = o.event_id
  LEFT JOIN syncapi_output_room_events_topology t ON t.event_id = o.event_id
  LEFT JOIN live_start l ON l.room_id = o.room_id
  WHERE o.exclude_from_sync = TRUE
)
SELECT count(*) AS hidden_total,
       count(*) FILTER (WHERE is_current
         AND type <> 'm.room.create'
         AND live_start IS NOT NULL
         AND topological_position >= live_start
       ) AS repair_candidates,
       count(*) FILTER (WHERE NOT is_current) AS hidden_history,
       count(*) FILTER (WHERE is_current
         AND (type = 'm.room.create' OR live_start IS NULL OR topological_position IS NULL OR topological_position < live_start)
       ) AS intentionally_hidden_current_state
FROM hidden h;

\echo ''
\echo '=== 1b. wrongly hidden current-state rows, by room (expect empty) ==='
WITH live_start AS (
  SELECT t.room_id, min(t.topological_position) AS live_start
  FROM syncapi_output_room_events_topology t
  JOIN syncapi_output_room_events e ON e.event_id = t.event_id
  CROSS JOIN LATERAL (
    SELECT right(
             e.headered_event_json,
             LEAST(
               NULLIF(strpos(reverse(e.headered_event_json), reverse('"signatures":')), 0),
               NULLIF(strpos(reverse(e.headered_event_json), reverse('"sender":')), 0)
             )
           ) AS suffix
  ) sk
  WHERE e.exclude_from_sync = FALSE
    AND e.type <> 'm.room.create'
    AND substring(sk.suffix FROM 1 FOR NULLIF(strpos(sk.suffix, '"type":'), 0) - 1)
        ~ '"state_key":"([^"\\]|\\.)*",$'
  GROUP BY t.room_id
)
SELECT o.room_id, o.type, count(*) AS wrongly_hidden, min(o.id) AS first_id, max(o.id) AS last_id
FROM syncapi_output_room_events o
JOIN syncapi_current_room_state s ON s.room_id = o.room_id AND s.event_id = o.event_id
JOIN syncapi_output_room_events_topology t ON t.event_id = o.event_id
JOIN live_start l ON l.room_id = o.room_id
WHERE o.exclude_from_sync = TRUE
  AND o.type <> 'm.room.create'
  AND t.topological_position >= l.live_start
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
CROSS JOIN LATERAL (
  SELECT right(
           o.headered_event_json,
           LEAST(
             NULLIF(strpos(reverse(o.headered_event_json), reverse('"signatures":')), 0),
             NULLIF(strpos(reverse(o.headered_event_json), reverse('"sender":')), 0)
           )
         ) AS suffix
) sk
WHERE o.exclude_from_sync = TRUE
  AND o.type <> 'm.room.member'
  AND substring(sk.suffix FROM 1 FOR NULLIF(strpos(sk.suffix, '"type":'), 0) - 1)
      ~ '"state_key":"([^"\\]|\\.)*",$'
  AND o.id > lj.join_id
GROUP BY o.room_id
ORDER BY hidden_state_after_last_join DESC;

\echo ''
\echo '=== 2a. content backlogs: roomserver pruned vs sync richer vs both pruned ==='
-- Empty content on both sides is how many event types look after a failed
-- content-hash check, but an event can legitimately have empty content. These
-- rows have no richer local copy to repair from, so they are counted
-- separately and verified in 2e before they can become refetch requests.
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
         WHERE rs_rb IS NULL AND sync_rb IS NULL
           AND jsonb_typeof(rs_content) = 'object'
           AND rs_content = '{}'::jsonb
           AND sync_content = '{}'::jsonb
       ) AS empty_content_needs_verification,
       count(*) FILTER (
         WHERE (rs_rb IS NOT NULL OR sync_rb IS NOT NULL)
           AND rs_content IS DISTINCT FROM sync_content
       ) AS genuine_redaction_mismatch
FROM cmp;

\echo ''
\echo '=== 2b. content repair candidates, by event type ==='
-- Only rows where the sync copy is strictly richer than the roomserver copy.
-- Rows where both sides are empty have no richer local copy; they are counted
-- as empty_content_needs_verification in 2a and listed for verification in 2e.
SELECT o.type, count(*) AS locally_repairable
FROM syncapi_output_room_events o
JOIN roomserver_events e ON e.event_id = o.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND (j.event_json::jsonb -> 'content') IS DISTINCT FROM (o.headered_event_json::jsonb -> 'content')
  AND (o.headered_event_json::jsonb -> 'content') @> (j.event_json::jsonb -> 'content')
GROUP BY o.type
ORDER BY locally_repairable DESC;

\echo ''
\echo '=== 2c. same, for current state (these change what /state returns) ==='
SELECT s.type, count(*) AS locally_repairable_current_state
FROM syncapi_current_room_state s
JOIN roomserver_events e ON e.event_id = s.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND s.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND (j.event_json::jsonb -> 'content') IS DISTINCT FROM (s.headered_event_json::jsonb -> 'content')
  AND (s.headered_event_json::jsonb -> 'content') @> (j.event_json::jsonb -> 'content')
GROUP BY s.type
ORDER BY locally_repairable_current_state DESC;

\echo ''
\echo '=== 2d. per-room totals for the backlog (output rows) ==='
SELECT o.room_id, count(*) AS affected_output_rows
FROM syncapi_output_room_events o
JOIN roomserver_events e ON e.event_id = o.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
  AND (
    ((j.event_json::jsonb -> 'content') IS DISTINCT FROM (o.headered_event_json::jsonb -> 'content'))
    OR (jsonb_typeof(j.event_json::jsonb -> 'content') = 'object'
      AND (j.event_json::jsonb -> 'content') = '{}'::jsonb)
  )
GROUP BY o.room_id
ORDER BY affected_output_rows DESC;

\echo ''
\echo '=== 2e. empty-content copies: verification requests (dry run) ==='
-- These are the matching empty-content copies from 2a. SQL cannot recompute
-- the content hash, so this batch only asks the roomserver to run its
-- stored-copy check. Results of already_full are not pruned and must not be
-- sent again; only would_refetch/refetched results are confirmed hash-pruned
-- and may be repeated with dry_run false.
WITH candidates AS (
  SELECT o.id,
         o.room_id,
         o.event_id,
         row_number() OVER (ORDER BY o.id) - 1 AS n
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
  WHERE o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
    AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
    AND jsonb_typeof(j.event_json::jsonb -> 'content') = 'object'
    AND (j.event_json::jsonb -> 'content') = '{}'::jsonb
    AND (o.headered_event_json::jsonb -> 'content') = '{}'::jsonb
)
SELECT (n / 100) + 1 AS batch_number,
       jsonb_build_object(
         'dry_run', true,
         'events', jsonb_agg(jsonb_build_object('room_id', room_id, 'event_id', event_id) ORDER BY n)
       ) AS request_body
FROM candidates
GROUP BY n / 100
ORDER BY batch_number;

\echo ''
\echo '=== 2f. event IDs for POST /_zendrite/admin/refetchPruned (batches of 100) ==='
-- Each result is a complete request body and has at most 100 event objects.
-- Locally repairable rows are excluded: sync content already contains the
-- roomserver content, so these do not need a federation refetch. Empty-content
-- copies are excluded here too; 2e must confirm they are hash-pruned first.
WITH candidates AS (
  SELECT o.id,
         o.room_id,
         o.event_id,
         row_number() OVER (ORDER BY o.id) - 1 AS n
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
  WHERE (j.event_json::jsonb -> 'content') IS DISTINCT FROM (o.headered_event_json::jsonb -> 'content')
    AND NOT ((o.headered_event_json::jsonb -> 'content') @> (j.event_json::jsonb -> 'content'))
    AND (j.event_json::jsonb -> 'content') IS DISTINCT FROM '{}'::jsonb
    AND o.headered_event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
    AND j.event_json::jsonb #> '{unsigned,redacted_because}' IS NULL
)
SELECT (n / 100) + 1 AS batch_number,
       jsonb_build_object(
         'dry_run', true,
         'events', jsonb_agg(jsonb_build_object('room_id', room_id, 'event_id', event_id) ORDER BY n)
       ) AS request_body
FROM candidates
GROUP BY n / 100
ORDER BY batch_number;
