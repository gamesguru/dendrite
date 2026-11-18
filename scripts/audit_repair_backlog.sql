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
-- PostgreSQL rejects escaped NULs and lone surrogates in JSON, so sanitize
-- those escapes before parsing. JSON extraction checks state_key only at the
-- top level and cannot be confused by nested fields with the same name.
WITH live_start AS (
  SELECT t.room_id, min(t.topological_position) AS live_start
  FROM syncapi_output_room_events_topology t
  JOIN syncapi_output_room_events e ON e.event_id = t.event_id
  WHERE e.exclude_from_sync = FALSE
    AND e.type <> 'm.room.create'
    AND CASE WHEN e.headered_event_json LIKE '%\\u%'
             THEN regexp_replace(e.headered_event_json, '\\u(0000|[dD][89a-fA-F][0-9a-fA-F]{2})', '\\uFFFD', 'g')
             ELSE e.headered_event_json END::json -> 'state_key' IS NOT NULL
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
  WHERE e.exclude_from_sync = FALSE
    AND e.type <> 'm.room.create'
    AND CASE WHEN e.headered_event_json LIKE '%\\u%'
             THEN regexp_replace(e.headered_event_json, '\\u(0000|[dD][89a-fA-F][0-9a-fA-F]{2})', '\\uFFFD', 'g')
             ELSE e.headered_event_json END::json -> 'state_key' IS NOT NULL
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
-- Count membership rows whose JSONB cast is unsafe separately; their
-- membership is unknown. Never normalize escaped NUL or U+FFFD.
SELECT count(*) AS membership_events_needing_jsonb_verification
FROM syncapi_output_room_events
WHERE type = 'm.room.member'
  AND headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})';
WITH last_join AS (
  SELECT room_id, max(id) AS join_id
  FROM syncapi_output_room_events
  WHERE type = 'm.room.member'
    AND CASE WHEN headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})'
             THEN NULL
             ELSE headered_event_json::jsonb #>> '{content,membership}' END = 'join'
  GROUP BY room_id
)
SELECT o.room_id, count(*) AS hidden_state_after_last_join,
       min(o.id) AS first_id, max(o.id) AS last_id
FROM syncapi_output_room_events o
JOIN last_join lj ON lj.room_id = o.room_id
WHERE o.exclude_from_sync = TRUE
  AND o.type <> 'm.room.member'
    AND CASE WHEN o.headered_event_json LIKE '%\\u%'
             THEN regexp_replace(o.headered_event_json, '\\u(0000|[dD][89a-fA-F][0-9a-fA-F]{2})', '\\uFFFD', 'g')
             ELSE o.headered_event_json END::json -> 'state_key' IS NOT NULL
  AND o.id > lj.join_id
GROUP BY o.room_id
ORDER BY hidden_state_after_last_join DESC;

\echo ''
\echo '=== 2a. content backlogs: roomserver pruned vs sync richer vs both pruned ==='
-- Empty content on both sides is how many event types look after a failed
-- content-hash check, but an event can legitimately have empty content. These
-- rows have no richer local copy to repair from, so they are counted
-- separately and verified in 2e before they can become refetch requests.
WITH raw AS (
  SELECT o.event_id, j.event_json AS rs_json, o.headered_event_json AS sync_json
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
), parsed AS (
  SELECT *,
         rs_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' AS rs_unrepresentable,
         sync_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' AS sync_unrepresentable,
         CASE WHEN rs_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})'
              THEN NULL ELSE rs_json::jsonb END AS rs_doc,
         CASE WHEN sync_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})'
              THEN NULL ELSE sync_json::jsonb END AS sync_doc
  FROM raw
), cmp AS (
  SELECT event_id, rs_unrepresentable, sync_unrepresentable,
         rs_doc -> 'content' AS rs_content,
         sync_doc -> 'content' AS sync_content,
         rs_doc #> '{unsigned,redacted_because}' AS rs_rb,
         sync_doc #> '{unsigned,redacted_because}' AS sync_rb
  FROM parsed
)
SELECT count(*) FILTER (
         WHERE rs_rb IS NULL AND sync_rb IS NULL
           AND rs_content IS DISTINCT FROM sync_content
           AND sync_content @> rs_content
           AND NOT COALESCE(jsonb_path_exists(rs_content, 'strict $.** ? (@.type() == "array")'), FALSE)
           AND NOT COALESCE(jsonb_path_exists(sync_content, 'strict $.** ? (@.type() == "array")'), FALSE)
       ) AS locally_repairable,
       count(*) FILTER (
         WHERE rs_rb IS NULL AND sync_rb IS NULL
           AND rs_content IS DISTINCT FROM sync_content
           AND (NOT (sync_content @> rs_content)
             OR COALESCE(jsonb_path_exists(rs_content, 'strict $.** ? (@.type() == "array")'), FALSE)
             OR COALESCE(jsonb_path_exists(sync_content, 'strict $.** ? (@.type() == "array")'), FALSE))
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
       ) AS genuine_redaction_mismatch,
       count(*) FILTER (WHERE rs_unrepresentable OR sync_unrepresentable) AS jsonb_unrepresentable_needs_verification
FROM cmp;

\echo ''
\echo '=== 2b. content repair candidates, by event type ==='
-- Only rows where the sync copy is strictly richer than the roomserver copy.
-- Rows where both sides are empty have no richer local copy; they are counted
-- as empty_content_needs_verification in 2a and listed for verification in 2e.
WITH parsed AS (
  SELECT o.type,
         CASE WHEN j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE j.event_json::jsonb END AS rs_doc,
         CASE WHEN o.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE o.headered_event_json::jsonb END AS sync_doc
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
)
SELECT type, count(*) AS locally_repairable
FROM parsed
WHERE rs_doc IS NOT NULL AND sync_doc IS NOT NULL
  AND rs_doc #> '{unsigned,redacted_because}' IS NULL
  AND sync_doc #> '{unsigned,redacted_because}' IS NULL
  AND (rs_doc -> 'content') IS DISTINCT FROM (sync_doc -> 'content')
  AND (sync_doc -> 'content') @> (rs_doc -> 'content')
  AND NOT COALESCE(jsonb_path_exists(rs_doc -> 'content', 'strict $.** ? (@.type() == "array")'), FALSE)
  AND NOT COALESCE(jsonb_path_exists(sync_doc -> 'content', 'strict $.** ? (@.type() == "array")'), FALSE)
GROUP BY type
ORDER BY locally_repairable DESC;

\echo ''
\echo '=== 2c. same, for current state (these change what /state returns) ==='
SELECT count(*) AS current_state_jsonb_unrepresentable_needs_verification
FROM syncapi_current_room_state s
JOIN roomserver_events e ON e.event_id = s.event_id
JOIN roomserver_event_json j ON j.event_nid = e.event_nid
WHERE j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})'
   OR s.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})';
WITH parsed AS (
  SELECT s.type,
         CASE WHEN j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE j.event_json::jsonb END AS rs_doc,
         CASE WHEN s.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE s.headered_event_json::jsonb END AS sync_doc
  FROM syncapi_current_room_state s
  JOIN roomserver_events e ON e.event_id = s.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
)
SELECT type, count(*) AS locally_repairable_current_state
FROM parsed
WHERE rs_doc IS NOT NULL AND sync_doc IS NOT NULL
  AND rs_doc #> '{unsigned,redacted_because}' IS NULL
  AND sync_doc #> '{unsigned,redacted_because}' IS NULL
  AND (rs_doc -> 'content') IS DISTINCT FROM (sync_doc -> 'content')
  AND (sync_doc -> 'content') @> (rs_doc -> 'content')
  AND NOT COALESCE(jsonb_path_exists(rs_doc -> 'content', 'strict $.** ? (@.type() == "array")'), FALSE)
  AND NOT COALESCE(jsonb_path_exists(sync_doc -> 'content', 'strict $.** ? (@.type() == "array")'), FALSE)
GROUP BY type
ORDER BY locally_repairable_current_state DESC;

\echo ''
\echo '=== 2d. per-room totals for the backlog (output rows) ==='
WITH parsed AS (
  SELECT o.room_id,
         CASE WHEN j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE j.event_json::jsonb END AS rs_doc,
         CASE WHEN o.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE o.headered_event_json::jsonb END AS sync_doc
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
)
SELECT room_id, count(*) AS affected_output_rows
FROM parsed
WHERE rs_doc IS NOT NULL AND sync_doc IS NOT NULL
  AND sync_doc #> '{unsigned,redacted_because}' IS NULL
  AND rs_doc #> '{unsigned,redacted_because}' IS NULL
  AND ((rs_doc -> 'content') IS DISTINCT FROM (sync_doc -> 'content')
    OR (jsonb_typeof(rs_doc -> 'content') = 'object' AND (rs_doc -> 'content') = '{}'::jsonb))
GROUP BY room_id
ORDER BY affected_output_rows DESC;

\echo ''
\echo '=== 2e. empty-content copies: verification requests (dry run) ==='
-- These are the matching empty-content copies from 2a. SQL cannot recompute
-- the content hash, so this batch only asks the roomserver to run its
-- stored-copy check. already_full is not pruned and must not be sent again.
-- Treat would_refetched and refetched as successful hash-pruned results;
-- would_refetched_signature_unverified and refetched_signature_unverified
-- also passed the content-hash check, but carry a signature warning. Preserve
-- these statuses as returned, and include both successful statuses when
-- filtering candidates for a later dry_run=false request.
WITH parsed AS (
  SELECT o.id, o.room_id, o.event_id, o.headered_event_json,
         CASE WHEN j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE j.event_json::jsonb END AS rs_doc,
         CASE WHEN o.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE o.headered_event_json::jsonb END AS sync_doc
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
), candidates AS (
  SELECT o.id,
         o.room_id,
         o.event_id,
         row_number() OVER (ORDER BY o.id) - 1 AS n
  FROM parsed o
  WHERE o.rs_doc IS NOT NULL AND o.sync_doc IS NOT NULL
    AND o.sync_doc #> '{unsigned,redacted_because}' IS NULL
    AND o.rs_doc #> '{unsigned,redacted_because}' IS NULL
    AND jsonb_typeof(o.rs_doc -> 'content') = 'object'
    AND (o.rs_doc -> 'content') = '{}'::jsonb
    AND (o.sync_doc -> 'content') = '{}'::jsonb
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
\echo '=== 2f. verified local Sync API content repairs, by type ==='
-- A full roomserver copy passed the content-hash check. If Sync has empty
-- content, report a local Sync API repair rather than a federation refetch.
WITH parsed AS (
  SELECT o.type,
         CASE WHEN j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE j.event_json::jsonb END AS rs_doc,
         CASE WHEN o.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE o.headered_event_json::jsonb END AS sync_doc
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
)
SELECT type, count(*) AS sync_api_repair_cases
FROM parsed
WHERE rs_doc IS NOT NULL AND sync_doc IS NOT NULL
  AND jsonb_typeof(rs_doc -> 'content') = 'object'
  AND rs_doc -> 'content' <> '{}'::jsonb
  AND jsonb_typeof(sync_doc -> 'content') = 'object'
  AND sync_doc -> 'content' = '{}'::jsonb
  AND sync_doc #> '{unsigned,redacted_because}' IS NULL
  AND rs_doc #> '{unsigned,redacted_because}' IS NULL
GROUP BY type
ORDER BY sync_api_repair_cases DESC;

\echo ''
\echo '=== 2g. event IDs for POST /_zendrite/admin/refetchPruned (batches of 100) ==='
-- Refetch candidates are limited to cases where roomserver content itself is
-- empty and Sync cannot supply a local object repair: Sync either has no
-- content key or has content that does not contain the roomserver copy. A Sync
-- object contains the empty object, so that copy is locally repairable and is
-- listed by 2b instead; only non-object Sync content that cannot supply an
-- object repair reaches this list. Empty on both sides is sent through 2e's
-- verification flow first.
WITH parsed AS (
  SELECT o.id, o.room_id, o.event_id,
         CASE WHEN j.event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE j.event_json::jsonb END AS rs_doc,
         CASE WHEN o.headered_event_json ~ '(?<!\\)(\\\\)*(\\u0000|\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}|(\\\\)+u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2})' THEN NULL ELSE o.headered_event_json::jsonb END AS sync_doc
  FROM syncapi_output_room_events o
  JOIN roomserver_events e ON e.event_id = o.event_id
  JOIN roomserver_event_json j ON j.event_nid = e.event_nid
), candidates AS (
  SELECT o.id,
         o.room_id,
         o.event_id,
         row_number() OVER (ORDER BY o.id) - 1 AS n
  FROM parsed o
  WHERE o.rs_doc IS NOT NULL AND o.sync_doc IS NOT NULL
    AND jsonb_typeof(o.rs_doc -> 'content') = 'object'
    AND o.rs_doc -> 'content' = '{}'::jsonb
    AND NOT COALESCE((o.sync_doc -> 'content') @> (o.rs_doc -> 'content'), FALSE)
    AND o.sync_doc #> '{unsigned,redacted_because}' IS NULL
    AND o.rs_doc #> '{unsigned,redacted_because}' IS NULL
)
SELECT (n / 100) + 1 AS batch_number,
       jsonb_build_object(
         'dry_run', true,
         'events', jsonb_agg(jsonb_build_object('room_id', room_id, 'event_id', event_id) ORDER BY n)
       ) AS request_body
FROM candidates
GROUP BY n / 100
ORDER BY batch_number;
