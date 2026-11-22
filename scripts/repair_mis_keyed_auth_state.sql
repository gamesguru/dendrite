-- Repair auth/state events whose stored numeric state key disagrees with the
-- event JSON.
--
-- Incident: !YRswYZcqh4FWoo1LLk:nutra.tk had three incarnations, room NIDs 3,
-- 4 and 11. Rooms 3 and 4 are orphaned: their events survive, their
-- roomserver_rooms row does not. Before #445, fetchAuthEvents stored a fetched
-- auth event under the state key of the event that authorized it, so the auth
-- chain of @shane:den.nutra.tk's join (power levels, join rules, and the
-- memberships of @gg, @gamesguru:* and @ggdev:*) was all filed under
-- @shane:den.nutra.tk (NID 65536). 66 such rows exist in room NID 3.
--
-- Room 11's current state references those rows because
-- roomserver_event_id_unique is global: a state install by event ID resolves to
-- the existing row and inherits its key. That is why a point lookup for
-- (m.room.power_levels, "") misses while full enumeration still finds the
-- event.
--
-- This script fixes the event rows only. It does NOT touch state snapshots,
-- the HAMT, membership rows, topology, or stream positions. After COMMIT run
-- admin downloadState for the live room so it rebuilds its current snapshot
-- from the corrected rows. Order matters: rows first, downloadState second.
--
-- Scope guards, all fatal:
--   * the target room NID must be orphaned (no roomserver_rooms row), so this
--     can never run against a live incarnation;
--   * every mismatched auth-type row in the database must belong to the target
--     room, so a wider corruption cannot be silently partially repaired;
--   * the candidate set must be exactly the documented 66 rows, all of one
--     room and one old state key;
--   * every target state key must already have a NID. This script refuses to
--     invent NID mappings; that is the repair code's job, not an ops script's.
--
-- Usage, interactively, so the transaction can be inspected before COMMIT:
--     \set ON_ERROR_STOP on
--     \i scripts/repair_mis_keyed_auth_state.sql
--
-- The transaction is left OPEN at the end. Review every count, then COMMIT; or
-- ROLLBACK. On any guard failure the transaction is already aborted.
-- Drop zz_bak_mis_keyed_auth_state_20261118 only after the room is verified.
\pset pager off
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK off
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- PostgreSQL rejects the \u0000 and lone-surrogate escapes that valid event
-- content can contain. Parse a sanitized copy, as the stored-event parser does;
-- the original event_json TEXT column is left untouched.
CREATE OR REPLACE FUNCTION pg_temp.zz_safe_jsonb(doc text)
    RETURNS jsonb
    LANGUAGE sql
    IMMUTABLE
    AS $func$
    SELECT (CASE WHEN doc LIKE '%\\u%' THEN
        regexp_replace(doc, '\\u(0000|[dD][89a-fA-F][0-9a-fA-F]{2})', '\\uFFFD', 'g')
    ELSE
        doc
    END)::jsonb
$func$;
-- Do not queue behind a busy writer, and do not hold ingest hostage.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '300s';
-- Block concurrent event writers for the whole transaction. The UPDATE target
-- and the candidate set must describe the same database state; the table lock
-- is what guarantees that, the isolation level only makes the reads stable.
-- Expect roomserver ingest to wait until COMMIT or ROLLBACK; use a quiet
-- window.
LOCK TABLE roomserver_events IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE roomserver_event_state_keys IN SHARE MODE;
LOCK TABLE roomserver_membership IN SHARE ROW EXCLUSIVE MODE;
-- The whole script is parameterised here so it cannot drift from the incident.
CREATE TEMP TABLE zz_params ON COMMIT DROP AS
SELECT
    3::bigint AS target_room_nid, 66::bigint AS expected_rows, '!YRswYZcqh4FWoo1LLk:nutra.tk'::text AS live_room_id, '!YRswYZcqh4FWoo1LLk:nutra.tk'::text AS event_room_id;
-- ---------------------------------------------------------------------------
-- Candidate set. This is exactly what will change.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE zz_mis_keyed_auth_state ON COMMIT DROP AS
SELECT
    e.event_nid, e.room_nid, e.event_type_nid, e.event_state_key_nid AS old_state_key_nid, e.event_id, k.event_state_key AS old_state_key, pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key' AS json_state_key, pg_temp.zz_safe_jsonb(j.event_json) ->> 'room_id' AS json_room_id
FROM
    roomserver_events e
    JOIN roomserver_event_json j USING (event_nid)
    LEFT JOIN roomserver_event_state_keys k ON k.event_state_key_nid = e.event_state_key_nid
WHERE
    e.event_type_nid BETWEEN 1 AND 5
    AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS NOT NULL
    AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS DISTINCT FROM k.event_state_key
ORDER BY
    e.event_nid;
\echo '--- parameters ---'
SELECT
    target_room_nid,
    expected_rows,
    live_room_id
FROM
    zz_params;
\echo '--- live incarnation (must exist; note its room_nid and snapshot) ---'
SELECT
    r.room_nid,
    r.room_id,
    r.state_snapshot_nid
FROM
    roomserver_rooms r,
    zz_params p
WHERE
    r.room_id = p.live_room_id;
\echo '--- candidate rows (expected 66, all from the target room) ---'
SELECT
    event_nid,
    room_nid,
    event_type_nid,
    old_state_key_nid,
    old_state_key,
    json_state_key,
    event_id
FROM
    zz_mis_keyed_auth_state
ORDER BY
    event_nid;
-- ---------------------------------------------------------------------------
-- Guard 1: the database is the one this script was written for.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    params_ok boolean;
    live_room_nid bigint;
    target_orphaned boolean;
BEGIN
    SELECT
        (target_room_nid > 0
            AND expected_rows > 0)
    INTO
        params_ok
    FROM
        zz_params;
    IF NOT coalesce(params_ok, FALSE) THEN
        RAISE EXCEPTION 'zz_params is not configured; ROLLBACK';
    END IF;
    SELECT
        r.room_nid
    INTO
        live_room_nid
    FROM
        roomserver_rooms r,
        zz_params p
    WHERE
        r.room_id = p.live_room_id;
    IF live_room_nid IS NULL THEN
        RAISE EXCEPTION 'live room % is not present in roomserver_rooms; wrong database? ROLLBACK', (
                SELECT
                    live_room_id
                FROM
                    zz_params);
    END IF;
    SELECT
        NOT EXISTS (
            SELECT
                1
            FROM
                roomserver_rooms r,
                zz_params p
            WHERE
                r.room_nid = p.target_room_nid)
    INTO
        target_orphaned;
    IF NOT target_orphaned THEN
        RAISE EXCEPTION 'room NID % still has a roomserver_rooms row; this is not an orphan. '
                     'Use the #563 repair path instead, which reconciles memberships; ROLLBACK', (
                SELECT
                    target_room_nid
                FROM
                    zz_params);
    END IF;
    RAISE NOTICE 'live room NID %, target orphan NID %', live_room_nid, (
        SELECT
            target_room_nid
        FROM
            zz_params);
END
$$;
-- ---------------------------------------------------------------------------
-- Guard 2: blast radius. No mismatch anywhere may be outside the target room.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    outside bigint;
BEGIN
    SELECT
        count(*)
    INTO
        outside
    FROM
        roomserver_events e
        JOIN roomserver_event_json j USING (event_nid)
        LEFT JOIN roomserver_event_state_keys k ON k.event_state_key_nid = e.event_state_key_nid
    WHERE
        e.event_type_nid BETWEEN 1 AND 5
        AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS NOT NULL
        AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS DISTINCT FROM k.event_state_key
        AND e.room_nid <> (
            SELECT
                target_room_nid
            FROM
                zz_params);
    IF outside <> 0 THEN
        RAISE EXCEPTION '% mismatched auth-type rows sit outside room NID %; refusing to partially '
                     'repair the database; ROLLBACK', outside, (
                SELECT
                    target_room_nid
                FROM
                    zz_params);
    END IF;
END
$$;
-- ---------------------------------------------------------------------------
-- Guard 3: the candidate set is exactly the documented incident.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    candidate_count bigint;
    candidate_rooms bigint;
    distinct_old_nid bigint;
    wrong_room bigint;
    wrong_state_key bigint;
BEGIN
    SELECT
        count(*),
        count(DISTINCT room_nid),
        count(DISTINCT old_state_key_nid)
    INTO
        candidate_count,
        candidate_rooms,
        distinct_old_nid
    FROM
        zz_mis_keyed_auth_state;
    IF candidate_count <> (
        SELECT
            expected_rows
        FROM
            zz_params) THEN
        RAISE EXCEPTION 'expected % candidates, found %; ROLLBACK', (
                SELECT
                    expected_rows
                FROM
                    zz_params),
            candidate_count;
    END IF;
    IF candidate_rooms <> 1 THEN
        RAISE EXCEPTION 'candidates span % rooms; expected one; ROLLBACK', candidate_rooms;
    END IF;
    IF candidate_count <> (
        SELECT
            count(*)
        FROM
            zz_mis_keyed_auth_state
        WHERE
            room_nid = (
            SELECT
                target_room_nid
            FROM
                zz_params)) THEN
        RAISE EXCEPTION 'candidates include a room other than the target NID; ROLLBACK';
    END IF;
    -- The #445 signature is one authorizing key applied to the whole chain.
    IF distinct_old_nid <> 1 THEN
        RAISE EXCEPTION 'candidates carry % distinct old state key NIDs; expected one; ROLLBACK', distinct_old_nid;
    END IF;
    SELECT
        count(*)
    INTO
        wrong_room
    FROM
        zz_mis_keyed_auth_state c,
        zz_params p
    WHERE
        c.json_room_id IS DISTINCT FROM p.event_room_id;
    IF wrong_room <> 0 THEN
        RAISE EXCEPTION '% candidates name a room_id other than %; ROLLBACK', wrong_room, (
                SELECT
                    event_room_id
                FROM
                    zz_params);
    END IF;
    -- The JSON is the source of truth, so the target must differ from the old key.
    SELECT
        count(*)
    INTO
        wrong_state_key
    FROM
        zz_mis_keyed_auth_state
    WHERE
        json_state_key IS NULL
        OR json_state_key = old_state_key;
    IF wrong_state_key <> 0 THEN
        RAISE EXCEPTION '% candidates do not have a JSON state key different from the stored one; ROLLBACK', wrong_state_key;
    END IF;
    RAISE NOTICE 'candidate set: % rows, one room, old key % (NID %)', candidate_count, ( SELECT DISTINCT
            old_state_key
        FROM
            zz_mis_keyed_auth_state),
    ( SELECT DISTINCT
            old_state_key_nid
        FROM
            zz_mis_keyed_auth_state);
END
$$;
-- ---------------------------------------------------------------------------
-- Guard 4: every target state key already has a NID.
-- The empty string is preseeded as NID 1; the member keys exist because those
-- users appear in other rooms. If one is missing this stops: inventing a NID
-- during an ops repair would be a second, silent change.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    missing_keys bigint;
BEGIN
    SELECT
        count(*)
    INTO
        missing_keys
    FROM ( SELECT DISTINCT
            c.json_state_key
        FROM
            zz_mis_keyed_auth_state c
        LEFT JOIN roomserver_event_state_keys k ON k.event_state_key = c.json_state_key
    WHERE
        k.event_state_key_nid IS NULL) missing;
    IF missing_keys <> 0 THEN
        RAISE EXCEPTION '% target state keys have no existing NID; refusing repair; ROLLBACK', missing_keys;
    END IF;
END
$$;
-- ---------------------------------------------------------------------------
-- Guard 5: the orphan room has no membership rows.
--
-- PurgeRoom deletes roomserver_membership for the room NID, so a clean purge
-- leaves none. Rows here would mean the late writes that created the orphan
-- events also updated membership, and this script would then leave the
-- membership table inconsistent with the re-keyed events.
--
-- Room NID 3 is dead, so such rows are unreachable from the live room and this
-- is not fatal: the script reports them and continues. If you want them
-- reconciled before commit, stop here and inspect:
--     SELECT * FROM roomserver_membership
--     WHERE room_nid = 3 ORDER BY target_nid;
-- To make this fatal instead, replace the RAISE NOTICE with RAISE EXCEPTION.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    member_rows bigint;
BEGIN
    SELECT
        count(*)
    INTO
        member_rows
    FROM
        roomserver_membership m,
        zz_params p
    WHERE
        m.room_nid = p.target_room_nid;
    IF member_rows <> 0 THEN
        RAISE NOTICE '% roomserver_membership rows exist for orphan room NID %. They are unreachable '
                  'from the live room and are out of scope for this repair.', member_rows, (
            SELECT
                target_room_nid
            FROM
                zz_params);
    ELSE
        RAISE NOTICE 'no roomserver_membership rows for orphan room NID %', (
            SELECT
                target_room_nid
            FROM
                zz_params);
    END IF;
END
$$;
-- Guard live membership rows before changing event NIDs. A membership row
-- points at the candidate event but still uses the old state-key target; this
-- script's policy is to abort so the operator can reconcile it explicitly.
DO $$
DECLARE
    stale_live_memberships bigint;
BEGIN
    SELECT count(*) INTO stale_live_memberships
    FROM roomserver_membership m
    JOIN roomserver_events e ON e.event_nid = m.event_nid
    JOIN zz_mis_keyed_auth_state c ON c.event_nid = e.event_nid
    JOIN roomserver_rooms live ON live.room_nid = m.room_nid
    WHERE m.target_nid = c.old_state_key_nid;
    IF stale_live_memberships <> 0 THEN
        RAISE EXCEPTION '% live membership rows still use candidate old state-key NIDs; reconcile them before this repair; ROLLBACK', stale_live_memberships;
    END IF;
END
$$;
-- ---------------------------------------------------------------------------
-- Backup. Deliberately a fixed, non-temporary name: a second run, or a run
-- after a partial attempt, fails here instead of overwriting the pre-image.
-- ---------------------------------------------------------------------------
CREATE TABLE zz_bak_mis_keyed_auth_state_20261118 AS
SELECT
    *
FROM
    zz_mis_keyed_auth_state;
\echo '--- backup (must equal the candidate count) ---'
SELECT
    (
        SELECT
            count(*)
        FROM
            zz_bak_mis_keyed_auth_state_20261118) AS backup_rows,
    (
        SELECT
            count(*)
        FROM
            zz_mis_keyed_auth_state) AS candidates;
-- ---------------------------------------------------------------------------
-- Repair. Match on the old NID as well, so a row that moved between the
-- candidate read and the UPDATE is not silently rewritten.
-- ---------------------------------------------------------------------------
UPDATE
    roomserver_events e
SET
    event_state_key_nid = k.event_state_key_nid
FROM
    zz_mis_keyed_auth_state c
    JOIN roomserver_event_state_keys k ON k.event_state_key = c.json_state_key
WHERE
    e.event_nid = c.event_nid
    AND e.event_state_key_nid = c.old_state_key_nid;
-- ---------------------------------------------------------------------------
-- Post-checks. Any failure aborts the transaction.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    repaired bigint;
    still_old_nid bigint;
    remaining_all bigint;
    remaining_room bigint;
    pl_rules_wrong bigint;
BEGIN
    SELECT
        count(*)
    INTO
        repaired
    FROM
        roomserver_events e
        JOIN zz_mis_keyed_auth_state c USING (event_nid)
        JOIN roomserver_event_state_keys k ON k.event_state_key = c.json_state_key
    WHERE
        e.event_state_key_nid = k.event_state_key_nid;
    IF repaired <> (
        SELECT
            count(*)
        FROM
            zz_mis_keyed_auth_state) THEN
        RAISE EXCEPTION 'repaired % of % candidates; ROLLBACK', repaired, (
                SELECT
                    count(*)
                FROM
                    zz_mis_keyed_auth_state);
    END IF;
    SELECT
        count(*)
    INTO
        still_old_nid
    FROM
        roomserver_events e
        JOIN zz_mis_keyed_auth_state c USING (event_nid)
    WHERE
        e.event_state_key_nid = c.old_state_key_nid;
    IF still_old_nid <> 0 THEN
        RAISE EXCEPTION '% candidates still carry the old state key NID; ROLLBACK', still_old_nid;
    END IF;
    -- Whole-table check, not just the target room: the repair must leave no
    -- mismatch of any auth type anywhere.
    SELECT
        count(*)
    INTO
        remaining_all
    FROM
        roomserver_events e
        JOIN roomserver_event_json j USING (event_nid)
        LEFT JOIN roomserver_event_state_keys k ON k.event_state_key_nid = e.event_state_key_nid
    WHERE
        e.event_type_nid BETWEEN 1 AND 5
        AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS NOT NULL
        AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS DISTINCT FROM k.event_state_key;
    IF remaining_all <> 0 THEN
        RAISE EXCEPTION '% mismatched auth-type rows remain after update; ROLLBACK', remaining_all;
    END IF;
    SELECT
        count(*)
    INTO
        remaining_room
    FROM
        roomserver_events e
        JOIN roomserver_event_json j USING (event_nid)
        LEFT JOIN roomserver_event_state_keys k ON k.event_state_key_nid = e.event_state_key_nid
    WHERE
        e.room_nid = (
            SELECT
                target_room_nid
            FROM
                zz_params)
        AND e.event_type_nid BETWEEN 1 AND 5
        AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS NOT NULL
        AND (pg_temp.zz_safe_jsonb(j.event_json) ->> 'state_key') IS DISTINCT FROM k.event_state_key;
    IF remaining_room <> 0 THEN
        RAISE EXCEPTION '% mismatched rows remain in the target room; ROLLBACK', remaining_room;
    END IF;
    -- The symptom that started this: empty-keyed state must be on NID 1.
    SELECT
        count(*)
    INTO
        pl_rules_wrong
    FROM
        roomserver_events e
        JOIN zz_mis_keyed_auth_state c USING (event_nid)
    WHERE
        c.event_type_nid IN (2, 3)
        AND e.event_state_key_nid <> 1;
    IF pl_rules_wrong <> 0 THEN
        RAISE EXCEPTION '% power-level/join-rules rows are not on the empty-key NID 1; ROLLBACK', pl_rules_wrong;
    END IF;
    RAISE NOTICE 'repair verified: % rows re-keyed, 0 mismatches remain', repaired;
END
$$;
\echo '--- re-keyed power levels / join rules (state key NID must be 1) ---'
SELECT
    e.event_nid,
    e.event_type_nid,
    e.event_state_key_nid,
    k.event_state_key
FROM
    roomserver_events e
    JOIN zz_mis_keyed_auth_state c USING (event_nid)
    LEFT JOIN roomserver_event_state_keys k ON k.event_state_key_nid = e.event_state_key_nid
WHERE
    c.event_type_nid IN (2, 3)
ORDER BY
    e.event_nid;
\echo '--- re-keyed memberships (each must match its JSON state key) ---'
SELECT
    e.event_nid,
    k.event_state_key
FROM
    roomserver_events e
    JOIN zz_mis_keyed_auth_state c USING (event_nid)
    LEFT JOIN roomserver_event_state_keys k ON k.event_state_key_nid = e.event_state_key_nid
WHERE
    c.event_type_nid = 5
ORDER BY
    e.event_nid;
\echo 'Transaction remains open.'
\echo 'COMMIT to keep the row repair; ROLLBACK to undo it. Nothing else has changed.'
\echo 'After COMMIT (in this order):'
\echo '  1. start/keep dendrite running on the repaired database'
\echo '  2. GET /_zendrite/admin/downloadState/nutra.tk/!YRswYZcqh4FWoo1LLk:nutra.tk'
\echo '  3. GET /state/m.room.power_levels must return 200 with $x3R5YpR4R3zr3OWSskyoXlMUQkCMgjIhwOHCk_hADzo'
\echo '  4. GET /state/m.room.power_levels/@shane:den.nutra.tk must return 404 (old leaf gone, not shadowed)'
\echo '  5. roomserver_state_hamt_snapshots for the live room NID must have a new root_hash'
\echo '  6. retry the pin'
\echo ''
\echo 'Post-commit snapshot check (run outside this transaction):'
\echo '  SELECT s.state_snapshot_nid, s.room_nid, encode(s.root_hash, ''hex'') AS root_hash, s.entry_count'
\echo '  FROM roomserver_state_hamt_snapshots s, roomserver_rooms r'
\echo '  WHERE r.room_id = ''!YRswYZcqh4FWoo1LLk:nutra.tk'' AND s.state_snapshot_nid = r.state_snapshot_nid;'
\echo '  -- 36451 / fb9247c8... was the bad snapshot; the hash must change.'
\echo ''
\echo 'Rollback of the data, if ever needed (backup persists):'
\echo '  UPDATE roomserver_events e SET event_state_key_nid = b.old_state_key_nid'
\echo '  FROM zz_bak_mis_keyed_auth_state_20261118 b WHERE e.event_nid = b.event_nid;'
