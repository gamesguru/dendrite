---
title: Auto-purging empty rooms
description: Automatically remove rooms that have no local members
---

When a Matrix room has no local members joined, the roomserver still holds all of its state (events, memberships, state snapshots, redactions, etc.) in the database.
For long-running homeservers these "empty" rooms accumulate as users leave or are kicked, taking up disk and slowing full-table scans.

Zendrite purges them automatically.
The feature is controlled by a single `room_server.auto_purge_empty_rooms` flag, which is **`true` by default**:

```yaml
room_server:
  auto_purge_empty_rooms: true
```

Two behaviours fire together when enabled.

## Startup sweep

On every roomserver startup, a one-shot sweep enumerates rooms which currently have no local members and schedules a purge for each.
This catches anything that built up while the feature was disabled, as well as rooms left over from a crash mid-purge during a previous shutdown.

Before enabling, you can inspect the candidates via [`GET /_zendrite/admin/emptyRooms`](/administration/adminapi/#get-_zendriteadminemptyrooms).

## Runtime trigger

Whenever a local user transitions out of `join` (leave, ban, or otherwise) and no local members remain joined, the roomserver schedules an asynchronous purge of the room.
The purge runs in the background after the membership transaction commits.
It removes all room data (events, memberships, state, redactions) and notifies downstream components (syncapi, federationapi) to drop their copies.

## Rejoin behaviour

If a local user attempts to rejoin a room while its purge is still in flight, the join blocks for up to 30 seconds waiting for the purge to complete.
If the purge does not finish in time, the join returns an `M_UNKNOWN` error suggesting the client retry.
This prevents a rejoin from racing the per-component purge fanout and ending up with a half-purged room state.

Remote joins (made by federated users on other servers) are not blocked at the federation layer; they will fail naturally because the room state no longer exists locally, and the room would have to be re-discovered via federation if any local user later joins.

## Disabling

Setting `auto_purge_empty_rooms: false` disables both the startup sweep and the runtime trigger.
Administrators can still purge individual rooms manually via [`POST /_zendrite/admin/purgeRoom/{roomID}`](/administration/adminapi/#post-_zendriteadminpurgeroomroomid).

## Caveats

- `room_aliases` are not currently cleared by the purge.
  Aliases pointing at a purged room remain in the alias table and resolve to a now-nonexistent room ID.
  This is pre-existing behaviour of the underlying [`/admin/purgeRoom`](/administration/adminapi/#post-_zendriteadminpurgeroomroomid) endpoint, which the auto-purge feature reuses.
- Media files attached to messages in purged rooms are **not** removed.
- The purge runs the same code path as the admin endpoint, so progress and failure modes are identical.
