---
title: Auto-purging empty rooms
description: Automatically remove rooms that have no local activity
---

When a Matrix room has no local activity, the roomserver still holds all of its state (events, memberships, state snapshots, redactions, etc.) in the database.
For long-running homeservers these rooms accumulate as users leave or are kicked, taking up disk and slowing full-table scans.

Zendrite can purge them automatically.
The trigger is controlled by `room_server.auto_purge_empty_rooms`, which is a tri-state value:

```yaml
room_server:
  auto_purge_empty_rooms: on_all_forgotten
```

| Value | Behaviour |
| --- | --- |
| `never` | Disabled. Operators can still purge individual rooms via [`/admin/purgeRoom`](/administration/adminapi/#post-_zendriteadminpurgeroomroomid). |
| `on_empty` | Purge as soon as no local user has a **joined** membership. Users who left but did not `/forget` lose their pre-leave history access along with the room. |
| `on_all_forgotten` *(default)* | Purge once no local user has any **non-forgotten** membership row. Users who left but did not `/forget` keep their pre-leave history access via `/messages` until they explicitly forget. |

For backwards compatibility the YAML key also accepts a boolean: `true` is equivalent to `on_all_forgotten`, `false` is equivalent to `never`.

## Triggers

Two triggers fire whenever the mode is anything other than `never`:

- **Startup sweep.** On every roomserver startup, a one-shot sweep enumerates rooms currently eligible under the configured mode and schedules a purge for each. This catches anything that built up while the feature was disabled, as well as rooms left over from a crash mid-purge during a previous shutdown.
- **Membership-change trigger.** After every local membership transition that may leave the room eligible (leave, ban, or a forget under `on_all_forgotten`), the roomserver re-evaluates the room and schedules an asynchronous purge if it now qualifies. The purge runs in the background once the membership transaction has committed.

Before enabling, you can inspect the rooms with no joined local member via [`GET /_zendrite/admin/emptyRooms`](/administration/adminapi/#get-_zendriteadminemptyrooms). This endpoint always reports the `on_empty` view regardless of the configured mode, so administrators can see candidates explicitly.

## Combining with auto-forget on leave

The auto-purge mode pairs with [`auto_forget_on_leave`](/administration/auto-forget-on-leave/) (which marks rooms as forgotten in the same transaction as the leave). The two interact as follows:

| `auto_forget_on_leave` | `auto_purge_empty_rooms` | Behaviour |
| --- | --- | --- |
| `false` (default) | `on_all_forgotten` (default) | Users keep history access after leaving until they `/forget`. The room is purged after the last non-forgotten row goes away. |
| `true` | `on_empty` *or* `on_all_forgotten` | Instant cleanup: the last leave forgets the user, and the room is purged immediately. The two modes coincide in this case. |
| `false` | `on_empty` | The room is purged when the last local user leaves, even if they did not `/forget`. Their `/messages` access disappears with the room. See the [caveat in the auto-forget docs](/administration/auto-forget-on-leave/#caveat-on_empty-with-auto_forget_on_leave-false): the advertised `m.forget_forced_upon_leave: false` capability is honest for multi-user rooms but does not predict the last-user-leaves case. |
| any | `never` | Nothing happens automatically. Use [`/admin/purgeRoom`](/administration/adminapi/#post-_zendriteadminpurgeroomroomid) to clean up. |

The default combination (`auto_forget_on_leave: false`, `auto_purge_empty_rooms: on_all_forgotten`) is the most conservative choice that still removes truly idle rooms: users get to browse pre-leave history until they explicitly forget, and once they do the storage is reclaimed.

## Rejoin behaviour

If a local user attempts to rejoin a room while its purge is still in flight, the join blocks for up to 30 seconds waiting for the purge to complete.
If the purge does not finish in time, the join returns an `M_UNKNOWN` error suggesting the client retry.
This prevents a rejoin from racing the per-component purge fan-out and ending up with a half-purged room state.

Remote joins (made by federated users on other servers) are not blocked at the federation layer; they will fail naturally because the room state no longer exists locally, and the room would have to be re-discovered via federation if any local user later joins.

## Disabling

Setting `auto_purge_empty_rooms: never` (or, for older configs, `false`) disables both the startup sweep and the membership-change trigger.
Administrators can still purge individual rooms manually via [`POST /_zendrite/admin/purgeRoom/{roomID}`](/administration/adminapi/#post-_zendriteadminpurgeroomroomid).

## Caveats

- `room_aliases` are not currently cleared by the purge.
  Aliases pointing at a purged room remain in the alias table and resolve to a now-nonexistent room ID.
  This is pre-existing behaviour of the underlying [`/admin/purgeRoom`](/administration/adminapi/#post-_zendriteadminpurgeroomroomid) endpoint, which the auto-purge feature reuses.
- Media files attached to messages in purged rooms are **not** removed.
- The purge runs the same code path as the admin endpoint, so progress and failure modes are identical.
