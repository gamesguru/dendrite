---
title: Auto-forgetting rooms on leave
description: Automatically mark rooms as forgotten when a user leaves
---

Matrix distinguishes two related concepts:

- **Leave** is a public state-event change. Everyone in the room sees that the user left. The user keeps history access (via `/messages`, `/context`, etc.) up to their leave event, and the room shows up in their client under "Historical" / "Left rooms".
- **Forget** is a per-user, server-side flag. The server stops returning the room in `/sync` for that user, the room disappears from the client list, and the user can no longer browse its history. Forget can only be applied after the user has already left or been banned.

By default Matrix clients call `/forget` explicitly when the user clicks "Forget room?" in the UI. Zendrite can do this automatically — marking the room as forgotten in the same database transaction as the leave — by enabling the `auto_forget_on_leave` option:

```yaml
room_server:
  auto_forget_on_leave: true
```

The default is `false`.

When enabled, Zendrite advertises the Matrix 1.18 [`m.forget_forced_upon_leave`](https://spec.matrix.org/v1.18/client-server-api/#mforget_forced_upon_leave-capability) capability (introduced by [MSC4267](https://github.com/matrix-org/matrix-spec-proposals/blob/main/proposals/4267-forget-room-on-leave.md)) via [`/_matrix/client/v3/capabilities`](https://spec.matrix.org/v1.18/client-server-api/#get_matrixclientv3capabilities) so compliant clients know to skip the "Forget room?" prompt.

## Why default off

The default is conservative because forgetting is a one-way operation that affects user-facing behaviour:

- Forgotten rooms disappear from a user's "archived rooms" / "left rooms" client UI entirely.
- A user can no longer call `/messages` or `/context` against a forgotten room to look at pre-leave history.
- If a user is kicked or banned (the spec includes both transitions), auto-forget removes their ability to review the room to understand what happened.
- After auto-forget, a later rejoin presents the room as if it were brand new — pre-leave history has to be backfilled from federation, which is noticeably slower in large rooms.

Enable it if you prefer the "leaving the room cleans up client UI" behaviour over the "user can browse rooms they have left" behaviour. For most homeservers the latter is the safer default.

## Behaviour

When the flag is on, every membership transition to `leave` or `ban` for a **local** user causes the membership row to be marked as forgotten in the same transaction. This applies regardless of who triggered the change:

- The user calls `/leave` on themselves.
- The user is kicked (membership set to `leave` by another user).
- The user is banned (membership set to `ban`).

Remote users on other homeservers are unaffected — their forgotten state is their own homeserver's concern.

The flag interacts cleanly with [auto-purge of empty rooms](/administration/auto-purge-empty-rooms/), which has its own tri-state setting (`never`, `on_empty`, `on_all_forgotten`).

- With the default purge mode `on_all_forgotten`, enabling `auto_forget_on_leave` forces a forget at every leave, so the last leave becomes both the last membership transition and the last forget. The room is purged immediately.
- With `on_empty`, the room is already purged on the last leave regardless of forgotten state, so auto-forget makes no difference to the purge timing. It still clears the leaving user's history access for any room that stays around (e.g. because other locals are still in it).

See [Auto-purging empty rooms](/administration/auto-purge-empty-rooms/) for the full interaction table.

### Caveat: `on_empty` with `auto_forget_on_leave: false`

This combination is the one mismatch worth flagging.
With `auto_forget_on_leave: false` the server advertises `m.forget_forced_upon_leave: { enabled: false }`, so a compliant client expects rooms to stick around after a leave and shows the "Forget room?" prompt.
With `auto_purge_empty_rooms: on_empty` the room is destroyed whenever the last local member leaves, which from that user's point of view is indistinguishable from forgetting.
The client's expectation no longer matches what the server does for that specific case.

Zendrite does not pretend the capability is `true` here, because for multi-user rooms it really isn't (the leaving user keeps history access until they `/forget`, exactly as advertised). The mismatch only bites when the leaving user is also the last local member. If that combination matters for your deployment, enable `auto_forget_on_leave` so the capability and the behaviour line up.

## Capability advertisement

Regardless of the flag value, the `/_matrix/client/v3/capabilities` endpoint exposes the current state:

```json
{
  "capabilities": {
    "m.forget_forced_upon_leave": {
      "enabled": true
    }
  }
}
```

Clients use this to decide whether to show a "Forget room?" prompt after a leave (when `enabled` is `false`) or to skip it (when `true`).

## Manual /forget

Explicit `/forget` calls remain available regardless of this setting. They are also a no-op for rooms the server no longer knows about, so a client that has cached a now-purged room and tries to forget it gets a clean response rather than an error.
