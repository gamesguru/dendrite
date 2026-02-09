---
title: Architecture
description: How Dendrite and gomatrixserverlib relate to each other.
---

Dendrite is built on top of [gomatrixserverlib](https://github.com/matrix-org/gomatrixserverlib), a Go library that implements the core Matrix protocol.
Understanding the boundary between the two is important when contributing or implementing new [MSCs](https://spec.matrix.org/proposals/).

## gomatrixserverlib (protocol layer)

gomatrixserverlib is a standalone library with no knowledge of Dendrite.
It provides the building blocks of the Matrix protocol:

- **Event types and formats** — PDU interface, event versions (V1–V12), event builders
- **Room versions** — version metadata, stability flags, format differences
- **State resolution** — algorithms for resolving conflicting room state (V1, V2, V2.1)
- **Event authentication** — rules for validating events against auth state
- **Federation types** — request/response structs (`PublicRoom`, `RoomHierarchyRoom`, `RespSend`, etc.)
- **Error codes** — Matrix spec error types (`M_FORBIDDEN`, `M_NOT_FOUND`, `M_TOO_LARGE`, etc.)
- **Cryptographic operations** — event signing and signature verification
- **Redaction logic** — spec-compliant event content stripping
- **Identity types** — user IDs, room IDs, server names, sender IDs

## Dendrite (application layer)

Dendrite is the homeserver application that uses gomatrixserverlib.
It handles everything above the protocol layer:

- **HTTP endpoints** — Client-Server API, Federation API, Admin API routing
- **Database storage** — event persistence, state snapshots, device data
- **Configuration** — which room version is default, which MSCs are enabled
- **Business logic** — access control, sync, presence, push notifications
- **MSC feature toggles** — opt-in MSCs via config (`setup/mscs/`)

## Where do MSC changes go?

| Change type                                                 | Where                                           |
| ----------------------------------------------------------- | ----------------------------------------------- |
| New error codes (e.g. `M_TOO_LARGE`)                        | gomatrixserverlib (`spec/`)                     |
| New fields on protocol types (e.g. `PublicRoom.Encryption`) | gomatrixserverlib (`fclient/`)                  |
| New room version definitions or state resolution changes    | gomatrixserverlib (`eventversion.go`)           |
| New event types or content formats                          | gomatrixserverlib (`spec/`)                     |
| New API endpoints or handlers                               | Dendrite (`clientapi/`, `federationapi/`, etc.) |
| New server behavior or business logic                       | Dendrite                                        |
| New configuration options                                   | Dendrite (`setup/config/`)                      |
| New database tables or queries                              | Dendrite (`storage/`)                           |

Many MSCs require changes in **both** — protocol types in gomatrixserverlib and endpoint handlers in Dendrite.

### Example: MSC3266 (room summary)

- **gomatrixserverlib**: adds `Encryption` and `RoomVersion` fields to `PublicRoom` struct
- **Dendrite**: implements the `/summary` endpoint in `clientapi/routing/room_summary.go`, populates the new fields from room state

### Example: MSC2836 (threading)

- **gomatrixserverlib**: defines `MSC2836EventRelationshipsResponse` federation type
- **Dendrite**: implements the `/event_relationships` endpoint, database tables for parent-child tracking, and the opt-in config toggle in `setup/mscs/msc2836/`

## gomatrixserverlib fork

This fork uses a patched version of gomatrixserverlib via a `replace` directive in `go.mod`:

```go
replace github.com/matrix-org/gomatrixserverlib => github.com/jackmaninov/gomatrixserverlib ...
```

When upstream gomatrixserverlib adds new protocol features (error codes, struct fields, room versions), the fork must be synced to pick them up.
Dendrite code that references features missing from the fork will fail to compile.
