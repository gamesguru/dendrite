---
title: MSC Support
description: Implementation status of Matrix Spec Changes (MSCs) in Zendrite.
---

[MSCs](https://spec.matrix.org/proposals/) (Matrix Spec Changes) are proposals to extend or modify the Matrix protocol.
This page tracks the implementation status of notable MSCs in Zendrite.

Many MSCs listed here have since been merged into the [Matrix specification](https://spec.matrix.org/) proper.
They are listed by their original MSC number and the first spec version which includes them.
Feature support does not imply support for an entire specification version; see the [client API conformance audit](/development/client-api-conformance/) for discovery policy and remaining blockers.

## Sync

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC4186](https://github.com/matrix-org/matrix-spec-proposals/pull/4186) | Simplified Sliding Sync | N/A | Implemented (native, no proxy needed) |
| [MSC3575](https://github.com/matrix-org/matrix-spec-proposals/pull/3575) | Sliding Sync (v1) | N/A | Implemented (legacy, superseded by MSC4186) |

## Messaging

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC2675](https://github.com/matrix-org/matrix-spec-proposals/pull/2675) | Serverside aggregations of message relationships | 1.3 | Implemented: [bundled aggregations](#bundled-aggregations), and `/relations` in topological order with `/messages` and `/sync` tokens |
| [MSC3981](https://github.com/matrix-org/matrix-spec-proposals/pull/3981) | Recursive relations | 1.10 | Implemented: `recurse` follows relations three levels deep |
| [MSC2676](https://github.com/matrix-org/matrix-spec-proposals/pull/2676) | Message editing | 1.4 | Implemented: the latest valid edit is bundled under `m.replace`, in the v1.7 format; invalid edits are ignored, and a redacted original carries no edits |
| [MSC3316](https://github.com/matrix-org/matrix-spec-proposals/pull/3316) | Timestamp massaging | 1.3 | Implemented: the `ts` parameter on `/send`, `/state`, membership endpoints, `/createRoom` and profile updates only applies to requests made with an application service token, and is ignored otherwise |
| [MSC3958](https://github.com/matrix-org/matrix-spec-proposals/pull/3958) | Suppress notifications from message edits | 1.9 | Implemented: the `.m.rule.suppress_edits` default push rule, also added to existing accounts' rules |
| [MSC2677](https://github.com/matrix-org/matrix-spec-proposals/pull/2677) | Reactions | 1.7 | Partial: relation storage and retrieval; the spec defines no server-side aggregation for annotations |
| [MSC3267](https://github.com/matrix-org/matrix-spec-proposals/pull/3267) | Reference relations | 1.5 | Implemented: references are bundled under `m.reference` |
| [MSC3440](https://github.com/matrix-org/matrix-spec-proposals/pull/3440) | Threading via `m.thread` | 1.4 | Implemented: `m.thread` aggregations, `GET /v1/rooms/{roomId}/threads`, and 400 `M_UNKNOWN` for a thread rooted at an event that is itself a relation |
| [MSC3771](https://github.com/matrix-org/matrix-spec-proposals/pull/3771) | Threaded read receipts | 1.4 | Implemented: receipts carry `thread_id` and clear only their thread's notifications |
| [MSC3773](https://github.com/matrix-org/matrix-spec-proposals/pull/3773) | Notifications for threads | 1.4 | Implemented: with `unread_thread_notifications` in the timeline filter, `/sync` reports `unread_thread_notifications` per thread root and `unread_notifications` for the main timeline only |
| [MSC2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836) | Threading (generic relationships) | N/A | Opt-in (`msc2836`) |
| [MSC2285](https://github.com/matrix-org/matrix-spec-proposals/pull/2285) | Private read receipts | 1.4 | Implemented |

### Bundled aggregations

Zendrite adds `unsigned.m.relations` to events served by `/messages`, `/context`, `/event`, `/relations`, `/search` and `/threads`, and to `/sync` and sliding sync timelines that are `limited`.
An unlimited timeline already carries every child event the client has not seen, so the client aggregates those itself.
Each bundle holds the thread summary (`m.thread`), the most recent valid edit (`m.replace`) and the referencing events (`m.reference`).
Only child events the requesting user may see are aggregated, and events from ignored users are left out.
State events, and aggregations that arrive with events over federation, are never served with bundles.

## Rooms & Spaces

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC1772](https://github.com/matrix-org/matrix-spec-proposals/pull/1772) | Spaces | 1.2 | Implemented |
| [MSC2946](https://github.com/matrix-org/matrix-spec-proposals/pull/2946) | Spaces summary / room hierarchy | 1.2 | Implemented |
| [MSC2432](https://github.com/matrix-org/matrix-spec-proposals/pull/2432) | Publishing room aliases | 1.2 | Implemented: new canonical and alternative aliases resolve locally or over federation; retained aliases are not revalidated |
| [MSC3083](https://github.com/matrix-org/matrix-spec-proposals/pull/3083) | Restricted rooms (space-based membership) | 1.2 | Implemented: joins distinguish uncheckable conditions (`M_UNABLE_TO_AUTHORISE_JOIN`) from a missing local authorising user (`M_UNABLE_TO_GRANT_JOIN`) |
| [MSC2403](https://github.com/matrix-org/matrix-spec-proposals/pull/2403) | Knocking | 1.1 | Implemented: `/knock`, `make_knock` and `send_knock`, `rooms.knock` in `/sync` and knocked rooms in sliding sync. A knock is rescinded with `/leave`. Kicks and bans that end an invite or knock are also sent to the target's server when it is not in the room |
| [MSC2367](https://github.com/matrix-org/matrix-spec-proposals/pull/2367) | Reasons in all membership events | 1.1 | Implemented: `/join`, `/leave`, `/invite`, `/kick`, `/ban`, `/unban` and `/knock` accept `reason`. A leave through another server adds the reason to the `make_leave` template |
| [MSC3266](https://github.com/matrix-org/matrix-spec-proposals/pull/3266) | Room summary API | 1.15 | Implemented |
| [MSC3765](https://github.com/matrix-org/matrix-spec-proposals/pull/3765) | Rich text in room topics | 1.15 | Not implemented |
| [MSC4267](https://github.com/matrix-org/matrix-spec-proposals/pull/4267) | Forget room on leave ([`m.forget_forced_upon_leave` capability](https://spec.matrix.org/v1.18/client-server-api/#mforget_forced_upon_leave-capability)) | 1.18 | Implemented (opt-in via [`auto_forget_on_leave`](/administration/auto-forget-on-leave/)) |

## Registration

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC3231](https://github.com/matrix-org/matrix-spec-proposals/pull/3231) | Token authenticated registration | 1.2 | Implemented: registration tokens and the rate-limited, unauthenticated `/v1/register/m.login.registration_token/validity` check |

## Profiles

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC3550](https://github.com/matrix-org/matrix-spec-proposals/pull/3550) | HTTP 403 for profile lookups | 1.2 | Implemented: remote lookup restrictions return 403 `M_FORBIDDEN`; local profiles remain unrestricted |
| [MSC4133](https://github.com/matrix-org/matrix-spec-proposals/pull/4133) | Extended profiles: custom profile fields | 1.16 | Implemented: see [custom profile fields](#custom-profile-fields) |
| [MSC4175](https://github.com/matrix-org/matrix-spec-proposals/pull/4175) | Time zone profile field (`m.tz`) | 1.16 | Implemented: `m.tz` must be a string |

### Custom profile fields

`GET`, `PUT` and `DELETE /_matrix/client/v3/profile/{userId}/{keyName}` read, set and remove any profile field, and `GET /_matrix/client/v3/profile/{userId}` returns them all.
The same endpoints are served under `/_matrix/client/unstable/uk.tcpip.msc4133/`, and `/versions` advertises `uk.tcpip.msc4133` and `uk.tcpip.msc4133.stable`.
`/capabilities` reports `m.profile_fields` (and `uk.tcpip.msc4133.profile_fields`) as enabled without restrictions, together with `m.set_displayname` and `m.set_avatar_url`.

A custom field name is a namespaced identifier such as `org.example.job_title`, at most 255 bytes long, and its value can be any JSON.
A profile, including its display name and avatar URL, is limited to 64 KiB of canonical JSON; a change that would exceed this fails with `M_PROFILE_TOO_LARGE`.
Only the display name and avatar URL are copied into membership events; deleting either one clears it.
Guests may only change their display name.
`/_matrix/federation/v1/query/profile` serves every field, also one at a time via `field`, and profiles of remote users include their custom fields.
Deactivating an account removes its custom fields.

## Encryption & Security

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC1219](https://github.com/matrix-org/matrix-spec-proposals/pull/1219) | Server-side key backup | 1.1 | Implemented: backup versions, and uploading, fetching and deleting keys for a version, a room or a session |
| [MSC3916](https://github.com/matrix-org/matrix-spec-proposals/pull/3916) | Authenticated media | 1.11 | Implemented |
| [MSC2732](https://github.com/matrix-org/matrix-spec-proposals/pull/2732) | OLM fallback keys | 1.2 | Implemented: ordinary and dehydrated devices upload reusable fallback keys; claims mark them used and sync reports unused algorithms |
| [MSC3814](https://github.com/matrix-org/matrix-spec-proposals/pull/3814) | Dehydrated devices v2 | N/A | Opt-in (`msc3814`) |
| [MSC4115](https://github.com/matrix-org/matrix-spec-proposals/pull/4115) | Membership metadata on events | 1.11 | Implemented in sync; no opt-in required |

## Federation

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC3706](https://github.com/matrix-org/matrix-spec-proposals/pull/3706) | Partial state in `/send_join` (faster joins) | 1.6 | Implemented |
| [MSC2444](https://github.com/matrix-org/matrix-spec-proposals/pull/2444) | Peeking over federation | N/A | Opt-in (`msc2444`) |
| [MSC4436](https://github.com/matrix-org/matrix-spec-proposals/pull/4436) | Case-insensitive server ACLs | 1.19 | Implemented: `allow` and `deny` globs match the whole server name, ignoring case |
| [MSC2753](https://github.com/matrix-org/matrix-spec-proposals/pull/2753) | Peeking via `/sync` | N/A | Opt-in (`msc2753`) |
| [MSC3821](https://github.com/matrix-org/matrix-spec-proposals/pull/3821) | Update redaction rules, again | 1.8 | Implemented: from room version 11, redacting an `m.room.member` event keeps `content.third_party_invite.signed`, so redacted events hash to the same event ID as on other servers |
| [MSC4311](https://github.com/matrix-org/matrix-spec-proposals/pull/4311) | Create event in invite and knock stripped state | 1.16 | Implemented: `invite_room_state` and `knock_room_state` are sent as full PDUs including `m.room.create`. From room version 12, invites whose state holds PDUs that fail validation are rejected with `M_MISSING_PARAM`. Missing or stripped-only state is accepted with a warning, per the MSC's migration section, as are all failures in earlier room versions. Knock state entries that fail validation are dropped |

## Authentication

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC2265](https://github.com/matrix-org/matrix-spec-proposals/pull/2265) | Case folding of email addresses | 1.1 | Implemented: email 3PIDs are case-folded before they are stored, looked up or sent to an identity server. Addresses stored by older versions are folded on upgrade, except where the folded form is already in use, which is logged |
| [MSC2713](https://github.com/matrix-org/matrix-spec-proposals/pull/2713) | Remove deprecated identity service endpoints | 1.1 | Implemented: identity servers are only called through the authenticated v2 API, with the client's `id_access_token` or, for `email_validation_server`, a token the homeserver registers for its server notices user. 3PID invites require `id_access_token` and use hashed lookups |
| [MSC3288](https://github.com/matrix-org/matrix-spec-proposals/pull/3288) | Room type in `store-invite` | 1.2 | Implemented: 3PID invites send `room_type`, along with the room's name, avatar, join rule and alias |
| [MSC2858](https://github.com/matrix-org/matrix-spec-proposals/pull/2858) | Multiple SSO identity providers | 1.1 | Not applicable: SSO is only available through [MSC3861](#msc3861-oidc-delegated-authentication), where the authentication service offers the upstream providers, so the `m.login.sso` flow lists no `identity_providers` |
| [MSC2918](https://github.com/matrix-org/matrix-spec-proposals/pull/2918) | Refresh tokens | 1.3 | Implemented: clients sending `refresh_token: true` to `/login` or `/register` get a refresh token and an access token that expires after `client_api.refreshable_access_token_lifetime` (default `5m`, `0s` disables refresh tokens), which `/refresh` renews. An expired access token returns 401 `M_UNKNOWN_TOKEN` with `soft_logout: true`. A used refresh token stays valid until its successor is used, so a lost `/refresh` response can be retried. With [MSC3861](#msc3861-oidc-delegated-authentication), the authentication service issues the tokens |
| [MSC3861](https://github.com/matrix-org/matrix-spec-proposals/pull/3861) | Next-gen auth (OIDC) | 1.15 | Opt-in (`msc3861`) |

## VoIP

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) | MatrixRTC transport discovery | N/A | Implemented (configure via [`rtc_foci`](/administration/matrixrtc/)) |

## Opt-in MSCs

The following MSCs are not enabled by default and must be activated in the `mscs` section of the config file:

```yaml
mscs:
  mscs:
    - msc2836
    - msc2444
    - msc2753
    - msc3814
    - msc3861
```

### MSC2836: Threading

The relationship index behind `POST /_matrix/client/unstable/event_relationships` is built from the roomserver output stream by the durable JetStream consumer `MSC2836RoomServerConsumer`.
A relationship is therefore queryable shortly after its event is sent, not necessarily by the time the send request returns.
A failed index write is retried until it succeeds, including by an HA successor, so a stored event never permanently loses its relationship.
Disabling the MSC removes the consumer at the next start, except in HA mode, where it must be removed during the offline upgrade.

### MSC2444 and MSC2753: Peeking

Peeking into a room whose ID names no server, as in room version 12, works locally when this server is in the room.
Any other such room needs `?server_name=` on `POST /_matrix/client/v3/peek/{roomIdOrAlias}` to peek over federation.
Only world-readable, unencrypted rooms can be peeked, locally or by a remote server.
Remote peeks ask to be renewed hourly; peeking again after the interval renews an expired outbound peek.

### MSC3814: Dehydrated Devices v2

MSC3814 allows clients to store a "dehydrated" device on the server so that encrypted to-device messages (e.g. key shares) can be delivered while the user is offline.
When the user signs in again, the client rehydrates the device and retrieves messages it missed.

**What changes when MSC3814 is enabled:**

- `PUT /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` — store a dehydrated device with its keys.
- `GET /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` — retrieve the current dehydrated device metadata.
- `DELETE /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` — remove the dehydrated device.
- `POST /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device/{deviceID}/events` — retrieve to-device events addressed to the dehydrated device.

**Configuration:**

```yaml
mscs:
  mscs:
    - msc3814
```

No additional configuration is needed beyond enabling the MSC.

### MSC3861: OIDC Delegated Authentication

MSC3861 delegates authentication to an external OpenID Connect (OIDC) provider such as [Matrix Authentication Service (MAS)](https://github.com/element-hq/matrix-authentication-service).
When enabled, Zendrite validates access tokens via OAuth 2.0 token introspection instead of managing passwords directly, and provisions accounts and devices on demand.

This changes the behaviour of a large part of the client-server API: password login and registration are disabled, session management moves to the provider, and clients without native OIDC support go through a legacy SSO compatibility layer.

**Configuration:**

```yaml
mscs:
  mscs:
    - msc3861
  msc3861:
    issuer: "https://auth.example.com/"
    client_id: "0000000000000000000ZENDRITE"
    client_secret: "secret"
```

See [Delegated authentication (OIDC)](/administration/oidc/) for the full setup guide, the complete configuration reference, client support, account provisioning rules, migration caveats and troubleshooting.
