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
| [MSC4354](https://github.com/matrix-org/matrix-spec-proposals/pull/4354) | Sticky events | N/A | Implemented (unstable prefix): see [sticky events](#sticky-events) |
| [MSC4480](https://github.com/matrix-org/matrix-spec-proposals/pull/4480) | Sliding sync extension: sticky events | N/A | Implemented: the `org.matrix.msc4354.sticky_events` extension |
| [MSC4222](https://github.com/matrix-org/matrix-spec-proposals/pull/4222) | `state_after` in `/sync` | 1.16 | Implemented: with `use_state_after=true`, joined and left rooms carry `state_after`, the state at the end of the timeline, instead of `state`. The unstable `org.matrix.msc4222.use_state_after` parameter is answered with `org.matrix.msc4222.state_after`. See [state after the timeline](#state-after-the-timeline) |

### Sticky events

`/versions` advertises `org.matrix.msc4354` in `unstable_features`, which Element Call needs before it sends call memberships as sticky events.
`PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}` accepts `org.matrix.msc4354.sticky_duration_ms` and puts it into the event as `msc4354_sticky.duration_ms`.
As in Synapse, the duration must be a non-negative integer, otherwise the request fails with 400 `M_INVALID_PARAM`, and durations above an hour are kept in the event but count as one hour.
The parameter is ignored when sending state events.

An event stays sticky from the earlier of its `origin_server_ts` and the time Zendrite received it, for its duration of at most an hour.
Redacting an event removes its stickiness.
While an event is sticky, `/sync` delivers it in a `msc4354_sticky.events` section of its joined room, unless the timeline of the same response carries it after the timeline filter.
Each sticky event is delivered once as the client's sync position advances, at most 100 per response, and joining a room delivers all of the room's sticky events at once.
Sticky events of ignored users are left out, and with lazy-loaded members their senders' memberships are included.
Sticky events in the sticky section and in timelines carry `unsigned.msc4354_sticky_duration_ttl_ms`, the milliseconds until they expire.

Sliding sync delivers sticky events through the `org.matrix.msc4354.sticky_events` extension, for every joined room the lists or room subscriptions match, also outside the lists' windows.
The extension takes `since` and `limit` (default 100), and answers with `rooms` and `next_batch`, or is left out when there is nothing to deliver.
Sticky events in the timelines of the response are left out of the extension and carry their TTL in the timeline instead, when the extension is enabled.

History visibility does not hide sticky events from joined users, in `/sync`, sliding sync, `/messages`, `/context`, `/event` and the other endpoints that filter by it.
`/sync` and sliding sync timelines keep the usual rules, as sticky events reach clients through their own section.

When a server joins a room, Zendrite sends it the sticky events that local users sent there and that are still sticky.
Zendrite remembers these events in memory, so after a restart it only sends sticky events created since.
Soft-failed sticky events are not re-evaluated when the room state changes.

### State after the timeline

A client that opts in to [MSC4222](https://github.com/matrix-org/matrix-spec-proposals/pull/4222) must not apply the state events of the timeline itself, so `state_after` holds them as well.
An initial sync, a sync with `full_state=true` and a sync that newly joined a room send the room's whole current state.
An incremental sync sends the state changes between the `since` token and the end of the timeline, including those of a gappy (`limited`) timeline.
For a room the user left, the state ends at their leave.
`state_after` is sent even when it is empty, and the `state` filter applies to it.
With `lazy_load_members`, it holds the memberships of the timeline senders, and an incremental sync keeps every membership change.
Zendrite advertises `org.matrix.msc4222` in `unstable_features` for clients that check it before opting in.

## Messaging

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC2675](https://github.com/matrix-org/matrix-spec-proposals/pull/2675) | Serverside aggregations of message relationships | 1.3 | Implemented: [bundled aggregations](#bundled-aggregations), and `/relations` in topological order with `/messages` and `/sync` tokens |
| [MSC3981](https://github.com/matrix-org/matrix-spec-proposals/pull/3981) | Recursive relations | 1.10 | Implemented: `recurse` follows relations three levels deep |
| [MSC2676](https://github.com/matrix-org/matrix-spec-proposals/pull/2676) | Message editing | 1.4 | Implemented: the latest valid edit is bundled under `m.replace`, in the v1.7 format; invalid edits are ignored, and a redacted original carries no edits |
| [MSC3316](https://github.com/matrix-org/matrix-spec-proposals/pull/3316) | Timestamp massaging | 1.3 | Implemented: the `ts` parameter on `/send`, `/state`, membership endpoints, `/createRoom` and profile updates only applies to requests made with an application service token, and is ignored otherwise |
| [MSC3958](https://github.com/matrix-org/matrix-spec-proposals/pull/3958) | Suppress notifications from message edits | 1.9 | Implemented: the `.m.rule.suppress_edits` default push rule, also added to existing accounts' rules |
| [MSC2677](https://github.com/matrix-org/matrix-spec-proposals/pull/2677) | Reactions | 1.7 | Implemented: relations are stored and served through `/relations`. Sending an annotation that the sender has already made, with the same event type and key on the same event and not redacted, fails with 400 `M_DUPLICATE_ANNOTATION`; retrying the original transaction still returns its event. The spec defines no server-side aggregation for annotations |
| [MSC3970](https://github.com/matrix-org/matrix-spec-proposals/pull/3970) | Scope transaction IDs to devices | 1.7 | Implemented: `/send`, `/redact`, `/sendToDevice`, delayed events and server notices recognise a retransmission by device, endpoint and transaction ID, so it survives a refreshed access token. Application services share one scope per application service, whichever user they act as |
| [MSC3267](https://github.com/matrix-org/matrix-spec-proposals/pull/3267) | Reference relations | 1.5 | Implemented: references are bundled under `m.reference` |
| [MSC3440](https://github.com/matrix-org/matrix-spec-proposals/pull/3440) | Threading via `m.thread` | 1.4 | Implemented: `m.thread` aggregations, `GET /v1/rooms/{roomId}/threads`, and 400 `M_UNKNOWN` for a thread rooted at an event that is itself a relation |
| [MSC3771](https://github.com/matrix-org/matrix-spec-proposals/pull/3771) | Threaded read receipts | 1.4 | Implemented: receipts carry `thread_id` and clear only their thread's notifications |
| [MSC3773](https://github.com/matrix-org/matrix-spec-proposals/pull/3773) | Notifications for threads | 1.4 | Implemented: with `unread_thread_notifications` in the timeline filter, `/sync` reports `unread_thread_notifications` per thread root and `unread_notifications` for the main timeline only |
| [MSC2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836) | Threading (generic relationships) | N/A | Opt-in (`msc2836`) |
| [MSC2285](https://github.com/matrix-org/matrix-spec-proposals/pull/2285) | Private read receipts | 1.4 | Implemented |
| [MSC2174](https://github.com/matrix-org/matrix-spec-proposals/pull/2174) | Move the `redacts` property to `content` | 1.8 | Implemented: from room version 11, redactions are applied from `content.redacts` and the top-level key is ignored. Clients receive `redacts` both at the top level and in `content` in every room version |
| [MSC4169](https://github.com/matrix-org/matrix-spec-proposals/pull/4169) | Backwards-compatible redaction sending using `/send` | 1.18 | Implemented: `PUT /send/m.room.redaction` copies `content.redacts` to the top level, so it redacts in every room version. Advertised as `com.beeper.msc4169` in `/versions` |

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
| [MSC3827](https://github.com/matrix-org/matrix-spec-proposals/pull/3827) | Filtering of `/publicRooms` by room type | 1.4 | Implemented: client and federation `/publicRooms` accept `room_types` (`null` matches rooms without a type) and return `room_type`. Results from remote servers are filtered again, for servers that ignore the filter |
| [MSC2432](https://github.com/matrix-org/matrix-spec-proposals/pull/2432) | Publishing room aliases | 1.2 | Implemented: new canonical and alternative aliases resolve locally or over federation; retained aliases are not revalidated |
| [MSC3083](https://github.com/matrix-org/matrix-spec-proposals/pull/3083) | Restricted rooms (space-based membership) | 1.2 | Implemented: joins distinguish uncheckable conditions (`M_UNABLE_TO_AUTHORISE_JOIN`) from a missing local authorising user (`M_UNABLE_TO_GRANT_JOIN`) |
| [MSC2403](https://github.com/matrix-org/matrix-spec-proposals/pull/2403) | Knocking | 1.1 | Implemented: `/knock`, `make_knock` and `send_knock`, `rooms.knock` in `/sync` and knocked rooms in sliding sync. A knock is rescinded with `/leave`. Kicks and bans that end an invite or knock are also sent to the target's server when it is not in the room |
| [MSC2367](https://github.com/matrix-org/matrix-spec-proposals/pull/2367) | Reasons in all membership events | 1.1 | Implemented: `/join`, `/leave`, `/invite`, `/kick`, `/ban`, `/unban` and `/knock` accept `reason`. A leave through another server adds the reason to the `make_leave` template |
| [MSC3820](https://github.com/matrix-org/matrix-spec-proposals/pull/3820) | Room version 11 | 1.8 | Implemented: includes the redaction changes of MSC2174 and MSC3821 |
| [MSC3266](https://github.com/matrix-org/matrix-spec-proposals/pull/3266) | Room summary API | 1.15 | Implemented |
| [MSC3765](https://github.com/matrix-org/matrix-spec-proposals/pull/3765) | Rich text in room topics | 1.15 | Not implemented |
| [MSC4267](https://github.com/matrix-org/matrix-spec-proposals/pull/4267) | Forget room on leave ([`m.forget_forced_upon_leave` capability](https://spec.matrix.org/v1.18/client-server-api/#mforget_forced_upon_leave-capability)) | 1.18 | Implemented (opt-in via [`auto_forget_on_leave`](/administration/auto-forget-on-leave/)) |

## Registration

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC3231](https://github.com/matrix-org/matrix-spec-proposals/pull/3231) | Token authenticated registration | 1.2 | Implemented: registration tokens and the rate-limited, unauthenticated `/v1/register/m.login.registration_token/validity` check |
| [MSC1692](https://github.com/matrix-org/matrix-spec-proposals/pull/1692) | Terms of service at registration | 1.11 | Implemented (opt-in): with `client_api.registration_terms`, every registration flow ends with `m.login.terms`, whose params list the policies. The stage also has a fallback page at `/_matrix/client/v3/auth/m.login.terms/fallback/web` |

## Accounts

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC1929](https://github.com/matrix-org/matrix-spec-proposals/pull/1929) | Homeserver admin contact and support page | 1.10 | Implemented: `/.well-known/matrix/support` serves `global.support` and returns 404 when it is unset |
| [MSC4010](https://github.com/matrix-org/matrix-spec-proposals/pull/4010) | Push rules and account data | 1.10 | Implemented: setting `m.push_rules` or `m.fully_read` through `/account_data` fails with 405 `M_BAD_JSON`, for global and room account data |
| [MSC4025](https://github.com/matrix-org/matrix-spec-proposals/pull/4025) | Local user erasure requests | 1.10 | Implemented: see [erasure](#erasure) |

### Erasure

`POST /_matrix/client/v3/account/deactivate` with `"erase": true`, and the MAS `delete_user` call with `erase`, mark the user as erased in `roomserver_erased_users`.
From then on, their events are served redacted to clients and servers that see them only through `shared` or `world_readable` history visibility, in `/messages`, `/context`, `/event`, `/relations`, threads, `/sync` timelines, and federation `/event`, `/backfill` and `/get_missing_events`.
A user who was joined when an event was sent, or invited under `invited` visibility, still sees it unredacted, as does a server that had such a user; this follows Synapse and approximates "joined before the erasure".
No redaction events are sent and nothing is shared over federation, so other servers may still serve unredacted copies.
Erasure also deletes the user's account data and 3PIDs, after their identity server bindings have been removed.
Reactivating the account lifts the erasure, as in Synapse.

Deactivation, with or without `erase`, unbinds the user's 3PIDs from `id_server`, or from every identity server that holds a binding, and reports `id_server_unbind_result`.

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

## Media

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC2246](https://github.com/matrix-org/matrix-spec-proposals/pull/2246) | Asynchronous media uploads | 1.7 | Implemented: see [asynchronous uploads](#asynchronous-uploads) |
| [MSC2705](https://github.com/matrix-org/matrix-spec-proposals/pull/2705) | Animated thumbnails | 1.11 | Implemented: without `animated=true`, thumbnails are never animated; an animated GIF, APNG or WebP that would answer the request itself is replaced by a still JPEG rendition. With `animated=true`, an animated image no larger than the requested size is served as it is, and larger ones get a still thumbnail, which the spec allows |
| [MSC3860](https://github.com/matrix-org/matrix-spec-proposals/pull/3860) | Media download redirects | 1.7 | Implemented: `allow_redirect` is accepted, and Zendrite always serves media itself, which the spec allows. When a remote server answers a federation media request with a `Location` instead of the media, Zendrite fetches it from there, following up to five more redirects, and only from public addresses |

### Asynchronous uploads

`POST /_matrix/media/v1/create` reserves an `mxc://` URI and returns it with `unused_expires_at`, after which an unused reservation lapses (`media_api.async_uploads.unused_expiry`, default `24h`).
A user may hold `media_api.async_uploads.max_pending_per_user` reservations (default 10) that are not uploaded or expired; beyond that, `/create` fails with 429 `M_LIMIT_EXCEEDED`.
`PUT /_matrix/media/v3/upload/{serverName}/{mediaId}` uploads the content.
Only the user who created the URI may upload (403 `M_FORBIDDEN`), only once (409 `M_CANNOT_OVERWRITE_MEDIA`), and only before it expires (404 `M_NOT_FOUND`).

Downloads and thumbnails of a reserved URI, over the client and federation APIs, wait for the upload for `timeout_ms` (default 20 seconds, at most `media_api.async_uploads.max_download_wait`, default `60s`), and then fail with 504 `M_NOT_YET_UPLOADED`.
A download waiting on one replica sees an upload completed on another within half a second.
Expired reservations are deleted every hour.
When a remote server answers 504 for its own media, the client gets 504 `M_NOT_YET_UPLOADED` too, and nothing is cached.

### URL previews

`GET /_matrix/client/v1/media/preview_url` (and the deprecated `/_matrix/media/v3/preview_url`) is opt-in with `media_api.url_preview.enabled`, as in Synapse, since it makes the server fetch URLs for its users; disabled, it answers 404 `M_UNRECOGNIZED`.
For an HTML page it returns the `og:` properties of the page's head, falling back to `<title>` and the `description` meta tag.
`og:image` is downloaded and stored as local media, and the preview carries its `mxc://` URI with `matrix:image:size`, `og:image:type`, `og:image:width` and `og:image:height`; a URL that points at an image gets the same fields for the image itself.

Only public addresses are fetched: loopback, private, link-local, carrier-grade NAT and similar addresses are refused with 403 `M_FORBIDDEN`, also after redirects (at most five) and DNS changes, because the check applies to the address being connected to.
`ip_range_whitelist` allows non-public ranges such as an internal wiki, and `ip_range_blacklist` refuses more ranges and takes precedence.
Pages are read up to `max_page_size_bytes` (default 10 MiB) and images up to `max_file_size_bytes`; a failed fetch returns 502.
Previews are cached in memory per URL for `cache_duration` (default `1h`), concurrent requests for the same URL share one fetch, and `ts` is ignored, which the spec allows.

## Encryption & Security

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC1219](https://github.com/matrix-org/matrix-spec-proposals/pull/1219) | Server-side key backup | 1.1 | Implemented: backup versions, and uploading, fetching and deleting keys for a version, a room or a session |
| [MSC3916](https://github.com/matrix-org/matrix-spec-proposals/pull/3916) | Authenticated media | 1.11 | Implemented, including [URL previews](#url-previews). A `Location` in a remote multipart response is followed, see [MSC3860](#media) |
| [MSC2732](https://github.com/matrix-org/matrix-spec-proposals/pull/2732) | OLM fallback keys | 1.2 | Implemented: ordinary and dehydrated devices upload reusable fallback keys; claims mark them used and sync reports unused algorithms |
| [MSC3814](https://github.com/matrix-org/matrix-spec-proposals/pull/3814) | Dehydrated devices v2 | N/A | Opt-in (`msc3814`) |
| [MSC4115](https://github.com/matrix-org/matrix-spec-proposals/pull/4115) | Membership metadata on events | 1.11 | Implemented: `unsigned.membership` is the requesting user's membership at each event, including the change made by the user's own membership events, wherever events pass the history visibility filter (sync timelines, `/messages`, `/context`, `/event`, `/relations`, threads). Events without a known membership, such as the `state` section of `/sync`, carry none, which the spec allows |

## Federation

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC3706](https://github.com/matrix-org/matrix-spec-proposals/pull/3706) | Partial state in `/send_join` (faster joins) | 1.6 | Implemented |
| [MSC3030](https://github.com/matrix-org/matrix-spec-proposals/pull/3030) | Jump to date API endpoint | 1.6 | Implemented: see [jump to date](#jump-to-date) |
| [MSC3938](https://github.com/matrix-org/matrix-spec-proposals/pull/3938) | Remove deprecated `keyId` parameters from `/keys` endpoints | 1.6 | Implemented: `GET /_matrix/key/v2/query/{serverName}` returns all keys of the server. The deprecated forms with a key ID are still served |
| [MSC2444](https://github.com/matrix-org/matrix-spec-proposals/pull/2444) | Peeking over federation | N/A | Opt-in (`msc2444`) |
| [MSC4436](https://github.com/matrix-org/matrix-spec-proposals/pull/4436) | Case-insensitive server ACLs | 1.19 | Implemented: `allow` and `deny` globs match the whole server name, ignoring case |
| [MSC2753](https://github.com/matrix-org/matrix-spec-proposals/pull/2753) | Peeking via `/sync` | N/A | Opt-in (`msc2753`) |
| [MSC3821](https://github.com/matrix-org/matrix-spec-proposals/pull/3821) | Update redaction rules, again | 1.8 | Implemented: from room version 11, redacting an `m.room.member` event keeps `content.third_party_invite.signed`, so redacted events hash to the same event ID as on other servers |
| [MSC4311](https://github.com/matrix-org/matrix-spec-proposals/pull/4311) | Create event in invite and knock stripped state | 1.16 | Implemented: `invite_room_state` and `knock_room_state` are sent as full PDUs including `m.room.create`. From room version 12, invites whose state holds PDUs that fail validation are rejected with `M_MISSING_PARAM`. Missing or stripped-only state is accepted with a warning, per the MSC's migration section, as are all failures in earlier room versions. Knock state entries that fail validation are dropped |

### Jump to date

`GET /_matrix/client/v1/rooms/{roomId}/timestamp_to_event` returns the event closest to `ts` in the direction `dir` that the user may see.
`GET /_matrix/federation/v1/timestamp_to_event/{roomId}` answers the same question for another server, from local events only.
It returns the closest event the requesting server may see, checking at most 1000 events, and a server banned by the room's ACL gets 403.

The local history of a room can have gaps, for example before the server joined it.
Zendrite knows them from the backward extremities, the events whose `prev_events` it never fetched.
When such a gap can hold an event closer to `ts` than the local result, and the user may see the event after the gap, the client endpoint asks up to five other servers in the room, those with the most joined members first.
The first server that answers decides: if its event is closer, Zendrite backfills it with up to 20 events before it, so that `/context` and `/messages` can use it, and returns it when the user may see it.
Otherwise, or when no server answers, the local result stands.

## Application services

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC3905](https://github.com/matrix-org/matrix-spec-proposals/pull/3905) | Application services should only be interested in local users | 1.5 | Implemented: `users` namespaces only match users on this server or its virtual hosts, so a remote sender, member or joined user no longer makes an event interesting to an application service |
| [MSC2659](https://github.com/matrix-org/matrix-spec-proposals/pull/2659) | Application service ping endpoint | 1.7 | Implemented: `POST /_matrix/client/v1/appservice/{appserviceId}/ping`, authenticated with the application service's `as_token`, calls `POST /_matrix/app/v1/ping` with the `hs_token` and the `transaction_id`, and returns `duration_ms`. Failures return `M_URL_NOT_SET`, `M_CONNECTION_FAILED`, `M_CONNECTION_TIMEOUT` (after 30 seconds) or `M_BAD_STATUS` with the status and up to 4 KiB of the body |

## Authentication

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC2265](https://github.com/matrix-org/matrix-spec-proposals/pull/2265) | Case folding of email addresses | 1.1 | Implemented: email 3PIDs are case-folded before they are stored, looked up or sent to an identity server. Addresses stored by older versions are folded on upgrade, except where the folded form is already in use, which is logged |
| [MSC2713](https://github.com/matrix-org/matrix-spec-proposals/pull/2713) | Remove deprecated identity service endpoints | 1.1 | Implemented: identity servers are only called through the authenticated v2 API, with the client's `id_access_token` or, for `email_validation_server`, a token the homeserver registers for its server notices user. 3PID invites require `id_access_token` and use hashed lookups |
| [MSC3288](https://github.com/matrix-org/matrix-spec-proposals/pull/3288) | Room type in `store-invite` | 1.2 | Implemented: 3PID invites send `room_type`, along with the room's name, avatar, join rule and alias |
| [MSC2858](https://github.com/matrix-org/matrix-spec-proposals/pull/2858) | Multiple SSO identity providers | 1.1 | Not applicable: SSO is only available through [MSC3861](#msc3861-oidc-delegated-authentication), where the authentication service offers the upstream providers, so the `m.login.sso` flow lists no `identity_providers` |
| [MSC2918](https://github.com/matrix-org/matrix-spec-proposals/pull/2918) | Refresh tokens | 1.3 | Implemented: clients sending `refresh_token: true` to `/login` or `/register` get a refresh token and an access token that expires after `client_api.refreshable_access_token_lifetime` (default `5m`, `0s` disables refresh tokens), which `/refresh` renews. An expired access token returns 401 `M_UNKNOWN_TOKEN` with `soft_logout: true`. A used refresh token stays valid until its successor is used, so a lost `/refresh` response can be retried. With [MSC3861](#msc3861-oidc-delegated-authentication), the authentication service issues the tokens |
| [MSC3882](https://github.com/matrix-org/matrix-spec-proposals/pull/3882) | Sign in a new session from an existing one | 1.7 | Implemented: `POST /_matrix/client/v1/login/get_token` asks for the password every time and returns a single-use `m.login.token` that expires after two minutes. `/login` lists the `m.login.token` flow with `get_login_token: true`, and `/capabilities` reports `m.get_login_token`. Redeeming a token deletes it first, so concurrent logins cannot share one. With [MSC3861](#msc3861-oidc-delegated-authentication), the endpoint returns 403 and the capability is disabled |
| [MSC3861](https://github.com/matrix-org/matrix-spec-proposals/pull/3861) | Next-gen auth (OIDC) | 1.15 | Opt-in (`msc3861`) |

## VoIP

| MSC | Title | Spec | Status |
| --- | --- | --- | --- |
| [MSC4140](https://github.com/matrix-org/matrix-spec-proposals/pull/4140) | Cancellable delayed events | N/A | Implemented, disabled by default: see [delayed events](#delayed-events) |
| [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) | MatrixRTC transport discovery | N/A | Implemented (configure via [`rtc_foci`](/administration/matrixrtc/)) |

### Delayed events

[MSC4140](https://github.com/matrix-org/matrix-spec-proposals/pull/4140) lets a client schedule an event for the server to send later, unless the client restarts or cancels the schedule first.
Element Call uses it to end the call membership of a client that crashes or loses its connection; see [MatrixRTC](/administration/matrixrtc/#delayed-events).
Zendrite implements the unstable API that matrix-js-sdk and Synapse use:

- The `org.matrix.msc4140.delay` query parameter, in milliseconds, on `PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}` and `PUT /_matrix/client/v3/rooms/{roomId}/state/{eventType}/{stateKey}` schedules the event and returns `{"delay_id": ...}` instead of an event ID.
- `PUT /_matrix/client/unstable/org.matrix.msc4140/rooms/{roomId}/delayed_event/{eventType}/{txnId}` schedules an event from a body of `delay_ms`, `content` and an optional `state_key`.
- `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayId}/{cancel|restart|send}`, and the older form `POST .../delayed_events/{delayId}` with `{"action": ...}`, manage a scheduled event.
  As in Synapse, these work without an access token, since the delay ID identifies the event; with one, they are rate-limited per device instead of per address.
- `GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayId}` returns one of the user's delayed events, including a `finalised` object once it was sent, canceled or failed.
- `GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events` lists the user's scheduled events, or with `status=finalised` the finalized ones, optionally only those named by `delay_id` parameters, with the field names of earlier revisions (`delay`, `running_since`) included as Synapse does.
  The list is not paginated, so `from` is ignored.

When scheduling, the event is built against the room's current state, so a malformed event, or one the user may not send, is refused at once.
A delay must be a positive integer; one above the maximum fails with 400 `ORG.MATRIX.MSC4140_DELAY_TOO_LARGE`, and a user with as many events scheduled as allowed gets 429 `M_LIMIT_EXCEEDED` with `Retry-After` set to when their next event is due.
When delayed events are disabled, scheduling fails with 403 `M_FORBIDDEN`.

At its send time, the event is built from the room state at that moment and sent through the same path as a client's send, including the authorization checks.
A failure is recorded as the event's `finalised.error` and is not retried.
The `send` action differs: if sending fails, it returns the error and keeps the event scheduled.
The sent event carries `org.matrix.msc4140.delay_id` in its `unsigned` data in the sender's `/sync`, and no `transaction_id`.
A message event scheduled with `org.matrix.msc4354.sticky_duration_ms`, through either scheduling endpoint, is sent as a [sticky event](#sticky-events), whose sticky period starts when it is sent.
As for a direct send, the parameter is ignored for state events.

Retrying an action is safe: `send` succeeds for an event that was already sent, and `cancel` for one that was already canceled or failed.
Other actions on a finalized event return 404 `M_NOT_FOUND` as in Synapse, rather than the 409 of the current MSC text, because matrix-js-sdk schedules a new delayed leave event when a restart finds none.

A user's scheduled state event is canceled when another user changes the same state, and all of a user's delayed events are deleted when the account is deactivated.
Finalized events are kept for seven days, and at most 1000 per user.

Delayed events are stored in the user API database and sent by the process that serves clients.
In HA deployments, that is the active process: a successor picks up the schedule from the database, and resends any event its predecessor was sending when it stopped.
The resend uses the same roomserver transaction, keyed by the delay ID, so an event that did reach the room is not sent twice.

`/versions` advertises `org.matrix.msc4140` while delayed events are enabled, and `/capabilities` reports the limits as `org.matrix.msc4140.delayed_events`, where a `max_delay_ms` of 0 means disabled.

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
