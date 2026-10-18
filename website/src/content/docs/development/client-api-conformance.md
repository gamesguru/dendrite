---
title: Client API conformance
description: Version discovery policy, audited implementation gaps, and experimental feature flags.
---

## Advertisement decision

`GET /_matrix/client/versions` advertises `r0.0.1`, `r0.1.0`, `r0.2.0`, `r0.3.0`, `r0.4.0`, `r0.5.0`, `r0.6.1`, and `v1.0` to `v1.12`.
The versions up to `v1.2` are a compatibility baseline, not a new certification of every historical requirement.
`v1.3` to `v1.12` are advertised because the audit of every MSC merged into the specification since v1.1 ([issue #478](https://codefloe.com/pat-s/zendrite/issues/478)) leaves no unresolved requirement in those releases; the sections below record the evidence.
`v1.13` and later are not advertised while #478 lists open gaps in them.

The [version discovery contract](https://spec.matrix.org/v1.19/client-server-api/#api-versions) distinguishes specification versions from individual experimental features.
Authenticated media, room summaries, and selected other newer features are implemented, but their presence does not establish support for the other requirements introduced in the same release.
Room versions returned by `/capabilities` are a separate namespace and must not be copied into `/versions`.

## Scope and evidence

This is a source-level audit of the next candidate versions, v1.3 through v1.12, with targeted checks of later features that could otherwise motivate raising the ceiling.
It compares the release requirements with routing, handler behavior, storage, and existing tests in this repository.
It does not claim an exhaustive behavioral certification through v1.19 or a fresh Sytest/Complement pass.
A registered route establishes reachability only; authorization, pagination, response shape, and configuration dependence require separate evidence.
An unresolved mandatory requirement is sufficient to withhold a version advertisement.
Optional features and client-only presentation changes are not automatically blockers.

Source paths below are relative to the repository root.

### v1.3

The [v1.3 changes](https://spec.matrix.org/v1.3/changelog/) introduce optional `from` on messages, refresh tokens, relationship retrieval and aggregation rules, relaxed replies, and `knock_restricted` rooms.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Messages without `from` | `syncapi/routing/messages.go` constructs earliest/latest topology positions according to direction. | Implemented path; this alone does not certify pagination. |
| Refresh tokens | `clientapi/routing/refresh.go` serves `/v3/refresh` for refresh tokens issued by `/login` and `/register` for clients that request them; with MSC3861, `msc3861_sso_compat.go` delegates refresh to the provider. `clientapi/refresh_token_test.go` covers rotation, retrying a lost response, expiry and logout. | Implemented; access tokens of clients that request a refresh token expire after `client_api.refreshable_access_token_lifetime` (default five minutes), and other clients keep non-expiring tokens. |
| Relationship retrieval | `syncapi/routing/relations.go` serves all three `/v1/rooms/{roomID}/relations/...` forms in topological order; `syncapi/routing/relations_test.go` covers them. | Implemented; see below. |
| Aggregation framework and relaxed replies | `syncapi/internal/aggregations.go` calculates bundled relations for the endpoints the spec lists, including limited `/sync` timelines. | v1.3 defines no relation type requiring aggregation; the framework is covered by `syncapi/aggregations_test.go`. |
| `knock_restricted` | Room-version handling uses gomatrixserverlib; `roomserver/roomserver_test.go` exercises restricted and knock-restricted joins with room version 10. | Implementation and focused tests exist; not evidence for unrelated client APIs. |

The [v1.3 relations endpoint](https://spec.matrix.org/v1.3/client-server-api/#get_matrixclientv1roomsroomidrelationseventid) accepts pagination tokens from `/messages` and `/sync`, returns children in topological order, and specifies 404 for a missing or inaccessible parent.
`syncapi/routing/relations.go` accepts its own tokens, `/messages` topology tokens and `/sync` stream tokens for `from` and `to`, with the same cursor semantics as `/messages`.
The storage query orders children by event topology, so a backfilled child sorts by its depth rather than its arrival.
A missing parent, one in another room and one hidden by history visibility all return 404 `M_NOT_FOUND`.
Children hidden by history visibility or sent by ignored users are left out, and each child carries its own bundled aggregations.
These resolve the relations gaps that blocked v1.3.
Timestamp massaging (MSC3316) is limited to application services, and v1.3 is advertised.

### v1.4 and v1.5

The [v1.4 changes](https://spec.matrix.org/v1.4/changelog/) add edits, threads, private and threaded receipts, room-type directory filters, room-type preservation on upgrade, a server-ACL push rule, and media CORP headers.
The v1.3 relations behavior is covered above.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Private receipts | `clientapi/routing/receipt.go` accepts `m.read.private`; `readmarker.go` accepts optional markers; `syncapi/streams/stream_receipt.go` excludes another user's private receipts. | Retain the existing MSC2285 compatibility flag. |
| Thread listing | `syncapi/routing/threads.go` serves `/v1/rooms/{roomID}/threads` with `include` filtering and pagination. | Implemented; covered by `threads_test.go`. |
| Threaded receipts and notification counts | `clientapi/routing/receipt.go` validates `thread_id`, receipts keep their thread identity, and `syncapi/streams/stream_notificationdata.go` serves per-thread counts when the filter sets `unread_thread_notifications`. | Implemented; covered by `TestThreadNotificationCounts` and `TestThreadReplyCountedInThread`. |
| Edit and thread bundles | `syncapi/internal/aggregations.go` bundles the latest valid edit under `m.replace` and the thread summary under `m.thread`, filtered for history visibility and ignored users. | Implemented. |
| Room-type directory filters | `clientapi/routing/directory_public.go` and `federationapi/routing/publicrooms.go` filter with `roomserver/api.FilterPublicRooms` and return `room_type`; remote client results are filtered again. | Implemented; covered by `TestPublicRoomsRoomTypes`, `TestPublicRoomsRemoteRoomTypes` and `TestFederationPublicRoomsRoomTypes`. |
| Room upgrades | `roomserver/internal/perform/perform_upgrade.go` copies the old create event's content, including `type`, into the new create event. | Implemented. |
| Server ACL push rule | `internal/pushrules/default_override.go` installs `.m.rule.room.server_acl`. | Implemented. |
| Media CORP header | `mediaapi/routing/routing.go` sends `Cross-Origin-Resource-Policy: cross-origin`. | Implemented. |

[v1.5](https://spec.matrix.org/v1.5/changelog/) adds reference relations and documents in-room verification requests.
References are bundled under `m.reference` and can be paginated through `/relations`.
Application service `users` namespaces only match local users (MSC3905).
Both v1.4 and v1.5 are advertised.
MSC2836's experimental relationship API is not the stable `m.thread` protocol or thread-list endpoint.

### v1.6

The [v1.6 changes](https://spec.matrix.org/v1.6/changelog/) add jump to date, remove the `keyId` parameters from the key endpoints, and extend `send_join` for faster joins.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Jump to date (MSC3030) | `syncapi/routing/timestamp_to_event.go` serves `/v1/rooms/{roomID}/timestamp_to_event` and the federation `/v1/timestamp_to_event/{roomID}`. When the local history has a gap that may hold a closer event, the client endpoint asks other servers in the room and backfills their answer. `timestamp_to_event_test.go` covers both endpoints and the remote lookup. | Implemented. |
| Key queries without `keyId` (MSC3938) | `federationapi/routing/routing.go` serves `GET /_matrix/key/v2/query/{serverName}`; `TestNotaryServer` covers it. | Implemented. |
| Faster joins (MSC3706, MSC3943) | `federationapi/internal/federationclient.go` joins with `omit_members=true`. Serving `send_join`, Zendrite always returns the full state with `members_omitted` unset, which the spec allows. | Compliant. |

### v1.7

The [v1.7 changes](https://spec.matrix.org/v1.7/changelog/) add asynchronous media uploads, media download redirects, the application service ping, reactions, signing in from an existing session, and scope transaction IDs to devices.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Asynchronous uploads (MSC2246) | `mediaapi/routing/async_upload.go` serves `POST /v1/create` and `PUT /upload/{serverName}/{mediaId}`, and makes downloads of pending media wait for `timeout_ms` before 504 `M_NOT_YET_UPLOADED`. `async_upload_test.go` covers the errors, concurrent uploads, waiting across replicas and the pending limit. | Implemented. |
| Media redirects (MSC3860) | Zendrite never redirects, which the spec allows. `mediaapi/routing/media_redirect.go` follows a `Location` in a remote federation media response; `media_redirect_test.go` covers it. | Compliant. |
| Application service ping (MSC2659) | `clientapi/routing/appservice_ping.go` serves `POST /v1/appservice/{appserviceId}/ping`; `appservice_ping_test.go` covers every error code. | Implemented. |
| Duplicate annotations (MSC2677) | `roomserver_annotations` indexes unredacted annotations, and `validateAnnotation` in `clientapi/routing/sendevent.go` returns 400 `M_DUPLICATE_ANNOTATION`; `clientapi/annotations_test.go` covers it. | Implemented. |
| Login tokens (MSC3882) | `clientapi/routing/login_token.go` serves `POST /v1/login/get_token` behind password re-authentication, with the `m.get_login_token` capability and `get_login_token` on the `m.login.token` flow; `login_token_test.go` covers single use. | Implemented; disabled with MSC3861, which the spec allows. |
| Transaction IDs per device (MSC3970) | `internal/transactions` keys its cache by device, or by application service, and endpoint. | Implemented. |

### v1.8

The [v1.8 changes](https://spec.matrix.org/v1.8/changelog/) add room version 11, `matrix-fed` SRV discovery, `+` in user IDs, and require reporters of an event to be joined to its room.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Room version 11 (MSC3820, MSC2174, MSC2175, MSC2176, MSC3821, MSC3989) | gomatrixserverlib implements the version; the roomserver applies room v11 redactions from `content.redacts` and keeps `third_party_invite.signed` when redacting membership events. | Implemented. |
| `matrix-fed` SRV records (MSC4040) | gomatrixserverlib's `fclient/resolve.go` looks up `_matrix-fed._tcp` before the deprecated `_matrix._tcp`. | Implemented. |
| `+` in user IDs (MSC4009) | `internal/validate.go` accepts `+` in localparts. | Implemented. |
| Reporting events (MSC2249) | `clientapi/routing/report_event.go` returns 404 unless the reporter is joined to the room. | Implemented. |

### v1.9

[v1.9](https://spec.matrix.org/v1.9/changelog/) adds the `.m.rule.suppress_edits` default push rule (MSC3958), which `internal/pushrules/default_override.go` installs; the other changes are clarifications.

### v1.10

The [v1.10 changes](https://spec.matrix.org/v1.10/changelog/) add support discovery, local erasure requests, recursive relations, optional authentication on `/versions`, and `Retry-After` on rate limits, and reserve `m.push_rules` in the account data API.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Support discovery (MSC1929) | `clientapi/routing/routing.go` serves `/.well-known/matrix/support` from `global.support`, or 404 when unset; `wellknown_support_test.go` and `config_support_test.go` cover it. | Implemented. |
| Erasure requests (MSC4025) | `/account/deactivate` and the MAS `delete_user` call record the user in `roomserver_erased_users`. `ApplyHistoryVisibilityFilter` and the federation `/event`, `/backfill` and `/get_missing_events` paths serve their events redacted to viewers without membership at the event; account data and 3PIDs are deleted. `clientapi/erasure_test.go`, `roomserver/erasure_test.go` and `history_visibility_erasure_test.go` cover it. | Implemented. |
| Push rules in account data (MSC4010) | `clientapi/routing/account_data.go` rejects `m.push_rules` and `m.fully_read` with 405 `M_BAD_JSON`; `TestServerManagedAccountData` covers it. | Implemented. |
| Recursive relations (MSC3981) | `syncapi/routing/relations.go` parses `recurse`, follows relations three levels deep and reports `recursion_depth`. | Implemented. |
| Optional authentication on `/versions` (MSC4026) | `/versions` ignores credentials; `versions_test.go` covers unauthenticated and invalid bearer requests. | Compliant. |
| `Retry-After` (MSC4041) | `internal/httputil/rate_limiting.go` sets `Retry-After` next to `retry_after_ms`. | Implemented. |
| Application service login without a valid `as_token` | `clientapi/auth/login_application_service.go` requires an application service token. | Implemented. |

### v1.11

The [v1.11 changes](https://spec.matrix.org/v1.11/changelog/) add authenticated media with URL previews, animated thumbnails, terms of service at registration, membership metadata on events, and cross-signing uploads without UIA.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Authenticated media (MSC3916) | `mediaapi/routing/routing.go` serves the `/_matrix/client/v1/media/*` and `/_matrix/federation/v1/media/*` routes; `mediaapi/routing/url_preview.go` serves `preview_url` when `media_api.url_preview.enabled` is set, and `url_preview_test.go` covers OpenGraph parsing, images, caching and address blocking. | Implemented; previews are opt-in, as in Synapse. |
| Animated thumbnails (MSC2705) | `mediaapi/routing/download.go` never answers a thumbnail request without `animated=true` with an animated image; `animated_thumbnail_test.go` and `thumbnailer/animated_test.go` cover GIF, APNG and WebP. | Implemented. |
| Terms of service (MSC1692) | `client_api.registration_terms` adds `m.login.terms` to every registration flow, and `auth_fallback.go` serves its fallback page; `register_terms_test.go` covers both. | Implemented; optional. |
| Membership on events (MSC4115) | `ApplyHistoryVisibilityFilter` records the requesting user's membership at each event, which `synctypes.ToClientEvent` serves as `unsigned.membership`; `membership_annotation_test.go` covers `/sync`, `/messages` and `/event`. | Implemented. |
| Cross-signing without UIA (MSC3967) | `clientapi/routing/key_crosssigning.go` only asks for UIA when a master key exists and the upload changes the keys. | Implemented. |
| Optional access token on `/account/password` and `/account/deactivate` | Both require an access token, which the spec allows: the server may offer different flows depending on whether a token is given. Password reset by email is not offered. | Compliant. |
| Application services and `sender_localpart` | `setup/config/config_appservice.go` adds the `sender_localpart` user to the service's exclusive user namespaces, so events concerning it are delivered. | Implemented. |

### v1.12

The [v1.12 changes](https://spec.matrix.org/v1.12/changelog/) add account locking, media `Content-Type` and `Content-Disposition` rules, `via` on joins and knocks, guest access to authenticated media, and marking rooms as unread.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Account locking (MSC3939) | `internal/httputil/httpapi.go` refuses locked accounts with 401 `M_USER_LOCKED` and `soft_logout: true` on every authenticated route except `/logout` and `/logout/all`; `clientapi/routing/login.go` and `refresh.go` refuse new tokens. `clientapi/routing/admin_lock.go` serves `GET` and `PUT /v1/admin/lock/{userId}` (MSC4323). `clientapi/account_lock_test.go` and `internal/httputil/locked_test.go` cover them. | Implemented; with MSC3861 the authentication service locks accounts. |
| Media `Content-Type` and `Content-Disposition` (MSC2701, MSC2702) | `mediaapi/routing/upload.go` stores uploads without a type as `application/octet-stream`; `download.go` serves that type for untyped media and an `inline` disposition with a file name for thumbnails. `content_disposition_test.go` covers both. | Implemented. |
| `via` on joins and knocks (MSC4156) | `clientapi/routing/joinroom.go` reads `via`, falling back to the deprecated `server_name`. | Implemented. |
| Guest access to authenticated media (MSC4189) | `mediaapi/routing/routing.go` serves the `/v1/media` download and thumbnail routes through `MakeHTTPAPI`, which admits guests. | Implemented. |
| 403 on profile lookups (MSC4170) | The response is allowed, not required. Zendrite does not restrict lookups of its own profiles, and `clientapi/routing/profile.go` passes on a remote server's 403. | Compliant. |
| Marking rooms as unread (MSC2867) | `m.marked_unread` is room account data, which needs no server support. | Client-only. |

### v1.13 and later candidates

| Candidate | Evidence and remaining blockers | Decision |
| --- | --- | --- |
| [v1.15 room summaries](https://spec.matrix.org/v1.15/client-server-api/#room-summaries) | `/v1/room_summary/{roomIDOrAlias}` calls `GetRoomSummary`; `room_summary_test.go` tests access, response shape, and caching. | Selected feature implemented, not full v1.15 certification. |
| [v1.19](https://spec.matrix.org/v1.19/client-server-api/) | Earlier mandatory behavior remains unresolved; no full audit of intervening releases has established conformance. | Do not advertise the latest version based on route counts. |

### Related route-audit findings

- **3PID management:** `clientapi/routing/routing.go` provides listing, legacy association, deletion, and email token requests, but lacks the distinct `/account/3pid/add`, `/bind`, `/unbind`, and MSISDN token routes.
  Password reset by email (`/account/password/email/requestToken` and `/account/password` without an access token) works when `client_api.email_validation_server` is set; MSISDN password reset returns `M_THREEPID_MEDIUM_NOT_SUPPORTED`.
  MSC3861 intentionally disables local account mutation and delegates account management; that configuration is not evidence for native 3PID support.
- **Mutual rooms:** No mutual-rooms handler is registered in client or sync routing.
  Treat this as a feature gap, not as proof that an unspecified stable release requires an experimental endpoint.
- **Notifications ([#312](https://codefloe.com/pat-s/zendrite/issues/312)):** `GetNotifications` now validates query parameters and calls `QueryNotifications`; `notification_test.go` covers response behavior.
  This is not a stub on the audited branch and is not listed as an unresolved blocker here.
- **Room summaries ([#314](https://codefloe.com/pat-s/zendrite/issues/314)):** Stable and compatibility routes are present, backed by `GetRoomSummary` and focused tests.
  This resolves the route gap without establishing full v1.15 support.

## Experimental feature discovery

`clientapi/routing/routing.go` builds the feature map at startup.
Unknown or inactive MSC names in configuration must not turn into advertised support or override configuration-derived flags.
The explicit configurable list mirrors the implemented cases in `setup/mscs/mscs.go`.

| Flag | Advertisement condition | Implementation evidence |
| --- | --- | --- |
| `org.matrix.e2e_cross_signing` | Always, existing compatibility flag. | Signing-key upload and signature routes and handlers in `clientapi/routing`. |
| `org.matrix.msc2285.stable` | Always, existing compatibility flag. | Private receipts and optional read markers, including filtering in the sync receipt stream. |
| `org.matrix.msc3916.stable` | Always, existing compatibility flag. | Authenticated download, thumbnail, and configuration handlers in `mediaapi/routing/routing.go`. |
| `org.matrix.simplified_msc3575` | Always, existing compatibility flag. | Native sliding-sync handler and compatibility route in `syncapi/routing/routing.go`. |
| `org.matrix.msc4143` | True only with nonempty `global.rtc_foci`; false otherwise. | RTC transport endpoint and well-known focus discovery share that configuration. |
| `org.matrix.msc4222` | Always. | The `use_state_after` and `org.matrix.msc4222.use_state_after` `/sync` parameters in `syncapi/sync/request.go`, for clients that check the flag before opting in. |
| `org.matrix.msc4140` | True only with a positive `client_api.delayed_events.max_event_delay_duration` and `max_delayed_events_per_user`; false otherwise. | Delayed event scheduling, management and scheduler in `clientapi/routing/delayed_events.go`, with the `org.matrix.msc4140.delayed_events` capability. |
| `uk.tcpip.msc4133`, `uk.tcpip.msc4133.stable` | Always. | Profile field endpoints at the unstable and v3 paths in `clientapi/routing/profile.go`, with the `m.profile_fields` capability. |
| `org.matrix.msc2444`, `org.matrix.msc2753`, `org.matrix.msc2836`, `org.matrix.msc3814` | Only when the respective implemented MSC is configured. | Federation peeking, client peeking, experimental relations, and dehydrated-device modules respectively. |
| `org.matrix.msc3861` | Only when MSC3861 is enabled. | Delegated authentication routes and token introspection. |
| `org.matrix.msc2965`, `org.matrix.msc2967` | Implied by MSC3861, never enabled by listing those names alone. | Authentication metadata and OAuth scope integration. |

These retained flags describe existing feature implementations, not an exhaustive certification of every proposal revision.
MSC4115 membership annotation is already applied in sync; listing `msc4115` does not activate a separate implementation and no longer synthesizes an `org.matrix.msc4115` flag.
Likewise, listing `msc4143` alone cannot advertise RTC availability without configured foci.

## Verification and promotion procedure

`clientapi/routing/versions_test.go` exercises the real client router with a user API that cannot service any calls, proving discovery needs neither authentication nor storage.
It checks the exact version list and feature map with default settings, supported opt-ins, duplicate and unknown entries, OIDC on/off, RTC on/off, unauthenticated requests, invalid bearer credentials, and CORS preflight.
The existing `TestMSC3861_VersionsAdvertisesMSC2965` additionally covers discovery with a configured user API stack.

```sh
# Run discovery regression tests.
go test -tags goolm ./clientapi/routing -run 'TestVersions|TestMSC3861_VersionsAdvertisesMSC2965' -count=1
```

Before raising the ceiling, fix any remaining blockers and add behavioral tests for newly required endpoints.
Then review every server requirement and applicable clarification in the candidate release, run the relevant conformance suites in supported authentication configurations, and record that evidence here alongside the version-list change.
Do not equate a passing discovery test with protocol conformance.
