---
title: Client API conformance
description: Version discovery policy, audited implementation gaps, and experimental feature flags.
---

## Advertisement decision

`GET /_matrix/client/versions` retains the existing advertised versions: `r0.0.1`, `r0.1.0`, `r0.2.0`, `r0.3.0`, `r0.4.0`, `r0.5.0`, `r0.6.1`, `v1.0`, `v1.1`, and `v1.2`.
The audit for [issue #320](https://codefloe.com/pat-s/zendrite/issues/320) does not establish support for any newer complete client-server specification version.
The existing list is a compatibility baseline, not a new certification of every historical requirement.

The [version discovery contract](https://spec.matrix.org/v1.19/client-server-api/#api-versions) distinguishes specification versions from individual experimental features.
Authenticated media, room summaries, and selected other newer features are implemented, but their presence does not establish support for the other requirements introduced in the same release.
Room versions returned by `/capabilities` are a separate namespace and must not be copied into `/versions`.

## Scope and evidence

This is a source-level audit of the next candidate versions, v1.3 through v1.6, with targeted checks of later features that could otherwise motivate raising the ceiling.
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
| Refresh tokens | `clientapi/routing/routing.go` registers `/v3/refresh` only with MSC3861; `msc3861_sso_compat.go` delegates refresh to the provider. | Configuration-dependent, not native password-login refresh support; non-expiring tokens alone are not a blocker. |
| Relationship retrieval | `syncapi/routing/routing.go` registers all three `/v1/rooms/{roomID}/relations/...` forms. | Routes exist, but behavior blocks advertisement; see below. |
| Aggregation framework and relaxed replies | `syncapi/synctypes/clientevent.go` preserves event content and unsigned data, but does not calculate bundled relations. | v1.3 defines no relation type requiring aggregation, so absent bundles alone are not a v1.3 blocker; later versions do require them. |
| `knock_restricted` | Room-version handling uses gomatrixserverlib; `roomserver/roomserver_test.go` exercises restricted and knock-restricted joins with room version 10. | Implementation and focused tests exist; not evidence for unrelated client APIs. |

The [v1.3 relations endpoint](https://spec.matrix.org/v1.3/client-server-api/#get_matrixclientv1roomsroomidrelationseventid) accepts pagination tokens from `/messages` and `/sync`, returns children in topological order, and specifies 404 for a missing or inaccessible parent.
In `syncapi/routing/relations.go`, `from` and `to` are parsed with `types.NewStreamPositionFromString`, which accepts decimal integers rather than the topology and streaming tokens emitted by those endpoints.
`syncapi/storage/shared/storage_sync.go` and the SQL relations tables use relation stream positions rather than event topology for pagination.
The handler filters child events for history visibility but never loads and checks the parent; an unknown parent can produce an empty successful response.
These are behavioral gaps even though all three routes exist, and prevent promoting v1.3.

### v1.4 and v1.5

The [v1.4 changes](https://spec.matrix.org/v1.4/changelog/) add edits, threads, private and threaded receipts, room-type directory filters, room-type preservation on upgrade, a server-ACL push rule, and media CORP headers.
The v1.3 relations blockers remain relevant.

| Requirement | Implementation evidence | Assessment |
| --- | --- | --- |
| Private receipts | `clientapi/routing/receipt.go` accepts `m.read.private`; `readmarker.go` accepts optional markers; `syncapi/streams/stream_receipt.go` excludes another user's private receipts. | Retain the existing MSC2285 compatibility flag. |
| Thread listing | No `/v1/rooms/{roomID}/threads` route in client or sync routing. | Missing endpoint. |
| Threaded receipts and notification counts | `SetReceipt` does not parse `thread_id`; receipt production carries no thread identifier. | Room-level receipts do not establish threaded behavior. |
| Edit and thread bundles | Relations are stored and retrievable, but client event conversion does not generate `unsigned.m.relations` summaries. | Handling raw edits/replies is not server-side aggregation support. |
| Other v1.4 additions | Require a separate behavioral pass for filters, upgrades, push rules, and media headers after the above blockers are fixed. | Not certified by this audit. |

[v1.5](https://spec.matrix.org/v1.5/changelog/) adds reference relations and documents in-room verification requests.
Generic relation storage can retain `m.reference` events, but its presence does not prove the required bundling and pagination behavior.
Neither v1.4 nor v1.5 is eligible for advertisement.
MSC2836's experimental relationship API is not the stable `m.thread` protocol or thread-list endpoint.

### v1.6 and later candidates

| Candidate | Evidence and remaining blockers | Decision |
| --- | --- | --- |
| [v1.6](https://spec.matrix.org/v1.6/changelog/) | `/v1/rooms/{roomID}/timestamp_to_event` is absent from client and sync routing, in addition to the earlier relations/thread gaps. | Do not advertise. |
| [v1.7](https://spec.matrix.org/v1.7/changelog/) | Reactions can be stored as relations; this does not demonstrate aggregated reaction behavior, asynchronous media upload, or login-token issuance. | No full certification; earlier blockers remain. |
| [v1.10](https://spec.matrix.org/v1.10/changelog/) | `Relations` does not parse `recurse` or traverse descendant relations as required by the recursive relations API. | Do not advertise. |
| [v1.11](https://spec.matrix.org/v1.11/changelog/) | `mediaapi/routing/routing.go` registers authenticated client media routes, and sync annotates membership without an MSC4115 switch. | Keep feature-level discovery; earlier relations/thread/timestamp blockers still prevent a version bump. |
| [v1.15 room summaries](https://spec.matrix.org/v1.15/client-server-api/#room-summaries) | `/v1/room_summary/{roomIDOrAlias}` calls `GetRoomSummary`; `room_summary_test.go` tests access, response shape, and caching. | Selected feature implemented, not full v1.15 certification. |
| [v1.19](https://spec.matrix.org/v1.19/client-server-api/) | Earlier mandatory behavior remains unresolved; no full audit of intervening releases has established conformance. | Do not advertise the latest version based on route counts. |

### Related route-audit findings

- **3PID management:** `clientapi/routing/routing.go` provides listing, legacy association, deletion, and email token requests, but lacks the distinct `/account/3pid/add`, `/bind`, `/unbind`, and MSISDN token routes.
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

Before raising the ceiling, fix the identified blockers and add behavioral tests for parent visibility, interoperable pagination tokens, topological ordering, bundles, threaded receipts, and newly required endpoints.
Then review every server requirement and applicable clarification in the candidate release, run the relevant conformance suites in supported authentication configurations, and record that evidence here alongside the version-list change.
Do not equate a passing discovery test with protocol conformance.
