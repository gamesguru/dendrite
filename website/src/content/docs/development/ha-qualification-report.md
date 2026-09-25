---
title: HA qualification report
description: Measured recovery, durability and ownership results for active-passive high availability.
---

This report is the release evidence for [#352](https://codefloe.com/pat-s/zendrite/issues/352).
It records the configuration, the measured recovery times, the defects the qualification found, and what it could not cover.
Every number comes from the [HA qualification](/development/ha-qualification/) harness; rerun it to reproduce them.

## Configuration

| Setting | Value |
| --- | --- |
| Code | `main` at `29bd237c` with [#394](https://codefloe.com/pat-s/zendrite/pulls/394), [#395](https://codefloe.com/pat-s/zendrite/pulls/395), [#396](https://codefloe.com/pat-s/zendrite/pulls/396) and [#397](https://codefloe.com/pat-s/zendrite/pulls/397) |
| Election timings | the documented defaults: `lease_duration` 30s, `renew_deadline` 20s, `retry_period` 5s, `ownership_timeout` 45s |
| Recovery objective | 120 s from fault to successful client and federation requests (ADR 0001) |
| Candidates | 3, behind a router that checks the active-only endpoint every 500 ms, as the chart's HAProxy does |
| PostgreSQL | 17, primary with one synchronous standby, `synchronous_commit=on`, `track_commit_timestamp=on` |
| JetStream | 3 nodes, file storage, `sync_interval: always`, replicas 3 |
| Media | S3 API (gofakes3, directory-backed) |
| Kubernetes | kube-apiserver and etcd 1.37 from envtest, RBAC limited to Lease `get`, `create`, `update` |
| Federation | a standalone homeserver joined to the test room |
| Client workload | 5 messages/s with retried transaction IDs, long-poll `/sync` with existing tokens, to-device messages, read receipts, media upload and download, search, and 1 message/s from the remote server |
| Reference workload | 100 users and 1,000 rooms with three members each, seeded before the scenario (ADR 0001) |
| Host | one Linux container, amd64, 12 CPUs |

Run with:

```sh
HACHAOS_CONTAINER=1 HACHAOS_TIER=full build/scripts/ha-qualification.sh
HACHAOS_CONTAINER=1 HACHAOS_TIER=reference HACHAOS_SCENARIOS=leader-sigkill,postgres-primary-failover,leader-partitioned-from-database build/scripts/ha-qualification.sh
```

## Summary

Every scenario meets the safety contract and the recovery objective:

- No acknowledged write was lost in any scenario, and every one reached `/messages`, a `/sync` client that kept its token across takeover, and the remote server.
- The database audit found no overlap: in every scenario each generation's commits all precede the next generation's first commit.
- No retired generation published to the broker, wrote media or sent federation after the bound its freshness window allows.
- A resumed or partitioned former leader refused every direct write sent to it after its successor was promoted.
- The router never saw two active candidates, and no status sample showed two candidates ready.
- With the reference workload, a crashed leader is replaced and serving client and federation requests in 65.1 s.

One limitation remains open: a client retry that spans a takeover can create a duplicate event, as described under [findings](#findings).

## Recovery at the reference timings

Times run from fault injection to the first success after the first failure the fault caused, or to the first success when nothing failed.
Scenarios marked *suspends* stop serving by design while the fault lasts, so the objective does not apply to them.
Scenarios marked *no failed writes* delayed some requests but failed none.
Inbound federation depends on when the remote server retries; the peer here wakes its queue on inbound traffic at most once a minute, which bounds it to about 50 s.

| Scenario | Result | Client write | Sync | Federation in | Federation out | Generations |
| --- | --- | --- | --- | --- | --- | --- |
| `leader-sigkill` | pass | 32.2 s | 32.1 s | 50.4 s | 32.1 s | 2 |
| `leader-graceful-shutdown` | pass | 30.2 s | 30.1 s | 50.3 s | 29.2 s | 2 |
| `leader-paused-beyond-lease` | pass | 30.0 s | 30.5 s | 50.4 s | 30.0 s | 2 |
| `rolling-restart` | pass | 32.7 s | 33.1 s | 49.9 s | 32.7 s | 2 |
| `leader-partitioned-from-kubernetes` | pass | 35.6 s | 35.6 s | 50.3 s | 0.0 s | 2 |
| `leader-kubernetes-responses-lost` | pass | 36.1 s | 36.1 s | 50.3 s | 0.1 s | 2 |
| `leader-partitioned-from-database` | pass | 81.3 s | 81.2 s | 81.3 s | 81.3 s | 2 |
| `leader-partitioned-from-everything` | pass | 30.6 s | 30.6 s | 50.3 s | 30.6 s | 2 |
| `kubernetes-api-outage` | pass, suspends | 70.6 s | 70.8 s | 70.6 s | 0.1 s | 2 |
| `postgres-primary-failover` | pass | 3.2 s | n/a | 0.1 s | 3.2 s | 1 |
| `nats-stream-leader-loss` | pass, no failed writes | 10.6 s | n/a | 0.1 s | 6.0 s | 1 |
| `media-store-outage` | pass, no failed writes | 0.2 s | n/a | 0.1 s | 0.2 s | 1 |
| `fencing-cannot-be-proven` | pass, suspends | 121.9 s | 121.2 s | 121.1 s | 121.1 s | 3 |
| `leadership-lost-during-startup` | pass | 69.6 s | 69.6 s | 69.6 s | 69.6 s | 3 |
| `queued-federation-delivery` | pass | 29.2 s | 29.6 s | 40.3 s | 29.1 s | 2 |
| `coordinated-upgrade` | pass | 1.4 s | 1.5 s | 20.4 s | 1.4 s | 2 |

## Durability and idempotency

| Scenario | Acked writes | Lost | Stored, not in /messages | Duplicated | Duplicated across takeover | Retried | Unacked | Unacked but committed | Remote writes | To-device |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `leader-sigkill` | 193 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 75 | 21 |
| `leader-graceful-shutdown` | 188 | 0 | 0 | 0 | 1 | 1 | 0 | 0 | 74 | 22 |
| `leader-paused-beyond-lease` | 201 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 75 | 23 |
| `rolling-restart` | 191 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 75 | 22 |
| `leader-partitioned-from-kubernetes` | 277 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 74 | 31 |
| `leader-kubernetes-responses-lost` | 281 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 75 | 30 |
| `leader-partitioned-from-database` | 114 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 106 | 12 |
| `leader-partitioned-from-everything` | 180 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 75 | 22 |
| `kubernetes-api-outage` | 215 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 95 | 23 |
| `postgres-primary-failover` | 117 | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 28 | 12 |
| `nats-stream-leader-loss` | 158 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 44 | 22 |
| `media-store-outage` | 205 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 44 | 22 |
| `fencing-cannot-be-proven` | 106 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 144 | 13 |
| `leadership-lost-during-startup` | 112 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 94 | 12 |
| `queued-federation-delivery` | 204 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 75 | 23 |
| `coordinated-upgrade` | 315 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 74 | 37 |

## Ownership boundaries

Audited commits are transactions that wrote an application table, attributed to an activation.
Retired-generation publications arrived after the successor acquired ownership but within the freshness bound the design allows.

| Scenario | Audited commits | Broker publications | Retired-generation publications | Media writes | Federation requests |
| --- | --- | --- | --- | --- | --- |
| `leader-sigkill` | 5846 | 1225 | 0 | 2 | 209 |
| `leader-graceful-shutdown` | 5761 | 1206 | 0 | 2 | 205 |
| `leader-paused-beyond-lease` | 6020 | 1260 | 0 | 2 | 217 |
| `rolling-restart` | 5802 | 1219 | 0 | 2 | 207 |
| `leader-partitioned-from-kubernetes` | 7673 | 1578 | 0 | 2 | 299 |
| `leader-kubernetes-responses-lost` | 7766 | 1596 | 0 | 2 | 302 |
| `leader-partitioned-from-database` | 4609 | 1043 | 0 | 2 | 124 |
| `leader-partitioned-from-everything` | 5566 | 1176 | 2 | 2 | 196 |
| `kubernetes-api-outage` | 6650 | 1417 | 0 | 2 | 232 |
| `postgres-primary-failover` | 3467 | 691 | 0 | 2 | 129 |
| `nats-stream-leader-loss` | 4494 | 933 | 0 | 2 | 173 |
| `media-store-outage` | 5486 | 1117 | 0 | 2 | 220 |
| `fencing-cannot-be-proven` | 5143 | 1202 | 0 | 2 | 116 |
| `leadership-lost-during-startup` | 4369 | 975 | 0 | 2 | 122 |
| `queued-federation-delivery` | 5862 | 1272 | 0 | 2 | 148 |
| `coordinated-upgrade` | 8551 | 1744 | 0 | 2 | 341 |

Scenario notes:

- `leader-graceful-shutdown`: one logical transaction produced two events, because its first attempt reached the retired owner and its retry the successor, beyond the process-local transaction cache.
- `leader-paused-beyond-lease`: the resumed former leader refused all 25 direct writes sent to it.
- `leader-partitioned-from-kubernetes`: the former leader refused all 15 direct writes sent to it after its successor was promoted.
- `leader-partitioned-from-everything`: the former leader refused all 15 direct writes sent to it after the partition healed.
  Two of its publications arrived after its successor acquired ownership, within the 7 s freshness bound.
- `fencing-cannot-be-proven`: no candidate became active while the old owner's backend could not be terminated.
  Blocked candidates exit and restart, so none happened to be reporting the blocked boundary when the hold ended.

## Recovery with the reference workload

Seeding 1,000 rooms adds about 30 s to promotion, mostly rebuilding the search index and recovering sync state; the objective still holds.
The sync API also works through a larger backlog after a fault, so clients briefly receive limited timelines and fill them from `prev_batch`.

| Scenario | Result | Client write | Sync | Federation in | Federation out | Generations |
| --- | --- | --- | --- | --- | --- | --- |
| `leader-sigkill` | pass | 65.1 s | 65.1 s | 65.1 s | 65.1 s | 2 |
| `leader-partitioned-from-database` | pass | 111.0 s | 110.9 s | 111.0 s | 111.0 s | 2 |
| `postgres-primary-failover` | pass | 3.2 s | n/a | 0.1 s | 3.2 s | 1 |

| Scenario | Acked writes | Lost | Stored, not in /messages | Duplicated | Duplicated across takeover | Retried | Unacked | Unacked but committed | Remote writes | To-device |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `leader-sigkill` | 109 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 90 | 12 |
| `leader-partitioned-from-database` | 108 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 135 | 12 |
| `postgres-primary-failover` | 114 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 29 | 13 |

- `leader-sigkill`: syncing clients filled three pages of limited timelines from `prev_batch`.
- `leader-partitioned-from-database`: syncing clients filled one page of a limited timeline from `prev_batch`.
- `postgres-primary-failover`: syncing clients filled one page of a limited timeline from `prev_batch`.
  The receipt check had nothing to verify, because the reader's `/sync` was still catching up on the seeded backlog when the workload stopped.

## Findings

The qualification found these defects; each is fixed in its own pull request, and every result above includes the fixes.

| Defect | Effect | Fix |
| --- | --- | --- |
| Receipt, send-to-device and signing-key consumers refused messages they had handled | Every receipt and to-device message was sent to remote servers again every 30 s, and remote cross-signing keys were re-uploaded, forever | [#392](https://codefloe.com/pat-s/zendrite/pulls/392) |
| A JetStream publication whose broker died before acknowledging it waited on the caller's context | Client writes stalled for 30 s after losing the stream leader; an inbound federation transaction hung indefinitely and blocked the remote server's queue | [#393](https://codefloe.com/pat-s/zendrite/pulls/393) |
| A destination queue loaded its backlog unordered, after events queued since the restart | After a takeover, the remote server rejected all 53 queued events for missing `prev_events` | [#394](https://codefloe.com/pat-s/zendrite/pulls/394) |
| Backward pagination stepped over events that share a depth | `/messages` and `prev_batch` skipped acknowledged messages whenever a late remote event forked the room at the same depth | [#395](https://codefloe.com/pat-s/zendrite/pulls/395) |
| A Lease holder partitioned from PostgreSQL kept renewing the Lease | No standby could take over while the partition lasted; after the first fix, renewing through the drain still pushed takeover to 141.6 s with the reference workload | [#396](https://codefloe.com/pat-s/zendrite/pulls/396) |
| The sync and federation consumers panicked when a database write failed transiently | With the reference workload, a 3 s PostgreSQL failover crashed the leader and became a 64 s leadership change | [#397](https://codefloe.com/pat-s/zendrite/pulls/397) |

One limitation is documented rather than fixed:

- Transaction IDs are deduplicated in a process-local cache.
  If the old owner accepts a message into the room input stream but stops before answering, the successor applies it, and the client's retry with the same transaction ID creates a second event.
  This happened once in `leader-graceful-shutdown` and is counted as *duplicated across takeover* above; a durable fix is tracked in [#398](https://codefloe.com/pat-s/zendrite/issues/398).

Two observations need no code change but affect operation:

- The routing layer marks a backend down after one failed 500 ms check and closes its in-flight requests.
  A single slow probe under load did this once during setup; clients retried and succeeded, but a less aggressive `fall` would avoid it.
- When the Kubernetes API or fencing is unavailable, no candidate serves, by design.
  Recovery then starts when the dependency returns, and the measured times include the fault's duration.

## Limitations

The harness trades fidelity for reproducibility in a few places, so these still need exercising on real infrastructure:

- Every process shares one host and one clock, and partitions are emulated by proxies, not by packet loss between machines.
- The routing layer reimplements the chart's HAProxy check rather than running HAProxy.
- The S3 server is gofakes3, so S3 and shared-filesystem failover are not exercised.
- The PostgreSQL operator is emulated: the harness promotes the standby, moves the service address, and relaxes synchronous commit while no standby exists.
- Candidates restart one second after exiting, not with the kubelet's crash-loop backoff.
- Power loss, correlated storage failure and schema migrations between versions are out of scope; the upgrade scenario uses two builds of the same code.
- Appservice and push deliveries are not verified end to end.

## Acceptance criteria

| Criterion from #352 | Evidence |
| --- | --- |
| A resumed or partitioned old leader cannot perform stale authoritative work after successor promotion | Database audit, broker, media and federation attribution, and refused direct writes in the pause and partition scenarios |
| All documented failover scenarios meet the safety contract and measured recovery targets | 16 scenarios at the reference timings and 3 with the reference workload, all within 120 s or suspended by design |
| Acknowledged data and logical client transaction behavior meet the documented durability and idempotency guarantees | No lost acknowledged writes; the only duplicate is the documented process-local transaction cache limit, tracked in #398 |
| Recovery maintains sync, media, search and federation correctness | `/sync` with existing tokens, `/messages`, to-device, receipts, media, search and federation in both directions are verified after every scenario |
| Relevant Go race tests and standalone regressions pass | Every fix above adds regression tests and passes under `-race` against PostgreSQL and SQLite; full CI passes on each pull request |
| A release evidence report records configuration, observed recovery times, failures, and remaining limitations | This page |
