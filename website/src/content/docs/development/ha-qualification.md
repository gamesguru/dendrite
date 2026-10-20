---
title: HA qualification
description: Run the fault scenarios that qualify active-passive high availability.
---

The HA qualification harness in `test/hachaos` injects faults into a complete HA deployment and checks the safety contract described in [High availability](/administration/high-availability/).
It is the release gate for HA, tracked in [#352](https://codefloe.com/pat-s/zendrite/issues/352).

## What runs

Every scenario gets a fresh deployment on one host, built from real processes:

- A PostgreSQL primary with a synchronous standby behind a stable address, so promotion looks to candidates the way an operator's read-write Service does.
- A three-node JetStream cluster with file storage and `sync_interval: always`.
- An S3 server for media.
- etcd and a kube-apiserver with RBAC that grants candidates only `get`, `create` and `update` on Leases.
- Three zendrite candidates with HA enabled, supervised so that a process that exits is restarted, as the kubelet restarts a container.
- A router that checks the active-only endpoint every 500 ms and cuts in-flight requests when a backend fails, like the chart's HAProxy routing layer.
  A backend that answers 404 gets no new requests but keeps the ones in flight, and a request that arrives while no backend is up is held for up to 30 seconds, as the chart's router does.
- A standalone homeserver joined to the test room, so federation runs in both directions.

Candidates reach every dependency through fault proxies.
A proxy attributes each connection to the candidate that opened it, so a partition can apply to one candidate while the others keep working, and it can drop traffic in one direction only.
Federation requests pass through a gateway that terminates TLS, which lets the harness record every request with the process that sent it.

## Workload

While a fault is injected, clients keep working the way a careful client does:

- A writer sends messages and retries each one with its original transaction ID until it is acknowledged.
- A syncing client long-polls with its existing token across takeover.
- To-device messages, read receipts, media uploads and downloads, and search run alongside.
- A user on the remote server writes to the same room, and syncs to receive local messages over federation.
  The remote user also sends to-device messages and read receipts to the local user, which reach the HA server only as EDUs in federation transactions.
- Every server runs with the HA-qualified MSCs `msc2444`, `msc2753` and `msc2836` enabled; `HACHAOS_MSCS=none` turns them off.
  An MSC probe sends thread replies into world-readable room version 11 and 12 rooms, which a local user peeks into via `/sync` and a remote user peeks into over federation.
  The local user also peeks into a room on the remote server, in which the remote user writes.

## What is checked

After the fault heals, the harness compares what clients were told with what the servers hold:

- Every acknowledged write is in the room exactly once.
  A request that failed without an answer may have committed, which is allowed, but it must not produce two events.
- Every acknowledged write reaches the syncing client with the token it held before the fault, and reaches the remote server.
- Every acknowledged thread reply is returned by `/event_relationships`, and reaches the local and remote peekers with the tokens they held before the fault.
  Every acknowledged write in the remote room reaches the local peeker.
- Every acknowledged to-device message is delivered, the newest acknowledged receipt (or one still in flight when the workload stopped) is visible, media uploaded before the fault is intact, and search finds messages from before and after it.
- The same holds for the remote user's to-device messages and receipts, which arrive over federation.
  Receipts coalesce, so only the newest acknowledged one must be visible; to-device messages delivered twice are counted and reported, since delivery is at least once.
- Every stream and durable consumer has a leader at the end, and a scenario that replaced a broker reports how long its replicas took to catch up.
  A follower still behind while its stream keeps a leader and a current majority is noted with the broker's store errors rather than failed: nats-server can leave one behind after a broker rejoins on its old storage ([#473](https://codefloe.com/pat-s/zendrite/issues/473)), which Zendrite cannot repair.
  When that broker logged the store error, the harness then restarts it on its storage, as the [runbook](/administration/high-availability/#a-broker-replica-that-stops-catching-up) prescribes; the scenario fails if its replicas do not catch up then.

Federation transactions from the remote server are recorded by the gateway with what they carried and what the sender was told.
The report counts transaction attempts, distinct transactions, the transactions and distinct to-device messages and receipts affected by a failed attempt, and the candidates' logged publication failures with the distinct items they concerned, so a burst of retries of one transaction does not read as many lost items.
When an item is missing, `missing-federated-*.txt` lists every attempt that carried it.
The harness also samples the publish, consumer and transaction metrics of the serving candidate and reports how much they moved.

It also checks ownership at every protected boundary:

- **Database.** A trigger records the activation marker of every transaction that writes an application table, and commit timestamps order them.
  The commits of one generation must all precede the first commit of any later generation.
- **Broker, media and federation.** Publications, S3 writes and outbound federation requests are attributed to a generation.
  A retired generation may act only until it can notice its successor, within one heartbeat interval.
- **Routing.** No two candidates may pass the active-only check at the same time.
- **Resumed owners.** A candidate resumed after a successor took over must refuse writes sent to it directly.

Recovery time is measured from fault injection to the first successful client write, sync, inbound federation request and outbound federation request, and compared with the 120 second objective.

Planned operations, `rolling-restart` and `rolling-upgrade`, are held to a stricter claim than recovery: every client request and every inbound federation request that overlapped the operation must succeed, and a request that only succeeded when retried counts as a failure.
The report lists them separately, with the maximum and 99th percentile write latency, the outbound federation requests the successor had to retry, and how long search answered 503 while the successor rebuilt its index, which is the documented exception.

## Scenarios

| Scenario | Fault |
| --- | --- |
| `leader-sigkill` | The active candidate is killed. |
| `leader-graceful-shutdown` | The active candidate receives SIGTERM. |
| `leader-paused-beyond-lease` | The active candidate is frozen until a successor serves, then resumed. |
| `rolling-restart` | Every candidate restarts in turn, the active one last, and no request may fail. |
| `leader-partitioned-from-kubernetes` | The active candidate loses only the Kubernetes API. |
| `leader-kubernetes-responses-lost` | Lease renewals arrive but their responses are lost. |
| `leader-partitioned-from-database` | The active candidate loses only PostgreSQL. |
| `leader-partitioned-from-everything` | The active candidate is isolated from every dependency. |
| `kubernetes-api-outage` | The kube-apiserver is frozen for longer than the Lease. |
| `postgres-primary-failover` | The PostgreSQL primary is killed and its standby promoted. |
| `nats-stream-leader-loss` | The broker leading the room input stream is killed. |
| `media-store-outage` | The S3 server is frozen. |
| `fencing-cannot-be-proven` | A backend of the old owner cannot be terminated, so promotion must stay blocked. |
| `leadership-lost-during-startup` | The successor loses the Kubernetes API while fencing or recovering. |
| `queued-federation-delivery` | Federation is cut, events queue, and the active candidate is killed. |
| `coordinated-upgrade` | A build the upgrade contract does not admit must not become prepared or take the released Lease; the maintenance-window upgrade must keep every write. |
| `rolling-upgrade` | Every candidate is replaced by the next level, the active one last, and no request may fail. |
| `rolling-upgrade-successor-fails` | The candidate taking over during a rolling upgrade is killed while it activates. |
| `rolling-upgrade-broker-leader-lost` | The broker leading the room input stream is killed while the owner hands over during a rolling upgrade. |
| `msc-relationship-index-failure` | PostgreSQL rejects MSC2836 relationship index writes, and the active candidate is replaced before they are allowed again. |
| `nats-broker-host-frozen` | The broker the active candidate uses stops responding without closing connections, as on a host that goes NotReady, and is replaced on its storage after an extended absence. |
| `nats-broker-replaced-empty` | The receipt stream leader is killed and replaced, after an extended absence, by a broker with empty storage that must catch up. |
| `nats-publish-acks-lost` | Broker traffic towards the active candidate is held for 15 seconds, so every publication is ambiguous until it is retried. |

The broker scenarios keep each broker away for 90 seconds in the `ci` tier and four minutes otherwise.
During the first two, the remote server abandons a transaction after 20 seconds, as a sender's request timeout does, which cancels the request while the HA server is still publishing.
No takeover is needed when one broker of three is lost, so they are held to a 60 second recovery objective.

## Running it

The harness writes client-go's in-cluster service-account files, which needs root, so it runs in a disposable container:

```sh
# the bounded tier with short election timings, as CI runs it
HACHAOS_CONTAINER=1 build/scripts/ha-qualification.sh
# every scenario with the reference election timings
HACHAOS_CONTAINER=1 HACHAOS_TIER=full build/scripts/ha-qualification.sh
# the reference timings with the ADR reference workload seeded
HACHAOS_CONTAINER=1 HACHAOS_TIER=reference build/scripts/ha-qualification.sh
# one scenario
HACHAOS_CONTAINER=1 HACHAOS_SCENARIOS=leader-sigkill build/scripts/ha-qualification.sh
# one scenario after seeding 40,000 searchable messages, so the successor rebuilds a large search index
HACHAOS_CONTAINER=1 HACHAOS_SCENARIOS=leader-sigkill HACHAOS_SEARCH_HISTORY=40000 build/scripts/ha-qualification.sh
```

Each run writes `report.md`, `results.json` and, per scenario, every process log, a timeline of faults and phase changes, and the client ledger to `.hachaos/artifacts`.
The report counts the HTTP 503 responses the client workload received between the fault and the end of recovery.

CI runs the `ci` tier on changes to HA code, split into five parallel groups, in `.crow/ha-qualification.yaml`.
A shared build step downloads the control-plane tools and compiles Zendrite, the upgrade binaries and the test harness once; the groups then run in separate containers against those binaries.
The upgrade binaries are the same source with another version string and HA upgrade contract: `zendrite-next` is the adjacent level, which a rolling upgrade admits, and `zendrite-unadmitted` is a level that needs the offline upgrade.
Their contracts are set at link time through `setup/ha.contractOverride`, which configuration cannot change.
`HACHAOS_PHASE=build` prepares the shared tools, and `HACHAOS_PHASE=run` runs them without compiling or downloading Go modules.
The default `all` phase performs both steps for local runs.
The script logs tool preparation and build durations, and the test harness reports each scenario duration.
Manual runs and the `ha-qualification` cron can select the `full` or `reference` tier with `HACHAOS_TIER` and publish their artifacts to the package registry.

## Limitations

The harness trades fidelity for reproducibility in a few places:

- Every process shares one host and one clock, and partitions are emulated by proxies.
- The routing layer reimplements the chart's HAProxy check and hold rather than running HAProxy.
- Both upgrade builds share the release's schema, so a rolling upgrade exercises the handover, the upgrade contract and the successor's recovery, but no migration; an upgrade between real releases should also be tried on a staging deployment.
- The S3 server is gofakes3, so S3 failover itself is not exercised.
- The PostgreSQL operator is emulated: the harness promotes the standby and relaxes synchronous commit while no standby exists.
- Power loss and correlated storage failure are out of scope for a process-level harness.
