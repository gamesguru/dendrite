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

It also checks ownership at every protected boundary:

- **Database.** A trigger records the activation marker of every transaction that writes an application table, and commit timestamps order them.
  The commits of one generation must all precede the first commit of any later generation.
- **Broker, media and federation.** Publications, S3 writes and outbound federation requests are attributed to a generation.
  A retired generation may act only until it can notice its successor, within one heartbeat interval.
- **Routing.** No two candidates may pass the active-only check at the same time.
- **Resumed owners.** A candidate resumed after a successor took over must refuse writes sent to it directly.

Recovery time is measured from fault injection to the first successful client write, sync, inbound federation request and outbound federation request, and compared with the 120 second objective.

## Scenarios

| Scenario | Fault |
| --- | --- |
| `leader-sigkill` | The active candidate is killed. |
| `leader-graceful-shutdown` | The active candidate receives SIGTERM. |
| `leader-paused-beyond-lease` | The active candidate is frozen until a successor serves, then resumed. |
| `rolling-restart` | Every candidate restarts in turn, the active one last. |
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
| `coordinated-upgrade` | A different build must not join; the maintenance-window upgrade must keep every write. |
| `msc-relationship-index-failure` | PostgreSQL rejects MSC2836 relationship index writes, and the active candidate is replaced before they are allowed again. |

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
```

Each run writes `report.md`, `results.json` and, per scenario, every process log, a timeline of faults and phase changes, and the client ledger to `.hachaos/artifacts`.

CI runs the `ci` tier on changes to HA code, split into four parallel groups, in `.crow/ha-qualification.yaml`.
Manual runs and the `ha-qualification` cron can select the `full` or `reference` tier with `HACHAOS_TIER` and publish their artifacts to the package registry.

## Limitations

The harness trades fidelity for reproducibility in a few places:

- Every process shares one host and one clock, and partitions are emulated by proxies.
- The routing layer reimplements the chart's HAProxy check rather than running HAProxy.
- The S3 server is gofakes3, so S3 failover itself is not exercised.
- The PostgreSQL operator is emulated: the harness promotes the standby and relaxes synchronous commit while no standby exists.
- Power loss and correlated storage failure are out of scope for a process-level harness.
