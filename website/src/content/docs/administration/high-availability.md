---
title: High availability
description: Configure active-passive high availability on Kubernetes.
---

High availability (HA) runs one active Zendrite instance while other instances wait as standbys.
If the active instance fails, a standby takes over after confirming that it is safe to start.
The current implementation is experimental and disabled by default.
Ownership is enforced at every protected boundary: the database, the message broker, media, outbound dispatch, and HTTP admission.
The failure envelope is qualified by the fault harness described in [HA qualification](/development/ha-qualification/), and the [qualification report](/development/ha-qualification-report/) records measured recovery times and the remaining limitations.
Qualify HA on your own infrastructure before relying on it in production.
The selected design protects shared resources from stale leaders without cloud-provider or hypervisor power-control integration.

## Configuration

Configure HA in the `global.ha` section:

```yaml
global:
  ha:
    enabled: false
    backend: kubernetes
    lease_name: matrix-example-org
    lease_namespace: matrix
    candidate_id: pod-uid
    lease_duration: 30s
    renew_deadline: 20s
    retry_period: 5s
    ownership_timeout: 45s
    status_address: :8009
    handover_grace: 2s
    handover_timeout: 15s
```

Enable HA only once the [prerequisites](#prerequisites) are in place.
All instances of the same homeserver must use the same Lease name and namespace.
Use a separate Lease for each homeserver and set `candidate_id` to each pod's UID.

The example above uses the default timing values.
If you adjust them, `lease_duration` must be a whole number of seconds and greater than `renew_deadline`, which must exceed 1.2 times `retry_period`.
Longer timings allow more time for Kubernetes API requests but can delay failover.

`ownership_timeout` bounds how long a candidate may spend proving that the previous owner can no longer act.
It must be at least as long as `lease_duration`, so a successor can outwait an expiring Lease.
A candidate that exhausts this budget stays unready and reports the boundary that blocked it; it never activates as a fallback.

### Choosing election timings

Election timings trade how quickly a crashed owner is replaced against how long a healthy owner survives a stalled Kubernetes control plane.
Start from one of these profiles:

| Setting | Default | Faster failover |
| --- | --- | --- |
| `lease_duration` | 30s | 20s |
| `renew_deadline` | 20s | 15s |
| `retry_period` | 5s | 2s |
| `ownership_timeout` | 45s | 30s |
| Crash detected within | 35 seconds | 22 seconds |

- **Default:** Use with any control plane, including etcd members spread across data centers and managed control planes whose stall behavior is unknown.
- **Faster failover:** Use with a control plane whose Lease write latency you monitor and have seen stay well under 15 seconds, including during etcd leader elections.

The Helm chart's `ha.leaseDuration`, `ha.renewDeadline`, `ha.retryPeriod` and `ha.ownershipTimeout` values set these fields, and its defaults are the default profile.
Timings are not part of the deployment identity, so a normal rolling update can change them.

Follow these rules when you deviate from a profile:

- `renew_deadline` is how long an owner tolerates a control plane that does not answer.
  An owner whose renewal does not complete within it gives up leadership and exits, even if the control plane recovers a moment later.
  An etcd leader election stalls every Lease write while it lasts, often for several seconds and longer when etcd members span data centers.
  Set `renew_deadline` at least 5 seconds above the longest Lease write latency you have observed, and never below 10 seconds.
  With `renew_deadline: 6s`, one production deployment lost its owner to a Lease write that took about 8 seconds during an etcd leader election; 15 seconds would have kept it.
- `lease_duration` is how long a crashed owner goes unnoticed.
  A standby takes over at most `lease_duration` plus `retry_period` after the owner's last renewal.
  Keep it at least 5 seconds above `renew_deadline`, so an owner that gives up has begun stepping down before a successor can acquire the Lease; fencing still refuses anything it has not stopped.
- `retry_period` spaces out renewals and acquisition attempts.
  Keep it at most a quarter of `renew_deadline`, so a renewal that fails fast, for example on a refused connection, is retried several times before the deadline.
  Shorter periods detect a released Lease sooner but add Lease writes on every candidate.
- `ownership_timeout` must be at least `lease_duration`; leave about 10 seconds more, so fencing a retired owner that is slow to disconnect does not exhaust it.

A graceful shutdown releases the Lease, so a standby takes over at its next `retry_period` whatever the Lease duration.
After takeover, the successor opens its listeners a few seconds after establishing ownership, and the chart's router adds up to its 500 ms health check interval.
Watch `zendrite_ha_renewal_failures_total`: renewal failures without a leadership loss mean the control plane is approaching `renew_deadline`.
Validate actual service recovery with the chosen timings, for example with the [HA qualification](/development/ha-qualification/) harness.

## Probes and status

Every candidate serves liveness, readiness, status, and metrics on `status_address`, separately from the Matrix listeners.
This listener exists because a standby never binds the Matrix listeners at all.
Without it a standby would look dead to Kubernetes and be restarted, and its metrics would not be scrapable until it happened to be promoted.

| Endpoint | Meaning |
| --- | --- |
| `/_zendrite/monitor/up` | Liveness. Succeeds for any running candidate, including a standby. |
| `/_zendrite/monitor/health` | Readiness. Succeeds only for the active homeserver, and answers 404 while it hands over. |
| `/_zendrite/monitor/status` | Phase, leadership, readiness, and the blocking reason as JSON. |
| `/metrics` | Prometheus metrics, when `global.metrics.enabled` is set. |

Point the liveness probe at `/_zendrite/monitor/up` and the readiness probe at `/_zendrite/monitor/health`, both on the `status_address` port rather than the Matrix port.
The same paths also exist on the Matrix listener, which a standby never binds, so probing them there makes every standby fail its probes and restart.
Using the readiness endpoint for liveness has the same effect, because a standby is intentionally unready.

A candidate reports one of `starting`, `standby`, `fencing`, `recovering`, `active`, `handing-over`, `draining`, or `stopped`.
An elected candidate stays unready through `fencing` and `recovering`, so a Service only routes to it once ownership is established and recovery is complete.
When promotion is blocked, `/_zendrite/monitor/status` names the boundary and the reason.

## Metrics and alerts

All HA metrics use fixed label sets.
Candidate identity and ownership generation are deliberately never labels, because they grow without bound across failovers.

| Metric | Meaning |
| --- | --- |
| `zendrite_ha_phase{phase}` | 1 for the current phase, 0 for the others. |
| `zendrite_ha_leadership_transitions_total{transition}` | Leadership acquisitions and losses. |
| `zendrite_ha_renewal_failures_total` | Lease renewal failures, which precede an election. |
| `zendrite_ha_fencing_failures_total{boundary}` | Ownership establishment failures per boundary. |
| `zendrite_ha_recovery_duration_seconds` | Acquisition to readiness for the current activation. |
| `zendrite_ha_shutdown_duration_seconds` | Ownership loss to the end of draining. |
| `zendrite_ha_handover_duration_seconds` | Start of a planned handover to the end of its wait for requests in flight. |
| `zendrite_ha_standby_prepared` | 1 while this standby reaches the database and may take over, 0 otherwise. |
| `zendrite_ha_build_compatible` | 0 when the deployment's upgrade contract does not admit this build. |
| `zendrite_ha_build_info{protocol,level,upgrade_from,rollback_to}` | This build's upgrade contract; always 1. |

Alert on no ready leader, repeated elections, stuck recovery, repeated fencing failures, and a candidate whose build the upgrade contract refuses.
The chart ships these rules; see [Helm setup](/installation/helm/helm/).

A degraded broker does not fail any probe, so these metrics, which every deployment exposes, are the way to see it:

| Metric | Meaning |
| --- | --- |
| `zendrite_jetstream_publish_retries_total{stream}` | Publication attempts lost to a broker failure and retried under the same message ID. |
| `zendrite_jetstream_publish_failures_total{stream,reason}` | Publications given up on: `canceled` or `deadline` when the caller stopped waiting, `unavailable` when no broker answered, `refused` when the broker or the ownership barrier rejected it. |
| `zendrite_jetstream_consumer_pending_messages{consumer}` | Messages waiting behind the last one delivered, also read from the broker every 30 seconds so that it keeps growing when a consumer stops fetching. |
| `zendrite_jetstream_consumer_last_ack_timestamp_seconds{consumer}` | When the consumer last acknowledged a message. |
| `zendrite_jetstream_consumer_fetch_errors_total{consumer}` | Failed pull requests, such as those during a leader election. |
| `zendrite_jetstream_stream_replica_current{stream,replica}` | 1 while the broker `replica` is current with the stream's leader, 0 while it is behind or offline, read from the broker every 30 seconds. |
| `zendrite_jetstream_stream_replica_lag{stream,replica}` | Entries the broker `replica` is behind the stream's leader, as the leader reports. |
| `zendrite_federationapi_recv_transactions_total{outcome}` | Inbound federation transactions, `ok` or `retry` when one was refused because an event or EDU could not be queued. |

Alert when a consumer has pending messages and has not acknowledged one for a few minutes, `zendrite_jetstream_consumer_pending_messages > 0 and time() - zendrite_jetstream_consumer_last_ack_timestamp_seconds > 300`, and when `zendrite_jetstream_publish_failures_total` or the `retry` outcome keeps increasing for more than a few minutes.
A short burst of retries and fetch errors during a broker restart or leader election is expected.
Also alert when a stream replica stays behind, `max by (stream, replica) (zendrite_jetstream_stream_replica_current) == 0` for 10 minutes: the stream still works, but losing one more broker would leave it without a quorum.
Every candidate exports the same reading, hence the `max`.
See [A broker replica that stops catching up](#a-broker-replica-that-stops-catching-up).

Note that `zendrite_ha_shutdown_duration_seconds` is set as the process drains, so a scrape may not catch it before the process exits.
Treat it as a best-effort signal and read the logs for the authoritative shutdown record.

## Ownership generation

Election selects a candidate, but it does not stop a suspended process from acting when it resumes.
Zendrite therefore records a durable ownership generation in PostgreSQL, in the `zendrite_ha_ownership` table, alongside an append-only `zendrite_ha_ownership_history` table.
Promotion advances the generation with a conditional update, so two candidates promoting at once serialize and a retired generation can never be reused.

The generation is stored outside the Lease on purpose.
Deleting or recreating the Lease does not reset ownership authority.
If the current record is missing while history rows remain, promotion fails closed rather than treating the database as a fresh installation.
Do not delete either table to clear a blocked promotion; that is an offline bootstrap decision, not a recovery step.

## Database fencing

Every database connection a candidate opens carries an `application_name` that identifies its activation, and is validated against the recorded generation as it is established.

When a candidate promotes, it advances the generation, then terminates every PostgreSQL backend belonging to an earlier activation of this homeserver and confirms that none remain.
A connection opened before the advance is terminated by that sweep.
A connection opened afterwards reads the new generation and refuses itself.
A retired owner therefore cannot commit, whether or not it has noticed that it lost the Lease.

This enforcement is PostgreSQL's rather than the application's, which is why it also covers a process that was suspended past its lease and resumed after takeover.
It depends on every candidate using the same PostgreSQL role, which HA already requires by rejecting per-component database targets.
A transaction that had already flushed its commit before termination is ordered before the promotion, and the successor has not activated at that point, so authoritative effects do not overlap.

## Ownership freshness

The database can reject a retired owner by itself, but the message broker cannot.
A candidate therefore treats its ownership as valid only while the database has confirmed it recently.

A background heartbeat re-reads the generation on its own guarded connection every `retry_period`, and ownership stays valid for `renew_deadline` after each confirmation.
A process that was suspended past its lease resumes with a stale confirmation, so it refuses to publish or dispatch before it has checked again, and that check is refused outright once a successor has taken over.
A candidate that can reach the broker but not the database also goes stale and stops acting, which is the intended fail-closed behavior.
If ownership stays unconfirmed for longer than `lease_duration` after it went stale, the candidate also gives up leadership and exits.
Otherwise a Lease holder partitioned from the database, but not from the Kubernetes API, would keep renewing the Lease and keep every standby out for as long as the partition lasted.
The Lease then expires and a candidate that can reach the database takes over, roughly `renew_deadline` plus twice `lease_duration` after the partition began.
A candidate only tries to take the Lease after connecting to the database within one `retry_period`, so the restarted former holder, still partitioned, cannot win it back and spend `ownership_timeout` failing to fence.
Renewals are not checked this way; a holder that loses the database gives the Lease up as described above.

## Broker fencing

Every JetStream publication passes an ownership check and is stamped with the publishing generation in the `Zendrite-HA-Generation` header.
The connection name carries the activation, which makes an overlapping owner visible in broker monitoring.

Publication is gated on the freshness window rather than on a broker-side check, so a retired owner can still publish for a short period after its successor activates.
Consumers count such messages in `zendrite_ha_retired_generation_messages_total` and log them, rather than discarding them.
They are not discarded because a suspended owner only publishes for writes it already committed, so dropping them would lose events that exist in the database.

Consumers stop rather than acknowledge once ownership is lost, before fetching and again after the callback returns.
Unconfirmed ownership is treated differently from retirement: a database blip makes a consumer wait and resume, because a worker that exits is never restarted and would leave a leader that serves traffic while consuming nothing.
The roomserver's room workers manage their own subscriptions and apply the same barrier.
Acknowledging work that a retired owner did not complete would discard it, so the message is left pending for the promoted activation to redeliver instead.
Messages are never discarded on the basis of their generation: a suspended owner can publish an event that genuinely committed before it was fenced, and dropping that message would lose it.

## Media, dispatch, and admission

Media backends have no ownership check of their own, so a retired owner is refused at the file store: uploads, publication, thumbnail replacement, and deletion all fail.
Reads stay open, because serving media that already exists is not an authoritative effect and a draining candidate should still answer in-flight downloads.

Remote servers cannot reject a retired owner either, so outbound federation dispatch is refused before a request starts its round-trip.
This covers every call that makes a remote server accept an effect on this homeserver's behalf, not only transaction sends: joins, leaves, invites, knocks, third-party invite exchange, and one-time key claims.
Key claims matter most, because a spent remote one-time key cannot be recovered.
A request already dispatched may still complete; that is the documented at-least-once behavior, and the successor recovers the queue using stable transaction identifiers.

A refusal is distinguishable from a remote failure, so the destination queue stops instead of recording it.
Charging it to the remote server would persist retry state, divert traffic to relays and eventually blacklist servers that were never unreachable, in a database the successor inherits.

Appservice and push deliveries begin inside a consumer callback, which the fetch and acknowledgement barriers do not cover, so each checks ownership immediately before its HTTP request.

Admission is evaluated per request rather than only at readiness.
A request already parked on an established keep-alive connection never passes a new readiness check, so waiting for Kubernetes to remove the endpoint is not sufficient.
A candidate that loses ownership rejects client, federation, and admin traffic immediately.

Probe and metrics endpoints are exempt.
Refusing them would blind monitoring and make a candidate look dead to every probe at exactly the moment an operator needs to see why it stopped admitting traffic.

A media operation refused for lost ownership returns 503 rather than 400, so a client retries instead of being told its upload was malformed.

## Kubernetes permissions

The pods' service account needs `get`, `create`, and `update` permissions on `leases` in the `coordination.k8s.io` API group within the configured namespace.

## Prerequisites

- Kubernetes 1.26 or later, for a stable `coordination.k8s.io/v1` Lease API and `PodDisruptionBudget` in `policy/v1`.
- An external HA PostgreSQL with synchronous commit, and an external replicated JetStream.
  The bundled single-node PostgreSQL and the embedded NATS server are rejected in HA mode.
- Shared media: S3, or an explicitly shared POSIX filesystem.
- The same signing key, server name, and configuration on every candidate, running builds that the deployment's [upgrade contract](#upgrade-contract) admits.
- Every candidate must connect as the same PostgreSQL role, which HA already enforces by rejecting per-component database targets.

See [HA storage and recovery](/installation/manual/high-availability/) for the full storage contract.

## Signing keys and bootstrap

Every candidate must present the same signing identity, or remote servers will reject events depending on which candidate served them.
Mount one signing key Secret into every pod; do not let each pod generate its own.
Public signing identities are part of the compatibility fingerprint recorded on the Lease, so a candidate with a different key fails closed rather than serving traffic under the wrong identity.
That is a hard stop, not a fallback: fix the Secret rather than working around the mismatch.

To bootstrap, start with `replicaCount: 1` and HA enabled, and confirm the pod reaches `active`.
The first candidate creates the annotated Lease and the ownership record.
Only then scale up.
Starting several candidates against an empty database at once is safe but harder to diagnose if a prerequisite is wrong.

## Rolling upgrades

A build that the deployment's upgrade contract admits can replace the running one while clients keep working.
This needs active-only routing that holds requests while no candidate is active and drains a candidate that answers 404, as the Helm chart's `ha.routing.enabled` does; see [Helm setup](/installation/helm/helm/).
Without such routing, every handover is a short outage like a [manual failover](#manual-failover).

### Upgrade contract

Every build declares an upgrade contract instead of pinning its exact executable:

| Field | Meaning |
| --- | --- |
| `protocol` | The ownership, fencing and compatibility record protocol. Builds of different protocols never share a deployment. |
| `level` | The build's shared-state surface: the PostgreSQL schema and migrations, and the JetStream streams and message formats. It rises with every change to that surface. |
| `upgrade_from` | The oldest level this build can take over from, including the messages such a build left queued. |
| `rollback_to` | The oldest level that may take over from state this build has written. |

Each candidate logs its contract at startup as `HA upgrade contract`, and exports it as `zendrite_ha_build_info`.
Candidates must still agree on the deployment's identity: signing keys, server name, database, broker and media targets and namespaces, search settings, and enabled MSCs.
Changing any of those remains an [offline upgrade](#offline-upgrades).

The deployment records, in `zendrite_ha_compatibility`, the level of the build that activated last and a floor.
The floor is the highest `rollback_to` of every build that has activated, because storage keeps every migration a build ran, so it only rises.
A candidate may take over when its protocol matches, its level is at or above the floor, and, if its level is above the recorded one, its `upgrade_from` is at or below the recorded level.

The contract is checked three times, and never by writing storage the running owner uses:

- A standby checks the record every `retry_period`.
  It is a healthy candidate on `/_zendrite/monitor/candidate` only while the record admits it and it reaches the database, so an incompatible image never becomes Ready and a rollout stalls on it while the old candidates keep serving.
- Before every attempt to take the Lease, the candidate checks again, so an incompatible build never retires the running owner, even when the owner has released the Lease.
- After fencing, activation checks once more under the migration lock and records its own level before any migration runs.

`/_zendrite/monitor/status` reports the contract, the deployment's level, floor and last activated version, and whether the standby is prepared:

```json
{"phase": "standby", "upgrade": {"version": "3.5.0", "protocol": 1, "level": 2, "upgrade_from": 1, "rollback_to": 1, "deployment_level": 1, "deployment_floor": 1, "deployment_version": "3.4.0", "prepared": true, "compatible": true}}
```

A refused standby logs `HA standby is not prepared to take over` with the reason, reports `compatible: false`, and sets `zendrite_ha_build_compatible` to 0.

### Supported upgrades

The upgrade matrix is recorded in `setup/ha/testdata/levels.json` in the source repository, one entry per level, and CI refuses a change to the shared-state surface that does not declare a new level.

| Level | Introduced | Upgrades from | Rolls back to |
| --- | --- | --- | --- |
| 1 | first release after 3.3.0 | 1 | 1 |

Builds of the same level are always interchangeable, so patch releases and rebuilds roll without restrictions.
A new level lists how far back it can upgrade from; unless its `upgrade_from` says so, only adjacent levels are supported, and skipping a level needs an intermediate rollout or an offline upgrade.
Earlier releases pinned their exact executable and have no contract, so reaching level 1 is one last offline upgrade.

These changes still need an offline upgrade:

- Anything in the deployment identity, including enabling or disabling an MSC, changing the topic prefix, or moving media.
- A level whose `upgrade_from` excludes the running level, for example a stream storage, retention or subject change, which HA refuses to apply to an existing stream.
- A rollback below the floor.

### Runbook

1. Read the new release's level, `upgrade_from` and `rollback_to` in its release notes or `levels.json`, and compare them with `deployment_level` and `deployment_floor` in the status of a running candidate.
2. Back up the database, as for any upgrade that may run migrations.
3. Roll out the new image with `strategy.type: RollingUpdate`, `maxSurge: 0` and `maxUnavailable: 1`.
   Each replacement pod must become Ready, which for a standby means prepared, before the next old pod is stopped.
4. Watch `kubectl rollout status`.
   A rollout that stalls on an unready new pod whose status reports `compatible: false` needs the offline procedure; undo the rollout with `kubectl rollout undo`, and the running candidates are untouched.
5. When the rollout stops the active candidate, it hands over and a prepared candidate takes over.
   The successor logs `HA deployment level changed` when it activates a new level.

### Planned handover

When a rollout or a pod deletion stops the active candidate, the candidate hands over instead of stopping at once:

1. `/_zendrite/monitor/health` answers 404, so routers send it no new requests but keep the ones in flight, and hold new requests until a candidate is active.
2. Long-polling `/sync` and sliding sync requests answer at once with the client's own position, and the client's next request is held for the successor.
3. It keeps admitting requests for `handover_grace`, which must cover the routers' health check interval, then waits for every request in flight, for at most `handover_timeout` in total.
4. It stops its consumers and background work, releases the Lease, and exits.
   A prepared standby takes the Lease at its next `retry_period`, fences, runs any migrations, and becomes active; the routers then release the held requests to it.

Clients see a pause of a few seconds, which is the grace period, the retry period and the successor's activation, rather than errors.
`ha.terminationGracePeriodSeconds` must cover `handover_timeout` plus the 30 second shutdown budget.
Set `handover_timeout: 0s` to stop at once instead.

Some state is process-local and does not survive a handover, as after any takeover:

- Search answers HTTP 503 with `Retry-After` until the successor has rebuilt its index, which takes time in proportion to the searchable history.
- Sliding-sync connections start over, typing notifications are dropped, and a login, SSO or user-interactive authentication flow in progress must be restarted.
- Outbound federation, appservice and push deliveries in flight when the owner stops are retried by the successor with their original transaction IDs.

A rollout under `maxSurge: 0` may stop the active candidate before the standbys, so it can hand over twice: once to an old standby and once to a new one.
Each handover is uninterrupted.

### Aborting and rolling back

Before the active candidate is replaced, undoing the rollout is always safe: no new build has written anything.
If the successor fails while it activates, its Lease expires and another prepared candidate takes over, as in an ordinary takeover; clients may see errors once the routers' hold runs out.

Once a new level has activated, its migrations have run.
Rolling back to the old image is a rolling upgrade in reverse and is supported only while the old level is at or above the deployment's floor, which is the new build's `rollback_to`.
Below the floor, old candidates are refused before they take the Lease, so a rollback leaves the new build serving; restoring the old version then needs the offline procedure and the database backup.

### Offline upgrades

When the contract does not admit the new build, upgrade in a maintenance window, as described in [HA storage and recovery](/installation/manual/high-availability/): scale to zero, confirm no pod is running, deliberately reset both compatibility records, then scale back up on the new version.

## Manual failover

Delete the pod that reports `active`.
It [hands over](#planned-handover) like the active candidate in a rolling upgrade: it drains its requests in flight, then releases the Lease, so a standby takes over at its next `retry_period` rather than after `lease_duration`.
A leader that does not drain within the shutdown budget, or that crashes, leaves the Lease to expire instead.
Do not delete the Lease to force a failover.
The Lease is coordination, not authority, and deleting it does not retire the ownership generation.

With routing that holds requests, as the chart's does, clients see a pause rather than errors, apart from the process-local exceptions listed under [Planned handover](#planned-handover).
Without it, expect a visible interruption.
Clients receive connection errors or `503` responses until the successor is ready, then reconnect and resume from their existing sync tokens.
After an unplanned takeover, long-lived `/sync` connections are dropped and must be re-established.
Typing notifications, login and SSO intermediates, and sliding-sync sessions are process-local, so clients may need to retry a request or start a fresh session.
Retrying a message with the same transaction ID is safe across a takeover: the roomserver remembers which event each transaction produced for 24 hours, so a retry is answered with the event the first attempt stored, even if the old owner stored it.

## Rolling back to a single instance

Scale to `replicaCount: 1` and wait until exactly one candidate reports `active`.
To leave HA entirely, scale to zero first, confirm no pod is running, then set `ha.enabled: false` and start one instance with the same identity and storage.
Do not set `ha.enabled: false` while several pods are running: without election nothing prevents two active homeservers.

## Recovering from blocked fencing

A candidate that cannot establish ownership refuses to activate and exits with a nonzero status, so Kubernetes restarts it and it contends again.
This is intended: it fails closed rather than risking two active homeservers.

Read the blocking reason from the candidate's logs.
`/_zendrite/monitor/status` reports the boundary only during the brief window before the process exits, and `zendrite_ha_fencing_failures_total` is likely to be missed by a scrape for the same reason, so neither is a reliable record of a single failure.
A candidate stuck in a restart loop is visible through `ZendriteHANoReadyLeader` and through the pod's restart count.

Look at the named boundary first.
A `postgresql` block usually means a backend of the previous activation is still connected, which the status reason reports as a count.
Find it with `SELECT pid, application_name, state FROM pg_stat_activity WHERE application_name LIKE 'zendrite:%'`, and confirm the old pod is really gone.
Do not delete the ownership tables to clear the block; that discards the evidence that makes takeover safe.

## Guarantees and limitations

Supported today:

- One active homeserver at a time, enforced at the database, broker, media, dispatch, and admission boundaries.
- Automatic takeover after a candidate loses its Lease, provided ownership can be established.
- Committed PostgreSQL writes and acknowledged JetStream publications survive takeover under the documented storage settings.
- [Rolling upgrades](#rolling-upgrades) between builds that the upgrade contract admits, and planned handovers, without failed client requests, apart from the documented process-local exceptions.

Not supported, and not planned for this iteration:

- Active-active. Only one candidate serves traffic; the others are cold.
- Zero-downtime failover after an unplanned failure. Takeover restarts the homeserver and rebuilds process-local state; routing that holds requests only hides it while the takeover is shorter than the hold.
- Rolling upgrades that the contract does not admit, and automatic schema downgrade.
- Exactly-once external delivery. Federation, appservice, and push retries can duplicate remote deliveries.
  See [Inbound federation delivery](#inbound-federation-delivery) for what an inbound transaction guarantees.
- Non-Kubernetes election backends.

The [qualification report](/development/ha-qualification-report/) measures recovery against the ADR reference workload.
A successor opens its listeners without waiting for its search index, so the size of the search history does not delay takeover.
Search itself answers HTTP 503 until the rebuild finishes, which takes time in proportion to the searchable history: about 32 seconds for 84,000 messages on one production candidate.
Each activation logs `Activation phase finished` with the duration of every startup phase, and `Search index rebuilt` with the rebuild's duration.

## Failover behavior

Standbys do not serve traffic until takeover and startup are complete.
An instance that loses leadership stops accepting requests, shuts down, and exits.
If safe takeover cannot be confirmed, the standby does not start serving traffic.
Check the application logs for election and startup failures.

### Candidate health and active-only routing

`/_zendrite/monitor/candidate` returns HTTP 200 for a prepared standby or an active candidate.
A standby is prepared once it has checked that it reaches the database and that the deployment's upgrade contract admits its build.
Starting, fencing, recovering, draining, stopped, and failed candidates return HTTP 503.
`/_zendrite/monitor/health` remains the active-only traffic check.
Use the candidate endpoint for Kubernetes readiness only with a separate routing layer that checks the active-only endpoint before forwarding Matrix traffic.
The Helm chart supports this with `ha.routing.enabled`, using replicated HAProxy routers and DNS discovery of all candidate status endpoints.
This makes healthy standbys Ready without adding them directly to Matrix traffic or granting applications permission to modify routing resources.

Failover includes Lease expiry, election retry jitter, fencing, recovery, and routing convergence.
Shorter Lease and retry periods can reduce detection time but make leadership more sensitive to control-plane stalls and increase renewal traffic; see [Choosing election timings](#choosing-election-timings).
Reducing election timings never skips resource fencing or backlog recovery.

## Suspended owners and broker delivery

HA pull requests expire after one second, and unacknowledged durable deliveries become eligible for redelivery after five seconds.
A healthy long-running handler extends its delivery once per second while its ownership remains valid.
This lets a successor recover work held by a suspended process without waiting for that process to resume or deleting durable consumers.
Workers recheck ownership after fetching, and retired room workers must not acknowledge or answer a successor's synchronous request.
Non-HA delivery timing remains unchanged.

### Recovery while a broker is unavailable

Configure each broker address separately so the client can try another broker when an initial connection fails.
A single headless service address can resolve to an unresponsive broker before the client has discovered its peers.
Existing replicated streams need an elected leader and a current majority for activation; creation or replica-count changes still wait for every configured replica.
No recovery path reduces the configured replica count or deletes queued data.
Active traffic and candidate probes consult fencing validity as well as lifecycle state, so a resumed retired owner cannot re-enter routing while lease renewal is still timing out.

A broker whose host stops responding keeps its connections open, so nothing tells the client it is gone.
Zendrite pings its broker every 10 seconds and reconnects to another one after two unanswered pings, so a silent broker costs about 30 seconds of failed publications rather than the four to six minutes of the NATS client defaults.
No takeover is needed: the owner keeps its ownership and resumes once it reaches a broker that has a current majority.
A broker that rejoins, on its old storage or on an empty volume, catches up from its peers; check that every stream and consumer replica is current before taking another broker down.

### A broker replica that stops catching up

nats-server 2.15 can stop applying a stream on a broker that rejoined on its retained storage, for example after its node was NotReady and the pod was replaced ([#473](https://codefloe.com/pat-s/zendrite/issues/473)).
The stream keeps its leader and a current majority, so Zendrite keeps working and nothing is lost, but losing one more broker would leave the stream without a quorum.
The broker does not recover on its own.

To recognize it:

- `zendrite_jetstream_stream_replica_current` stays 0 for one broker and one or more streams while that broker is running.
- Zendrite logs `JetStream stream replica is not catching up with its leader` with the stream and the broker after five minutes, and `JetStream stream replica caught up with its leader` once it recovers.
- The broker logs `critical write error: expected sequence does not match store` for the stream, and its full `/healthz` on the monitoring port reports a `stream write error`.

To recover, restart that broker, for example by deleting its pod, and let it start on the same volume.
The restarted broker reloads the stream from its storage and catches up from the leader within seconds; check that `zendrite_jetstream_stream_replica_current` returns to 1 for every stream.
Restart one broker at a time, and only while the other brokers are current.
Wiping its volume is not needed, and `nats stream cluster peer-remove` does not help: in a three-broker cluster there is no spare broker to move the replica to.

### Inbound federation delivery

A remote server is told a transaction succeeded only once its contents are queued in the broker.
The guarantees depend on what the transaction carries:

- **Events.** An event that fails its checks is reported in the response, as the specification requires.
  An event that could not be queued fails the whole transaction with HTTP 503, so the sender retries it; the roomserver ignores events it already has.
- **To-device messages, device list updates and signing key updates.** One that could not be queued fails the transaction with HTTP 503 as well.
  Retried to-device messages are deduplicated by the sender's `message_id` for two minutes, the broker's deduplication window; a retry later than that can deliver a message twice, which clients already tolerate.
- **Read receipts.** Receipts are also retried this way, but they coalesce: a newer receipt from the same user replaces an older one, so only the newest is guaranteed to become visible.
- **Typing notifications and presence.** These are best effort and expire within seconds, so one that could not be queued is dropped rather than making the sender resend the transaction.

An accepted transaction is processed to completion for up to two minutes, even if the sender disconnects, so a sender that times out and retries the same transaction receives the outcome of the attempt still running rather than a partial result.
A retry joins a running attempt only when it carries the same events and EDUs; a retry that reuses the transaction ID with more content, as older Zendrite and Dendrite senders do, is processed on its own.
Zendrite itself now sends a retry under a new transaction ID whenever its content changed, since a destination may answer a known ID with its earlier outcome.
Each refused transaction is logged as `Refusing federation transaction for a retry` and counted in `zendrite_federationapi_recv_transactions_total{outcome="retry"}`.
