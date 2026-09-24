---
title: High availability
description: Configure active-passive high availability on Kubernetes.
---

High availability (HA) runs one active Zendrite instance while other instances wait as standbys.
If the active instance fails, a standby takes over after confirming that it is safe to start.
The current implementation is experimental and disabled by default.
Ownership is now enforced at every protected boundary: the database, the message broker, media, outbound dispatch, and HTTP admission.
Do not enable it in production yet.
The failure envelope has not been qualified, which is tracked in [#352](https://codefloe.com/pat-s/zendrite/issues/352), and the deployment integration in [#351](https://codefloe.com/pat-s/zendrite/issues/351) is still outstanding.
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
```

Keep `enabled` set to `false` until the failure envelope is qualified in [#352](https://codefloe.com/pat-s/zendrite/issues/352).
All instances of the same homeserver must use the same Lease name and namespace.
Use a separate Lease for each homeserver and set `candidate_id` to each pod's UID.

The example above uses the default timing values.
If you adjust them, `lease_duration` must be a whole number of seconds and greater than `renew_deadline`, which must exceed 1.2 times `retry_period`.
Longer timings allow more time for Kubernetes API requests but can delay failover.

`ownership_timeout` bounds how long a candidate may spend proving that the previous owner can no longer act.
It must be at least as long as `lease_duration`, so a successor can outwait an expiring Lease.
A candidate that exhausts this budget stays unready and reports the boundary that blocked it; it never activates as a fallback.

## Probes and status

Every candidate serves liveness, readiness, status, and metrics on `status_address`, separately from the Matrix listeners.
This listener exists because a standby never binds the Matrix listeners at all.
Without it a standby would look dead to Kubernetes and be restarted, and its metrics would not be scrapable until it happened to be promoted.

| Endpoint | Meaning |
| --- | --- |
| `/_zendrite/monitor/up` | Liveness. Succeeds for any running candidate, including a standby. |
| `/_zendrite/monitor/health` | Readiness. Succeeds only for the active homeserver. |
| `/_zendrite/monitor/status` | Phase, leadership, readiness, and the blocking reason as JSON. |
| `/metrics` | Prometheus metrics, when `global.metrics.enabled` is set. |

Point the liveness probe at `/_zendrite/monitor/up` and the readiness probe at `/_zendrite/monitor/health`, both on the `status_address` port rather than the Matrix port.
The same paths also exist on the Matrix listener, which a standby never binds, so probing them there makes every standby fail its probes and restart.
Using the readiness endpoint for liveness has the same effect, because a standby is intentionally unready.

A candidate reports one of `starting`, `standby`, `fencing`, `recovering`, `active`, `draining`, or `stopped`.
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

Alert on no ready leader, repeated elections, stuck recovery, and repeated fencing failures.
The chart ships these rules; see [Helm setup](/installation/helm/helm/).

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
- The same signing key, server name, and executable on every candidate.
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

## Upgrades

Rolling mixed-version upgrades are not supported: candidates pin themselves to one executable and configuration fingerprint.
A new-version pod fails the Lease compatibility check, so replacing pods one at a time deadlocks. Once the last old pod is gone, nothing can acquire the Lease without operator intervention.

Upgrade in a maintenance window instead, as described in [HA storage and recovery](/installation/manual/high-availability/): scale to zero, confirm no pod is running, deliberately reset both compatibility records, then scale back up on the new version.
`maxSurge: 0` keeps a new candidate from being started alongside an old owner, but it does not make a rolling upgrade work.

## Manual failover

Delete the pod that reports `active`.
The remaining candidates contend for the Lease, and one of them establishes ownership and recovers.
Do not delete the Lease to force a failover.
The Lease is coordination, not authority, and deleting it does not retire the ownership generation.

Expect a visible interruption.
Clients receive connection errors or `503` responses until the successor is ready, then reconnect and resume from their existing sync tokens.
Long-lived `/sync` connections are dropped and must be re-established.
Typing notifications, HTTP transaction caches, login and SSO intermediates, and sliding-sync sessions are process-local, so clients may need to retry a request or start a fresh session.

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

Not supported, and not planned for this iteration:

- Active-active. Only one candidate serves traffic; the others are cold.
- Zero-downtime failover. Takeover restarts the homeserver and rebuilds process-local state.
- Rolling mixed-version upgrades and automatic schema downgrade.
- Exactly-once external delivery. Federation, appservice, and push retries can duplicate remote deliveries.
- Non-Kubernetes election backends.

The failure envelope has not been qualified against a production workload; that work is [#352](https://codefloe.com/pat-s/zendrite/issues/352).
Recovery time has not been measured for large deployments, and a full search index rebuild can exceed the proposed 120 second takeover target.

## Failover behavior

Standbys do not serve traffic until takeover and startup are complete.
An instance that loses leadership stops accepting requests, shuts down, and exits.
If safe takeover cannot be confirmed, the standby does not start serving traffic.
Check the application logs for election and startup failures.

### Candidate health and active-only routing

`/_zendrite/monitor/candidate` returns HTTP 200 for a healthy standby or active candidate.
Starting, fencing, recovering, draining, stopped, and failed candidates return HTTP 503.
`/_zendrite/monitor/health` remains the active-only traffic check.
Use the candidate endpoint for Kubernetes readiness only with a separate routing layer that checks the active-only endpoint before forwarding Matrix traffic.
The Helm chart supports this with `ha.routing.enabled`, using replicated HAProxy routers and DNS discovery of all candidate status endpoints.
This makes healthy standbys Ready without adding them directly to Matrix traffic or granting applications permission to modify routing resources.

Failover includes Lease expiry, election retry jitter, fencing, recovery, and routing convergence.
Shorter Lease and retry periods can reduce detection time but make leadership more sensitive to control-plane stalls and increase renewal traffic.
Keep `lease_duration > renew_deadline > 1.2 * retry_period`, and validate actual service recovery with the intended timings.
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
