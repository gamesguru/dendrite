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

## Failover behavior

Standbys do not serve traffic until takeover and startup are complete.
An instance that loses leadership stops accepting requests, shuts down, and exits.
If safe takeover cannot be confirmed, the standby does not start serving traffic.
Check the application logs for election and startup failures.
