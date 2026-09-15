---
title: High availability
description: Configure active-passive high availability on Kubernetes.
---

High availability (HA) runs one active Zendrite instance while other instances wait as standbys.
If the active instance fails, a standby takes over after confirming that it is safe to start.
The current implementation provides experimental Kubernetes Lease election and is disabled by default.
Automatic failover is not yet supported: production promotion remains blocked until resource-enforced ownership covers every protected boundary and is verified.
Ownership is enforced at the PostgreSQL boundary today; the JetStream, media, and outbound dispatch boundaries are still unimplemented, and a candidate refuses to activate while any of them is missing.
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

Keep `enabled` set to `false` until the resource-enforced ownership work in [#349](https://codefloe.com/pat-s/zendrite/issues/349) is complete.
Setting it to `true` starts election but does not authorize application startup.
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

## Kubernetes permissions

The pods' service account needs `get`, `create`, and `update` permissions on `leases` in the `coordination.k8s.io` API group within the configured namespace.

## Failover behavior

Standbys do not serve traffic until takeover and startup are complete.
An instance that loses leadership stops accepting requests, shuts down, and exits.
If safe takeover cannot be confirmed, the standby does not start serving traffic.
Check the application logs for election and startup failures.
