---
title: High availability
description: Configure active-passive high availability on Kubernetes.
---

High availability (HA) runs one active Zendrite instance while other instances wait as standbys.
If the active instance fails, a standby takes over after confirming that it is safe to start.
The current implementation provides experimental Kubernetes Lease election and is disabled by default.
Automatic failover is not yet supported: production promotion remains blocked until resource-enforced ownership is implemented and verified.
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
```

Keep `enabled` set to `false` until the resource-enforced ownership work in [#349](https://codefloe.com/pat-s/zendrite/issues/349) is complete.
Setting it to `true` starts election but does not authorize application startup.
All instances of the same homeserver must use the same Lease name and namespace.
Use a separate Lease for each homeserver and set `candidate_id` to each pod's UID.

The example above uses the default timing values.
If you adjust them, `lease_duration` must be a whole number of seconds and greater than `renew_deadline`, which must exceed 1.2 times `retry_period`.
Longer timings allow more time for Kubernetes API requests but can delay failover.

## Kubernetes permissions

The pods' service account needs `get`, `create`, and `update` permissions on `leases` in the `coordination.k8s.io` API group within the configured namespace.

## Failover behavior

Standbys do not serve traffic until takeover and startup are complete.
An instance that loses leadership stops accepting requests, shuts down, and exits.
If safe takeover cannot be confirmed, the standby does not start serving traffic.
Check the application logs for election and startup failures.
