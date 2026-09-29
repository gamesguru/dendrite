---
title: HA storage and recovery
description: Storage prerequisites, recovery readiness, and coordinated upgrades for active-passive candidates.
---

HA remains opt-in and experimental.
Kubernetes Lease ownership and PostgreSQL advisory locks do not fence a suspended former owner.
Resource-enforced ownership now covers the database, broker, media, dispatch, and admission boundaries, described in [High availability](/administration/high-availability/).
The failure envelope is qualified by the [HA qualification](/development/ha-qualification/) harness, and the [qualification report](/development/ha-qualification-report/) records measured recovery times and the remaining limitations.
These checks add storage and recovery prerequisites to that qualification.

## Supported storage

- Configure one global PostgreSQL database and leave component connection strings empty or identical to the global connection string.
  Separate component databases and SQLite are not supported in HA mode.
- Enable only the experimental MSCs qualified for HA: `msc2444`, `msc2753`, and `msc2836`.
  Startup refuses any other MSC, including `msc3814` and `msc3861`.
  With `msc2836`, leave `mscs.database.connection_string` empty or identical to the global connection string, so the relationship index is fenced like every other table.
- Configure external JetStream addresses, a nonempty homeserver-specific topic prefix, and `global.jetstream.replicas: 3` or `5`.
  HA uses file storage for every stream, including typing and presence streams.
  Startup waits up to 30 seconds per stream for every configured replica to become current and online.
  It does not require a zero-lag snapshot while publishers are active.
- Use S3 media with the same endpoint, region, bucket, and prefix on every candidate, or explicitly set `media_api.storage.local.shared: true`.
  The shared-filesystem option asserts that every candidate mounts the same durable POSIX filesystem at the configured path.
  It requires synchronous client writes and synchronous server-side persistence, atomic rename, and coherent reads across clients, for example a synchronously mounted NFS export backed by redundant stable storage.
  An ordinary local volume, asynchronously written network mount, per-candidate PVC, or merely identical path strings do not satisfy this contract.
  The application cannot verify filesystem mount or storage-controller durability settings.

PostgreSQL must use synchronous commit and synchronously replicated storage with a failover policy that promotes only a replica containing acknowledged transactions.
Set `connect_timeout` in the connection string, for example `postgres://zendrite@db-primary/zendrite?connect_timeout=5`.
When the primary pod is deleted, connections to its address get no reply until the operator repoints the service, so without a timeout each new connection attempt blocks the request that needed it for as long as the failover takes.
The timeout does not shorten the failover itself; it makes those requests fail and be retried instead of hanging.
JetStream nodes must use independent persistent volumes in separate failure domains and `sync_interval: always` for the documented acknowledged-write guarantee across operating-system failures.
Replication alone does not imply immediate fsync; see the [NATS durability documentation](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md).
S3-compatible services must preserve acknowledged writes across their supported failover and provide read-after-write consistency.
These are deployment guarantees, not settings that Zendrite can establish through a stream API.

The guarantee covers committed database writes and acknowledged JetStream publications within configured retention limits.
It does not promise an atomic transaction spanning PostgreSQL, JetStream, media, and remote HTTP delivery.
The room input stream expires messages after 24 hours, typing after 60 seconds, and presence after five minutes.
Interest retention removes messages after all interested consumers acknowledge them, and operator-supplied size/count limits still apply.
Federation, appservice, and push retries can produce duplicate remote deliveries.

## Existing streams and consumers

Stream updates use the fully prefixed name and preserve operator settings such as size limits.
Explicit replication changes are applied in place, then verified against broker state.
In standalone mode, an omitted or zero `replicas` value preserves existing replica counts and defaults newly created streams to one replica.
HA rejects storage, retention, age, or subject changes rather than risking queued data.
A failed stream update never deletes and recreates the stream, in either HA or standalone mode.
Standalone retains its explicit memory-storage option and degraded memory fallback for new streams; HA never uses that fallback.
The package stream templates are never mutated by memory-mode startup.

Before enabling HA on an existing installation, migrate its memory-based typing/presence streams and legacy push/ack-all consumers during an offline maintenance window.
Drain or back up their data and durable acknowledgement positions before an operator changes them.
HA rejects incompatible existing durable consumer storage, replication, or acknowledgement settings and blocks application stream/consumer deletion paths.
The roomserver's headers-only ephemeral push consumer is allowed because it only wakes durable room workers and owns no recoverable state.
Consumer validation uses paginated listing with error reporting and cancels further enumeration requests and drains the consumer channel before returning on failure.
Do not delete occupied streams or consumers to make validation pass.

## Candidate compatibility and upgrades

Candidates compute a fingerprint from the exact executable, public signing identities, effective PostgreSQL target and session parameters, broker endpoints/domain/prefix/replication, media namespace, search settings, and the set of enabled MSCs.
Enabling or disabling an MSC is therefore an offline upgrade.
Disabling `msc2836` leaves its `MSC2836RoomServerConsumerPull` durable, named with the topic prefix, on the roomserver output stream, which HA will not delete; remove it during the same maintenance window, or it retains every roomserver output message.
Signing private keys and database passwords, NATS credentials, and S3 credentials are excluded from the stored record.
Error messages never print the fingerprint inputs.
Password/token rotation within the same database user, NATS account, and S3 namespace does not require resetting compatibility.
NATS credentials on every candidate must resolve to the same account; changing an account is an offline namespace migration.
Use the same executable artifact and homeserver configuration on every candidate; mixed binaries, including different local builds, are unsupported.

Ownership authority itself lives in `zendrite_ha_ownership`, separately from the compatibility records described here.
The Lease annotation `zendrite.codefloe.com/compatibility` pins the election scope before ownership is granted.
An existing Lease with a missing or mismatched annotation fails closed, including a manually precreated Lease.
The initial candidate creates the annotated Lease; do not replace an existing Lease to bypass a mismatch.
Owner history is retained independently of the Lease in `zendrite_ha_ownership_history`.
After fenced ownership, a dedicated PostgreSQL connection obtains an advisory migration lock and commits a second compatibility record in `zendrite_ha_compatibility` before component constructors run.
That connection holds the lock through initialization, recovery, and listener binding.
It carries the activation marker as well, so a successor's sweep terminates it like any other application backend.
Application initialization failure leaves the compatibility record pinned, stops readiness, and requires a fresh process.
Migration transactions retain their existing rollback behavior; a partially completed sequence is retried only by the same compatible binary under safe ownership.
Incompatible schema errors during initialization prevent readiness.

Upgrades require an offline maintenance window: stop and fence every candidate, back up the database/broker/media, verify the intended schema migration with the new binary, and deploy the same artifact everywhere.
Only after verifying that no former activation can return may an operator deliberately reset both compatibility records and bootstrap the new configuration.
A failed or ambiguous migration must leave candidates stopped until the schema is repaired or the coordinated backup is restored.
Changing the fingerprint is not a schema migration and does not make an old binary safe to run against a newer schema.
Rolling mixed-version upgrades and automatic schema downgrade are unsupported.

## Promotion recovery and search

A fresh process initializes caches and synchronously restores sync stream positions and notifier membership from PostgreSQL.
Federation destination enumeration must succeed in HA mode; recovered destination workers retain the existing staggered retry schedule, so readiness does not wait for remote homeservers to become available.
Durable consumers resume their persisted acknowledgement positions.
Before opening public listeners, startup waits for the sync consumers to apply their captured stream backlog, including already delivered but unacknowledged messages.
The presence consumer participates only when inbound or outbound presence is enabled.
Cancellation, broker errors, or a 120-second sync recovery timeout prevent readiness.
Typing state, login/SSO intermediates, and sliding-sync sessions remain transient process state; clients may need to retry or establish a fresh session after promotion.
Client transaction IDs are recorded in the database, so retrying a message after promotion does not store it twice.

When search is enabled, set `sync_api.search.rebuild_on_startup: true`.
Every activation builds a fresh Bleve index from PostgreSQL before starting sync consumers, then catches up the durable sync backlog before readiness.
Database reads, index writes, and ownership cancellation abort recovery on error.
The rebuild advances by the maximum stream position in each batch, independent of map iteration order.
It starts from an empty index so deleted or redacted documents cannot survive from a candidate's older local index.

`sync_api.search.index_path` is a candidate-local workspace for disposable recovery indexes, separate from shared media.
It does not require a shared index or persistent volume and must not point to a concurrently opened Bleve index.
Normal shutdown removes the activation's temporary index after closing it; a killed process can leave unused `recovery-*` directories for offline cleanup.
Provision space for a complete rebuilt index and possible crash leftovers.
Large indexes can exceed the 120-second overall takeover target; measure recovery for the deployment workload.

## Media fencing boundary and qualification

Media originals use content-hash keys and PostgreSQL metadata controls their publication.
A repeated original upload is idempotent, but thumbnail replacements and media/thumbnail deletion are mutable operations.
The fencing provider must drain or otherwise exclude previously dispatched mutable media operations before authorizing a successor, in addition to its PostgreSQL and broker barriers.
Shared media does not itself supply that barrier or solve local search-index persistence.
Deployments without those proofs remain blocked by the fencing release gate.

Automated tests cover configuration rejection, credential-independent compatibility, Lease mismatches, PostgreSQL migration serialization, search recovery errors/cancellation/cursors, namespace-safe stream updates, replication upgrades, and preservation of acknowledged messages and pending consumer state across a three-node broker leader failure.
They also cover the ownership failure envelope.
A leader paused beyond expiry and resumed after takeover is refused at the database, broker, media, admission, and dispatch boundaries, including on a connection pool it had already opened and used.
A leader partitioned from the Kubernetes API discovers that it was replaced through the database alone, while its Lease context is still live and it still holds data-plane access.
A sweep that completes a pass without converging blocks promotion and reports how many backends still hold the retired generation.
An application that will not stop within the shutdown budget forces process termination.
A transaction already dispatched completes while newly admitted dispatch is refused.
PostgreSQL failover and complete Matrix client, sync, media, search and federation recovery, including against the ADR reference workload, are covered by the [HA qualification](/development/ha-qualification/) harness.
Infrastructure power-off, correlated storage failures, and S3 or shared-filesystem failover remain outside it and should be exercised on the target infrastructure.
