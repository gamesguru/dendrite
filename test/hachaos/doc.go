// Package hachaos qualifies active-passive HA against real dependencies and
// injected faults. It is the release gate described in issue #352.
//
// The harness runs every dependency as a real process on one host: a
// PostgreSQL primary with a synchronous standby, a three-node JetStream
// cluster, an S3 server, etcd and a kube-apiserver. Candidates are real
// zendrite processes with HA enabled. Fault proxies sit between candidates and
// every dependency so that partitions can be applied per candidate, and a
// second, standalone homeserver exercises federation in both directions.
//
// The suite is skipped unless HACHAOS_TIER is set. Run it through
// build/scripts/ha-qualification.sh, which provisions the dependencies.
package hachaos
