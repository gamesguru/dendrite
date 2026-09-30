#!/usr/bin/env bash
# HA qualification harness (issue #352).
#
# Runs the fault scenarios in test/hachaos against real dependencies: a
# PostgreSQL primary with a synchronous standby, a three-node JetStream
# cluster, an S3 server, etcd and a kube-apiserver. Candidates are real
# zendrite processes with HA enabled, and a standalone homeserver exercises
# federation in both directions.
#
# The harness writes client-go's in-cluster service-account files under
# /var/run/secrets, which needs root, so run it in a disposable container.
# CI does; locally, set HACHAOS_CONTAINER=1 and Docker provides one.
#
# Usage: build/scripts/ha-qualification.sh [go test -run pattern]
#
# Environment:
#   HACHAOS_TIER       ci (default): bounded scenarios with short election timings
#                      full: every scenario with the reference timings
#                      reference: full, seeded with the ADR reference workload
#   HACHAOS_SCENARIOS  comma-separated scenario names to run (default: the tier's set)
#   HACHAOS_GROUP      crash, partition, dependency or lifecycle: one CI group
#   HACHAOS_PHASE      all (default), build, or run using already-built tools
#   HACHAOS_ARTIFACTS  output directory (default .hachaos/artifacts/<timestamp>)
#   HACHAOS_CONTAINER  1 to run inside a golang container via Docker
#   HACHAOS_DOCKER_ARGS extra docker run arguments, e.g. --cpus=2 to reproduce a loaded CI host
#   HACHAOS_TIMEOUT    go test timeout (default 60m)
#   HACHAOS_KEEP       1 to keep the scenarios' data directories
#   HACHAOS_TOOLS      downloaded tools and built binaries (default .hachaos/tools);
#                      use separate directories for concurrent builds, or share read-only in run phase
#   HACHAOS_SEARCH_HISTORY  searchable messages to send before the fault, so the
#                      successor rebuilds a large search index (default 0)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

GO_IMAGE="golang:1.27-trixie"
ENVTEST_VERSION="1.37.0"
declare -A ENVTEST_SHA512=(
  [amd64]="1d1c453633b72c161a5d5a886cde7ac850be1a2ac796a9e1d4ffacacc64868295bdd2d57aa66cd0c158f5ce510f5dfe3fbc61ac21bd3dcfb875bd70658aa663a"
  [arm64]="ae6a670502988200b0131c943758cfd3d3a58cf4e6247ef7f0e6a6467f1cd9a333c802e86101f0446b1b92a0e327387224e1e44fe1a4693da0929c6e529cfe9a"
)

export HACHAOS_TIER="${HACHAOS_TIER:-ci}"
HACHAOS_ARTIFACTS="${HACHAOS_ARTIFACTS:-$ROOT/.hachaos/artifacts/$(date -u +%Y%m%dT%H%M%SZ)-$HACHAOS_TIER}"
# Child processes need absolute paths independent of their working directory.
mkdir -p "$HACHAOS_ARTIFACTS"
HACHAOS_ARTIFACTS="$(cd "$HACHAOS_ARTIFACTS" && pwd)"
export HACHAOS_ARTIFACTS

if [[ "${HACHAOS_CONTAINER:-}" == 1 ]]; then
  mkdir -p "$HACHAOS_ARTIFACTS"
  # Map the artifact directory to the same path inside the container so the
  # report's paths make sense on the host.
  # Word splitting is intended: the value is a list of docker run options.
  # shellcheck disable=SC2086
  exec docker run --rm ${HACHAOS_DOCKER_ARGS:-} \
    -e HACHAOS_PHASE -e HACHAOS_TIER -e HACHAOS_SCENARIOS -e HACHAOS_GROUP -e HACHAOS_TIMEOUT -e HACHAOS_KEEP -e HACHAOS_TOOLS -e HACHAOS_SEARCH_HISTORY \
    -e HACHAOS_ARTIFACTS="$HACHAOS_ARTIFACTS" \
    -v "$ROOT:$ROOT" -w "$ROOT" \
    -v "$HACHAOS_ARTIFACTS:$HACHAOS_ARTIFACTS" \
    -v hachaos-go-mod:/go/pkg/mod -v hachaos-go-build:/root/.cache/go-build \
    "$GO_IMAGE" bash build/scripts/ha-qualification.sh "$@"
fi

mkdir -p "$HACHAOS_ARTIFACTS"
TOOLS="${HACHAOS_TOOLS:-$ROOT/.hachaos/tools}"
mkdir -p "$TOOLS"
TOOLS="$(cd "$TOOLS" && pwd)"

phase="${HACHAOS_PHASE:-all}"
case "$phase" in
  all|build|run) ;;
  *) echo "Unknown HACHAOS_PHASE: $phase (expected all, build or run)" >&2; exit 1 ;;
esac

# Runtime steps need no Go toolchain or module cache.
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
assets="$TOOLS/envtest-$ENVTEST_VERSION-$arch"
bin="$TOOLS/bin"
if [[ "$phase" != run ]]; then
  started=$SECONDS
  # etcd and kube-apiserver from the pinned envtest release.
  if [[ ! -x "$assets/kube-apiserver" ]]; then
    archive="$TOOLS/envtest-$ENVTEST_VERSION-$arch.tar.gz"
    curl -fsSL -o "$archive" "https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v$ENVTEST_VERSION/envtest-v$ENVTEST_VERSION-linux-$arch.tar.gz"
    echo "${ENVTEST_SHA512[$arch]}  $archive" | sha512sum -c - >/dev/null
    mkdir -p "$assets"
    tar -xzf "$archive" -C "$assets" --strip-components=2
    rm -f "$archive"
  fi
  echo "HA qualification: control-plane tools ready in $((SECONDS - started))s"

  # Build once for all groups, including the harness's NATS and S3 roles.
  # The upgrade builds differ from the release build only in their version
  # string and HA upgrade contract: zendrite-next is the adjacent level, which
  # a rolling upgrade admits, and zendrite-unadmitted is a level that needs
  # the offline upgrade.
  started=$SECONDS
  mkdir -p "$bin"
  contract="codefloe.com/pat-s/zendrite/setup/ha.contractOverride"
  version="codefloe.com/pat-s/zendrite/internal.version"
  go build -tags goolm -trimpath -buildvcs=false -o "$bin/zendrite" ./cmd/zendrite
  go build -tags goolm -trimpath -buildvcs=false -ldflags "-X $version=hachaos-next -X $contract=adjacent" -o "$bin/zendrite-next" ./cmd/zendrite
  go build -tags goolm -trimpath -buildvcs=false -ldflags "-X $version=hachaos-unadmitted -X $contract=unadmitted" -o "$bin/zendrite-unadmitted" ./cmd/zendrite
  go test -c -tags goolm -trimpath -buildvcs=false -o "$bin/hachaos.test" ./test/hachaos/
  echo "HA qualification: binaries built in $((SECONDS - started))s"
fi
[[ "$phase" != build ]] || exit 0

# Fail explicitly if the build step did not supply a complete tool set.
for executable in "$assets/kube-apiserver" "$assets/etcd" "$bin/zendrite" "$bin/zendrite-next" "$bin/zendrite-unadmitted" "$bin/hachaos.test"; do
  if [[ ! -x "$executable" ]]; then
    echo "Missing $executable; run HACHAOS_PHASE=build first" >&2
    exit 1
  fi
done
export HACHAOS_KUBE_ASSETS="$assets"
export HACHAOS_ZENDRITE="$bin/zendrite"
export HACHAOS_ZENDRITE_NEXT="$bin/zendrite-next"
export HACHAOS_ZENDRITE_UNADMITTED="$bin/zendrite-unadmitted"

started=$SECONDS
# PostgreSQL server binaries. Package scripts must not start a cluster of
# their own: the harness runs its own primary and standby.
if [[ -z "${HACHAOS_PG_BIN:-}" ]] && ! compgen -G "/usr/lib/postgresql/*/bin/pg_ctl" >/dev/null; then
  if [[ $(id -u) -ne 0 ]] || ! command -v apt-get >/dev/null; then
    echo "PostgreSQL server binaries are missing; install them or set HACHAOS_PG_BIN" >&2
    exit 1
  fi
  printf '#!/bin/sh\nexit 101\n' >/usr/sbin/policy-rc.d
  chmod +x /usr/sbin/policy-rc.d
  mkdir -p /etc/postgresql-common
  echo "create_main_cluster = false" >/etc/postgresql-common/createcluster.conf
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends postgresql >/dev/null
fi

echo "HA qualification: PostgreSQL tools ready in $((SECONDS - started))s"

echo "HA qualification: tier $HACHAOS_TIER, artifacts in $HACHAOS_ARTIFACTS"
status=0
"$bin/hachaos.test" -test.count=1 -test.timeout "${HACHAOS_TIMEOUT:-60m}" -test.v -test.run "${1:-.}" 2>&1 | tee "$HACHAOS_ARTIFACTS/go-test.log" || status=$?
if [[ -f "$HACHAOS_ARTIFACTS/report.md" ]]; then
  cat "$HACHAOS_ARTIFACTS/report.md"
fi
# Pull request runs keep only the step log, so show enough of every failed
# scenario to start investigating without its artifacts.
for result in "$HACHAOS_ARTIFACTS"/*/result.json; do
  if [[ ! -f "$result" ]] || ! grep -q '"passed": false' "$result"; then
    continue
  fi
  dir="$(dirname "$result")"
  echo "==== $(basename "$dir"): timeline"
  grep -E '"kind":"(fault|process|router)"' "$dir/timeline.jsonl" | tail -n 60 || true
  for diagnosis in "$dir"/duplicate-*.txt "$dir"/missing-*.txt; do
    [[ -f "$diagnosis" ]] || continue
    echo "==== $(basename "$dir"): $(basename "$diagnosis")"
    head -n 40 "$diagnosis"
  done
  for log in "$dir"/cand-*.log; do
    echo "==== $(basename "$dir"): $(basename "$log") warnings and errors"
    grep -E 'level=(warning|error|fatal|panic)|^====' "$log" | tail -n 40 || true
  done
  # Goroutine dumps taken while a setup request hung (#438), cut down to the
  # client request and room input paths. Consumer workers idling in a fetch
  # are their normal state and would crowd out the rest.
  for dump in "$dir"/goroutines-*.txt; do
    [[ -f "$dump" ]] || continue
    echo "==== $(basename "$dir"): $(basename "$dump"), request and room input goroutines"
    awk 'BEGIN { RS = ""; ORS = "\n\n" } /zendrite\/(clientapi\/routing|roomserver\/internal|roomserver\/producers)\// && !/jetstream\.FetchOwned/' "$dump" | head -n 300 || true
  done
  for state in "$dir"/jetstream-*.txt; do
    [[ -f "$state" ]] || continue
    echo "==== $(basename "$dir"): $(basename "$state")"
    head -n 60 "$state"
  done
done
exit "$status"
