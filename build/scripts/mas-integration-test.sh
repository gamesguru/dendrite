#!/usr/bin/env bash
# End-to-end integration test for MSC3861 delegated authentication against a
# real Matrix Authentication Service (MAS) instance.
#
# It builds Zendrite, starts MAS in Docker (with its own PostgreSQL), registers
# a password user via mas-cli and performs the compatibility login against
# MAS. This login is what triggers MAS to provision the account and device on
# Zendrite through the /_synapse/mas endpoints, so a 200 login response plus a
# successful authenticated call against Zendrite proves the whole loop:
#
#   client -> MAS compat login -> MAS provisions Zendrite (/_synapse/mas/*)
#   client -> Zendrite whoami  -> Zendrite introspects the token at MAS
#
# This is the exact flow that failed with 404 M_UNRECOGNIZED before the
# /_synapse/mas API was implemented (see oidc.md for the configuration this
# test mirrors).
#
# Requirements: docker, go, python3, openssl, jq.
#
# Environment overrides:
#   MAS_IMAGE        MAS container image (default ghcr.io/element-hq/matrix-authentication-service:1.22.0)
#   POSTGRES_IMAGE   PostgreSQL container image for MAS (default postgres:18-alpine)
#   ZENDRITE_PORT    port advertised in Zendrite's public_base_url (default 18008)
#   MAS_PORT         port advertised in MAS's issuer/public URLs (default 18009)
#
# No ports are published to the host: all HTTP traffic runs container to
# container on the Docker network, so the script also works on CI runners
# where published ports are unreachable from the job container.
#   KEEP=1           keep logs and working directory on exit

set -euo pipefail

MAS_IMAGE="${MAS_IMAGE:-ghcr.io/element-hq/matrix-authentication-service:1.22.0}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:18-alpine}"
ZENDRITE_PORT="${ZENDRITE_PORT:-18008}"
MAS_PORT="${MAS_PORT:-18009}"
NETWORK="zendrite-mas-e2e"
DATA_VOLUME="zendrite-mas-e2e-data"
MAS_CONFIG_VOLUME="zendrite-mas-e2e-mas-config"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

WORKDIR=""
CLEANED_UP=0

log() { echo "[mas-e2e] $*"; }
fail() {
  echo "[mas-e2e] FAILED: $*" >&2
  exit 1
}

# wait_for <attempts> <cmd...> retries cmd once per second until it succeeds
# and returns non-zero once the attempts are used up.
wait_for() {
  local attempts="$1"
  shift
  for _ in $(seq 1 "${attempts}"); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "$@" >/dev/null 2>&1
}

# Probes run from inside already running containers on the e2e network, so
# they exercise the DNS and TCP path the services use without spawning a
# throwaway container per attempt.
postgres_ready() { docker exec mas-e2e-postgres pg_isready -h mas-e2e-postgres -U mas; }
mas_ready() { docker exec mas-e2e-zendrite wget -q -O /dev/null http://mas-e2e-mas:8080/health; }
zendrite_ready() { docker exec mas-e2e-zendrite wget -q -O /dev/null http://127.0.0.1:8008/_matrix/client/versions; }
container_running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]; }

# mas_http <url> [wget args...] issues an HTTP request from the Zendrite
# container with busybox wget and prints the response body.
mas_http() {
  local url="$1"
  shift
  docker exec mas-e2e-zendrite wget -q -O - "$@" "${url}"
}

cleanup() {
  [[ "${CLEANED_UP}" -eq 1 ]] && return
  CLEANED_UP=1
  if [[ -n "${WORKDIR}" ]] && [[ "${KEEP:-0}" == "1" ]]; then
    log "KEEP=1: leaving containers running and workdir at ${WORKDIR}"
    return
  fi
  docker rm -f mas-e2e-mas mas-e2e-postgres mas-e2e-zendrite >/dev/null 2>&1 || true
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
  docker volume rm "${DATA_VOLUME}" "${MAS_CONFIG_VOLUME}" >/dev/null 2>&1 || true
  [[ -n "${WORKDIR}" ]] && rm -rf "${WORKDIR}"
}
trap cleanup EXIT

for tool in docker go python3 openssl jq; do
  command -v "${tool}" >/dev/null 2>&1 || fail "required tool '${tool}' not found in PATH"
done

cd "${REPO_ROOT}"

# The workdir only feeds host-side steps (go, openssl, python) and is piped
# into Docker through stdin, never bind-mounted: on CI runners the Docker
# daemon runs in a different mount namespace than the job, so bind mounts of
# job-local paths arrive as empty directories in the containers.
WORKDIR="$(mktemp -d /tmp/zendrite-mas-e2e.XXXXXX)"
log "working directory: ${WORKDIR}"

# --- Generate Zendrite key and config --------------------------------------

log "generating Zendrite signing key"
go run -tags goolm ./cmd/generate-keys -private-key "${WORKDIR}/matrix_key.pem" >/dev/null

log "generating Zendrite config"
# The -dir paths become absolute paths inside the Zendrite container, where
# the workdir is mounted at /data.
go run -tags goolm ./cmd/generate-config -ci -server "localhost" -dir /data >"${WORKDIR}/zendrite.yaml"

ADMIN_TOKEN="$(openssl rand -hex 32)"
CLIENT_SECRET="$(openssl rand -hex 32)"
# MAS requires client IDs to be ULIDs.
CLIENT_ID="$(
  python3 - <<'PYEOF'
import os, time
alpha = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
val = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
print("".join(alpha[(val >> (5 * i)) & 31] for i in range(25, -1, -1)))
PYEOF
)"

# Patch the generated config to enable MSC3861 against the local MAS. The
# replacements are exact-match and fail loudly if generate-config output ever
# drifts.
log "patching config for MSC3861"
python3 - "${WORKDIR}/zendrite.yaml" "${ADMIN_TOKEN}" "${CLIENT_SECRET}" "${CLIENT_ID}" "${MAS_PORT}" "${ZENDRITE_PORT}" <<'PYEOF' || fail "failed to patch zendrite config (generate-config output changed?)"
import re
import sys

path, admin_token, client_secret, client_id, mas_port, zendrite_port = sys.argv[1:7]
s = open(path).read()

old_mscs = """mscs:
  mscs:
  - msc2836
  - msc2444
  - msc2753
"""
new_mscs = """mscs:
  mscs:
  - msc3861
"""
assert old_mscs in s, "mscs list template not found"
s = s.replace(old_mscs, new_mscs, 1)

# Registration is fully delegated to MAS under MSC3861; the validator
# rejects the open-registration defaults the CI config generator emits.
assert "  registration_disabled: false" in s, "registration template not found"
s = s.replace("  registration_disabled: false", "  registration_disabled: true", 1)

block = re.compile(r"  msc3861:\n(?:    .*\n)*?    sso_redirect_allowlist: \[\]\n")
new_block = """  msc3861:
    issuer: "http://localhost:{mas_port}"
    client_id: "{client_id}"
    client_secret: "{client_secret}"
    client_auth_method: "client_secret_basic"
    admin_token: "{admin_token}"
    account_management_url: "http://localhost:{mas_port}/account/"
    introspection_endpoint: "http://mas-e2e-mas:8080/oauth2/introspect"
    public_base_url: "http://localhost:{zendrite_port}"
    sso_redirect_allowlist: []
""".format(
    mas_port=mas_port,
    zendrite_port=zendrite_port,
    client_id=client_id,
    client_secret=client_secret,
    admin_token=admin_token,
)
s, n = block.subn(new_block, s, count=1)
assert n == 1, "msc3861 block template not found"
open(path, "w").write(s)
PYEOF

# --- Start PostgreSQL and MAS ----------------------------------------------

# Pull every image up front. Pulling inline (at the first docker run that
# needs an image) can leave multi-minute gaps between steps, during which a
# memory-constrained CI runner may OOM-kill the idle PostgreSQL container.
log "pre-pulling container images"
docker pull "${POSTGRES_IMAGE}" >/dev/null
docker pull "${MAS_IMAGE}" >/dev/null
docker pull alpine:3 >/dev/null

log "starting Docker network and PostgreSQL for MAS"
# Leftovers from a previous (e.g. KEEP=1) run must not wedge this one.
docker rm -f mas-e2e-mas mas-e2e-postgres mas-e2e-zendrite >/dev/null 2>&1 || true
docker volume rm "${DATA_VOLUME}" >/dev/null 2>&1 || true
docker network create "${NETWORK}" >/dev/null 2>&1 || true
docker run -d --name mas-e2e-postgres --network "${NETWORK}" \
  -e POSTGRES_USER=mas -e POSTGRES_PASSWORD=mas -e POSTGRES_DB=mas \
  "${POSTGRES_IMAGE}" >/dev/null

# Wait for PostgreSQL to accept TCP connections on the Docker network, i.e.
# exactly the path MAS uses (DNS + TCP): pg_isready is pointed at the
# container's network name, not the unix socket.
log "waiting for PostgreSQL to accept connections"
for _ in $(seq 1 60); do
  postgres_ready >/dev/null 2>&1 && break
  container_running mas-e2e-postgres || fail "PostgreSQL container exited early; log follows:\n$(docker logs mas-e2e-postgres 2>&1 | tail -30)"
  sleep 1
done
postgres_ready >/dev/null 2>&1 || fail "PostgreSQL did not become ready"

# Address the database by its container IP instead of the docker DNS name.
# On some CI runners the embedded DNS resolves differently per container: the
# distroless MAS image got "connection refused" for mas-e2e-postgres while
# pg_isready from an alpine container against the same name succeeded. The
# IP removes the resolver from the path entirely.
POSTGRES_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mas-e2e-postgres)"
log "PostgreSQL address: ${POSTGRES_IP}:5432"

log "generating MAS signing key"
# MAS only accepts RSA or EC signing keys.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "${WORKDIR}/mas-key.pem" 2>/dev/null
# Indent the PEM for the YAML block scalar below. The content must sit
# deeper than the "key:" line it belongs to.
sed 's/^/        /' "${WORKDIR}/mas-key.pem" >"${WORKDIR}/mas-key-indented.pem"

MAS_ENCRYPTION_SECRET="$(openssl rand -hex 32)"

log "writing MAS config"
mkdir -p "${WORKDIR}/mas"
cat >"${WORKDIR}/mas/config.yaml" <<EOF
http:
  public_base: "http://localhost:${MAS_PORT}"
  listeners:
    - name: web
      resources:
        - name: discovery
        - name: human
        - name: oauth
        - name: compat
        - name: graphql
        - name: health
      binds:
        - host: "0.0.0.0"
          port: 8080
database:
  host: "${POSTGRES_IP}"
  port: 5432
  username: mas
  password: mas
  database: mas
secrets:
  encryption: "${MAS_ENCRYPTION_SECRET}"
  keys:
    - kid: "e2e"
      key: |
$(cat "${WORKDIR}/mas-key-indented.pem")
passwords:
  enabled: true
matrix:
  kind: synapse
  homeserver: "localhost"
  secret: "${ADMIN_TOKEN}"
  endpoint: "http://mas-e2e-zendrite:8008"
clients:
  - client_id: "${CLIENT_ID}"
    client_auth_method: "client_secret_basic"
    client_secret: "${CLIENT_SECRET}"
EOF

# Copy the MAS config into a named volume by piping a tar stream through
# stdin. Bind mounts are unusable here: when the daemon does not see the
# job's filesystem, a bind mount silently turns into an empty directory and
# mas-cli falls back to its defaults (database on localhost), which fails
# with a misleading "connection refused".
log "seeding MAS config volume"
docker volume create "${MAS_CONFIG_VOLUME}" >/dev/null
tar -C "${WORKDIR}/mas" -c . | docker run --rm -i \
  -v "${MAS_CONFIG_VOLUME}:/config" \
  alpine:3 tar -C /config -x

# CI runners can be memory constrained: the MAS container needs several
# hundred MB, and on a tight runner the OOM killer may take out PostgreSQL
# between steps. Retry the migration a couple of times, restarting
# PostgreSQL if it died, so a transient kill does not fail the whole run.
log "running MAS database migrations"
migrate_ok=0
for attempt in 1 2 3; do
  if docker run --rm --network "${NETWORK}" \
    -v "${MAS_CONFIG_VOLUME}:/config" \
    "${MAS_IMAGE}" database migrate -c /config/config.yaml >/dev/null; then
    migrate_ok=1
    break
  fi
  log "migration attempt ${attempt} failed; postgres at ${POSTGRES_IP}: $(docker inspect -f 'status={{.State.Status}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}}' mas-e2e-postgres 2>/dev/null)"
  docker start mas-e2e-postgres >/dev/null 2>&1 || true
  wait_for 30 postgres_ready || true
done
[[ "${migrate_ok}" == "1" ]] || fail "MAS database migrations failed; PostgreSQL log follows:\n$(docker logs mas-e2e-postgres 2>&1 | tail -30)"

log "building Zendrite (static Linux binary for the container)"
GOARCH="$(docker info --format '{{.Architecture}}')"
case "${GOARCH}" in
aarch64 | arm64) GOARCH=arm64 ;;
x86_64 | amd64) GOARCH=amd64 ;;
*) fail "unsupported container architecture ${GOARCH}" ;;
esac
# WebP's dynamic backend requires a libc loader even with CGO disabled.
CGO_ENABLED=0 GOOS=linux GOARCH="${GOARCH}" go build -tags goolm,nodynamic -o "${WORKDIR}/zendrite" ./cmd/zendrite

log "starting MAS"
docker run -d --name mas-e2e-mas --network "${NETWORK}" \
  -v "${MAS_CONFIG_VOLUME}:/config" \
  "${MAS_IMAGE}" server -c /config/config.yaml >/dev/null

# Same DNS-bypass as for the database: point Zendrite's introspection
# endpoint at the MAS container IP. The config was written with the
# mas-e2e-mas hostname as a placeholder and is rewritten in place here,
# before it is copied into the Zendrite data volume.
MAS_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mas-e2e-mas)"
log "MAS address: ${MAS_IP}:8080"
sed "s|http://mas-e2e-mas:8080|http://${MAS_IP}:8080|" "${WORKDIR}/zendrite.yaml" >"${WORKDIR}/zendrite.yaml.tmp" && mv "${WORKDIR}/zendrite.yaml.tmp" "${WORKDIR}/zendrite.yaml"

log "starting Zendrite"
# Zendrite runs in a container on the same Docker network as MAS so that no
# host-to-container networking is required. Its data (config, signing key,
# databases) lives on a named volume, seeded by piping a tar stream through
# stdin for the same bind-mount-visibility reasons as the MAS config.
docker volume create "${DATA_VOLUME}" >/dev/null
tar -C "${WORKDIR}" -c zendrite.yaml matrix_key.pem zendrite | docker run --rm -i \
  -v "${DATA_VOLUME}:/data" \
  alpine:3 sh -c 'tar -C /data -x && chmod +x /data/zendrite' >/dev/null
docker run -d --name mas-e2e-zendrite --network "${NETWORK}" \
  -v "${DATA_VOLUME}:/data" -w /data \
  alpine:3 /data/zendrite --config /data/zendrite.yaml >/dev/null

log "waiting for Zendrite and MAS to become ready"
# MAS compiles its policy module on startup, which can take a while on
# emulated or slow runners.
for _ in $(seq 1 180); do
  if zendrite_ready >/dev/null 2>&1 && mas_ready >/dev/null 2>&1; then
    break
  fi
  container_running mas-e2e-zendrite || fail "Zendrite container exited early; log follows:\n$(docker logs mas-e2e-zendrite 2>&1 | tail -30)"
  sleep 1
done
zendrite_ready >/dev/null 2>&1 || fail "Zendrite did not become ready; log follows:\n$(docker logs mas-e2e-zendrite 2>&1 | tail -30)"
mas_ready >/dev/null 2>&1 || fail "MAS did not become ready; log follows:\n$(docker logs mas-e2e-mas 2>&1 | tail -30)"

# --- Exercise the delegated auth flow --------------------------------------

log "registering a password user in MAS"
docker run --rm --network "${NETWORK}" \
  -v "${MAS_CONFIG_VOLUME}:/config" \
  "${MAS_IMAGE}" manage register-user \
  e2euser --password 'E2e-Passw0rd!' --yes \
  -c /config/config.yaml >/dev/null

log "performing compatibility login against MAS (triggers provisioning on Zendrite)"
login_body='{"type":"m.login.password","identifier":{"type":"m.id.user","user":"e2euser"},"password":"E2e-Passw0rd!"}'
login_response="$(
  mas_http "http://mas-e2e-mas:8080/_matrix/client/v3/login" \
    --header "Content-Type: application/json" --post-data "${login_body}"
)" || fail "compat login failed; MAS log follows:\n$(docker logs mas-e2e-mas 2>&1 | tail -50)\nZendrite log follows:\n$(docker logs mas-e2e-zendrite 2>&1 | tail -50)"

access_token="$(jq -r '.access_token // empty' <<<"${login_response}")"
user_id="$(jq -r '.user_id // empty' <<<"${login_response}")"
device_id="$(jq -r '.device_id // empty' <<<"${login_response}")"
[[ -n "${access_token}" ]] || fail "login response missing access_token: ${login_response}"
[[ "${user_id}" == "@e2euser:localhost" ]] || fail "unexpected user_id: ${user_id}"
[[ -n "${device_id}" ]] || fail "login response missing device_id: ${login_response}"
log "login ok: ${user_id} device ${device_id}"

log "calling /account/whoami on Zendrite with the MAS-issued token"
whoami_response="$(
  mas_http "http://mas-e2e-zendrite:8008/_matrix/client/v3/account/whoami" \
    --header "Authorization: Bearer ${access_token}"
)" || fail "whoami failed (token introspection or provisioning broken); Zendrite log follows:\n$(docker logs mas-e2e-zendrite 2>&1 | tail -50)"
whoami_user="$(jq -r '.user_id // empty' <<<"${whoami_response}")"
[[ "${whoami_user}" == "@e2euser:localhost" ]] || fail "whoami returned unexpected user: ${whoami_response}"
log "whoami ok: ${whoami_user}"

log "performing a second login (relogin path)"
mas_http "http://mas-e2e-mas:8080/_matrix/client/v3/login" \
  --header "Content-Type: application/json" --post-data "${login_body}" |
  jq -e ".access_token" >/dev/null ||
  fail "second login failed; MAS log follows:\n$(docker logs mas-e2e-mas 2>&1 | tail -50)"
log "relogin ok"

log "PASS: delegated auth end-to-end flow works"
