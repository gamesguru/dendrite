# Docker images

Docker images for Dendrite are available from:

- [Docker Hub](https://hub.docker.com/r/pats22/dendrite): `docker.io/pats22/dendrite`
- [Codefloe Registry](https://codefloe.com/pat-s/-/packages/container/pat-s/dendrite): `codefloe.com/pat-s/dendrite`

## Dockerfile

The `Dockerfile` is a multistage file which can build Dendrite.
From the root of the Dendrite repository, initialize the Rezzy submodule and
ensure the hooked gomatrixserverlib checkout is at `../gmsl`, then run:

```bash
git submodule update --init rezzy
docker buildx build --load \
  --build-context rezzy=./rezzy \
  --build-context gmsl=../gmsl \
  -t pats22/dendrite:latest .
```

## Compose file

The `docker-compose.yaml` file runs a Dendrite deployment with Postgres.

## Complement with MSC4521

`build/scripts/ComplementRezzy.Dockerfile` links the native Rezzy library, so
the MSC4521 reconciliation endpoints are real instead of returning the
unsupported stub. Build it with the same named contexts as the production
image:

```bash
git submodule update --init rezzy
docker buildx build --load \
  --build-context rezzy=./rezzy \
  --build-context gmsl=../gmsl \
  -f build/scripts/ComplementRezzy.Dockerfile \
  -t complement-dendrite:rezzy .
```

Then run the MSC4521 suite from the Complement branch that carries it
(requires Go 1.26 or later):

```bash
COMPLEMENT_REF=guru/feat/msc4521-libminisketch-set-recon
mkdir -p /tmp/complement-msc4521
wget -O - "https://github.com/gamesguru/complement/archive/refs/heads/${COMPLEMENT_REF}.tar.gz" \
  | tar -xz --strip-components=1 -C /tmp/complement-msc4521
cd /tmp/complement-msc4521
```

Zendrite implements the responder side and an explicit admin trigger, but not
periodic outbound reconciliation yet. Run the responder-side tests with:

```bash
COMPLEMENT_BASE_IMAGE=complement-dendrite:rezzy \
  go test -tags zendrite_blacklist -v -timeout 45m \
  -run 'TestMSC4521/(Primitives|Compatibility)' ./tests/msc4521/...
COMPLEMENT_BASE_IMAGE=complement-dendrite:rezzy \
  go test -tags zendrite_blacklist -v -timeout 15m \
  -run 'TestMSC4521/(Convergence|Adversarial)/(ActiveRoom|NonMembersAndACLBlocked)' \
  ./tests/msc4521/...
```

The remaining convergence and adversarial tests expect the homeserver to
initiate exchanges and will fail until outbound scheduling exists. Run them
with:

```bash
COMPLEMENT_BASE_IMAGE=complement-dendrite:rezzy \
  go test -tags zendrite_blacklist -v -timeout 60m ./tests/msc4521/...
```

The `msc4521_enabled` configuration option is set by `generate-config --ci`,
which the Complement image's entrypoint already runs.

## Configuration

The compose file refers to the `./config` volume as where the runtime config should come from.
The mounted folder must contain:

- `dendrite.yaml` configuration file (based on the [`dendrite-sample.yaml`](https://codefloe.com/pat-s/dendrite/src/branch/main/dendrite-sample.yaml))
- `matrix_key.pem` server key, as generated using `generate-keys`

To generate a signing key:

```bash
mkdir -p ./config
docker run --rm --entrypoint="/usr/bin/generate-keys" \
  -v $(pwd)/config:/mnt \
  pats22/dendrite:latest \
  -private-key /mnt/matrix_key.pem
```

The key file will now exist in `./config` and can be mounted into place.

## Starting Dendrite

Create your config based on the [`dendrite-sample.yaml`](https://codefloe.com/pat-s/dendrite/src/branch/main/dendrite-sample.yaml) sample configuration file.

Then start the deployment:

```bash
docker compose up
```
