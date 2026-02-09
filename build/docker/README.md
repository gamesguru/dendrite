# Docker images

Docker images for Dendrite are available from:

- [Docker Hub](https://hub.docker.com/r/pats22/dendrite): `docker.io/pats22/dendrite`
- [Codefloe Registry](https://codefloe.com/pat-s/-/packages/container/pat-s/dendrite): `codefloe.com/pat-s/dendrite`

## Dockerfile

The `Dockerfile` is a multistage file which can build Dendrite.
From the root of the Dendrite repository, run:

```bash
docker build -t pats22/dendrite:latest .
```

## Compose file

The `docker-compose.yaml` file runs a Dendrite deployment with Postgres.

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
