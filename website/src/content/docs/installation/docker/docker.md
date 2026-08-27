---
title: Docker Setup
description: Installing and running Zendrite with Docker
---

Zendrite provides an example Docker compose file in `build/docker/docker-compose.yaml`, which needs some preparation to start successfully.
Please note that this compose file only has Postgres as a dependency, and you need to configure a [reverse proxy](/installation/planning#reverse-proxy).

Docker images are available from:

- [Docker Hub](https://hub.docker.com/r/pats22/zendrite): `pats22/zendrite`
- [Codefloe Registry](https://codefloe.com/pat-s/-/packages/container/zendrite): `codefloe.com/pat-s/zendrite`

## Preparations

### Generate a private key

First we'll generate a private key, which is used to sign events.
The following will create one in `./config`:

```bash
mkdir -p ./config
docker run --rm --entrypoint="/usr/bin/generate-keys" \
  -v $(pwd)/config:/mnt \
  pats22/zendrite:latest \
  -private-key /mnt/matrix_key.pem
```

(**NOTE**: This only needs to be executed **once**, as you otherwise overwrite the key)

### Generate a config

Similar to the command above, we can generate a config to be used, which will use the correct paths as specified in the example docker-compose file.
Change `server` to your domain and `db` according to your changes to the docker-compose file (`services.postgres.environment` values):

```bash
mkdir -p ./config
docker run --rm --entrypoint="/bin/sh" \
  -v $(pwd)/config:/mnt \
  pats22/zendrite:latest \
  -c "/usr/bin/generate-config \
    -dir /var/zendrite/ \
    -db postgres://zendrite:itsasecret@postgres/zendrite?sslmode=disable \
    -server YourDomainHere > /mnt/zendrite.yaml"
```

You can then change `config/zendrite.yaml` to your liking.

## Starting Zendrite

Once you're done changing the config, you can now start up Zendrite with

```bash
docker compose up
```
