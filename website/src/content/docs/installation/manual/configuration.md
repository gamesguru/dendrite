---
title: Configuring Zendrite
description: YAML configuration options for Zendrite
---

A YAML configuration file is used to configure Zendrite.
A sample configuration file (`zendrite-sample.yaml`) is present in the top level of the Zendrite repository.

You will need to duplicate the sample, calling it `zendrite.yaml` for example, and then tailor it to your installation.
At a minimum, you will need to populate the following sections:

## Server name

First of all, you will need to configure the server name of your Matrix homeserver.
This must match the domain name that you have selected whilst [configuring the domain name delegation](/installation/domainname#delegation).

In the `global` section, set the `server_name` to your delegated domain name:

```yaml
global:
  # ...
  server_name: example.com
```

## Server signing keys

Next, you should tell Zendrite where to find your [server signing keys](/installation/manual/signingkey).

In the `global` section, set the `private_key` to the path to your server signing key:

```yaml
global:
  # ...
  private_key: /path/to/matrix_key.pem
```

## JetStream configuration

Zendrite deployments can use the built-in NATS Server rather than running a standalone server.
If you want to use a standalone NATS Server anyway, you can also configure that too.

### Built-in NATS Server

In the `global` section, under the `jetstream` key, ensure that no server addresses are configured and set a `storage_path` to a persistent folder on the filesystem:

```yaml
global:
  # ...
  jetstream:
    storage_path: /path/to/storage/folder
    topic_prefix: Zendrite
```

### Standalone NATS Server

To use a standalone NATS Server instance, you will need to configure `addresses` field to point to the port that your NATS Server is listening on:

```yaml
global:
  # ...
  jetstream:
    addresses:
      - localhost:4222
    topic_prefix: Zendrite
```

You do not need to configure the `storage_path` when using a standalone NATS Server instance.
In the case that you are connecting to a multi-node NATS cluster, you can configure more than one address in the `addresses` field.

### Authenticating to NATS

If your NATS deployment requires authentication, Zendrite supports the following methods:

- **NATS credentials file** — set `credentials_path` to a `.creds` file. The file is re-read on every reconnect, so you can rotate credentials without restarting Zendrite:

  ```yaml
  global:
    # ...
    jetstream:
      addresses:
        - nats://nats.example.com:4222
      credentials_path: /etc/zendrite/nats.creds
      topic_prefix: Zendrite
  ```

- **Username and password in the URL** — include the credentials directly in the `addresses` entry:

  ```yaml
  global:
    # ...
    jetstream:
      addresses:
        - nats://user:password@nats.example.com:4222
      topic_prefix: Zendrite
  ```

Both methods also work when NATS is configured to use [auth callout](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_callout), because the client-side authentication is unchanged: Zendrite presents the configured credentials on every connect/reconnect, and the auth callout service decides whether to accept them.

If the configured credentials are missing or rejected, Zendrite logs a clear authentication error and exits with a non-zero status instead of hanging.

## Database connection using a global connection pool

If you want to use a single connection pool to a single PostgreSQL database, then you must configure the `database` section within the `global` section:

```yaml
global:
  # ...
  database:
    connection_string: postgres://user:pass@hostname/database?sslmode=disable
    max_open_conns: 90
    max_idle_conns: 5
    conn_max_lifetime: -1
```

See also [Connection strings](/installation/manual/database/#connection-strings)

It's possible to configure per-component `database` sections in other areas of the configuration file, e.g. under the `app_service_api`, `federation_api`, `key_server`, `media_api`, `mscs`, `relay_api`, `room_server`, `sync_api` and `user_api` blocks, these will override the `global` database configuration.

## Full-text search

Zendrite supports full-text indexing using [Bleve](https://github.com/blevesearch/bleve).
It is configured in the `sync_api` section as follows.

Depending on the language most likely to be used on the server, it might make sense to change the `language` used when indexing, to ensure the returned results match the expectations.
A full list of possible languages can be found in `internal/fulltext/bleve.go`.

```yaml
sync_api:
  # ...
  search:
    enabled: false
    index_path: "./searchindex"
    language: "en"
```

If you enable this later you can [reindex existing rooms via the admin API](/administration/adminapi/#get-_zendriteadminfulltextreindex).

## OIDC delegated authentication (MSC3861)

Zendrite can delegate authentication to an external OIDC provider such as [Matrix Authentication Service (MAS)](https://github.com/element-hq/matrix-authentication-service).
When enabled, Zendrite no longer manages passwords or login sessions directly — all authentication is handled by the OIDC provider via token introspection.

See the [MSC3861 documentation](/mscs#msc3861-oidc-delegated-authentication) for full configuration details.

## Other sections

There are other options which may be useful so review them all.
In particular, if you are trying to federate from your Zendrite instance into public rooms then configuring the `key_perspectives` (like `matrix.org` in the sample) can help to improve reliability considerably by allowing your homeserver to fetch public keys for dead homeservers from another living server.
