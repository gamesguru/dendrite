---
title: Migration from Dendrite
description: Guide for upgrading from Dendrite to Zendrite
---

Zendrite is a fork of [Dendrite](https://github.com/element-hq/dendrite).
If you are running an existing Dendrite installation, follow the steps below to migrate to Zendrite.

## Binary name

The binary has been renamed from `dendrite` to `zendrite`.
Update any systemd unit files, scripts, or process managers that reference the old binary name.

## Config file

The default config file path changed from `dendrite.yaml` to `zendrite.yaml`.
If `zendrite.yaml` is not found, Zendrite will automatically fall back to `dendrite.yaml` and log a warning.
You can also pass the path explicitly:

```sh
zendrite --config dendrite.yaml
```

Renaming the file to `zendrite.yaml` is recommended to silence the warning.

The sample config file is now named `zendrite-sample.yaml`.

## Admin API endpoints

All admin API endpoints changed prefix from `/_dendrite/` to `/_zendrite/`.
Update any scripts, monitoring, or tooling that calls these endpoints.

For example:

- `/_dendrite/admin/evacuateRoom/{roomID}` → `/_zendrite/admin/evacuateRoom/{roomID}`
- `/_dendrite/admin/registrationTokens` → `/_zendrite/admin/registrationTokens`

## JetStream topic prefix

The default `topic_prefix` in the JetStream config changed from `Dendrite` to `Zendrite`.
If you are using the built-in NATS server (the default), the existing NATS data stored under the old prefix will be ignored.

You have two options:

1. **Fresh start (recommended):** Delete the old JetStream data directory and let Zendrite recreate the streams.
   Pending events in the old streams will be lost, but the streams will be recreated from the database on startup.
2. **Keep old prefix:** Set `topic_prefix: Dendrite` in your config file to continue using existing streams.

## Docker

The Docker image user and paths changed:

- Container user: `dendrite` → `zendrite`
- Config volume: `/etc/dendrite` → `/etc/zendrite`
- Binary path: `/usr/bin/dendrite` → `/usr/bin/zendrite`

Update any Docker Compose files or container orchestration configs that mount volumes or reference these paths.

## Environment variables

The `DENDRITE_TRACE_HTTP` environment variable has been renamed to `ZENDRITE_TRACE_HTTP`.
The old name is still accepted for backwards compatibility.

## Database

The database schema is automatically migrated on first startup.
Zendrite renames the internal `dendrite_version` column in the `db_migrations` table to `zendrite_version` — no manual intervention is required.
