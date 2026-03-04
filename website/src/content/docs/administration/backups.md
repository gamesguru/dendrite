---
title: Backups
description: What to back up for a Zendrite homeserver
---

This page lists the persistent data that should be included in your backup strategy.
The same list is useful when migrating Zendrite to a new server.

## Configuration

- **Server signing key** (`matrix_key.pem` by default, controlled by the `global.private_key` setting).
  Losing this key means other servers can no longer verify events your server has already signed.
  **This is the most critical item to preserve.**
  If you have rotated keys, also back up any files listed under `global.old_private_keys`.
- **Zendrite configuration file** (`zendrite.yaml` or equivalent).

## Database

Backup the database of your choice using a dedicated backup tool for your database type.

## Persistent directories

- **JetStream storage** — the directory set by `global.jetstream.storage_path` (default `./`).
  This contains NATS JetStream streams used for inter-component messaging.
  Without it, some in-flight events may be lost on restore.
- **Media store** — the directory set by `media_api.base_path` (default `./media_store`).
  Contains all uploaded and cached remote media.
- **Search index** — the directory set by `search.index_path` (default `./searchindex`).
  This can be regenerated from the database by doing a full re-index, so backing it up is optional but saves time on restore.
