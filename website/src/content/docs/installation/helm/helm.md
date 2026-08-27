---
title: Helm Setup
description: Installing Zendrite on Kubernetes with Helm
---

Install Zendrite using the [devxy/helm-zendrite](https://codefloe.com/devxy/helm-zendrite) Helm chart.

## Installation

Using OCI:

```bash
helm install zendrite oci://codefloe.com/devxy/zendrite
```

Or using a classic Helm repository:

```bash
helm repo add codefloe.com https://codefloe.com/api/packages/devxy/helm
helm install zendrite codefloe.com/zendrite
```

## Configuration

Create a `values.yaml` file and configure it to your liking.
All possible values can be found in the [chart README](https://codefloe.com/devxy/helm-zendrite/src/branch/main/charts/zendrite/README.md), but at least you need to configure a `server_name`:

```yaml
zendrite_config:
  global:
    server_name: "localhost"
```

If you are going to use an existing Postgres database, you'll also need to configure this connection:

```yaml
zendrite_config:
  global:
    database:
      connection_string: "postgresql://PostgresUser:PostgresPassword@PostgresHostName/ZendriteDatabaseName"
      max_open_conns: 90
      max_idle_conns: 5
      conn_max_lifetime: -1
```

## Installing with PostgreSQL

The chart comes with a dependency on Postgres, which can be installed alongside Zendrite. Enable it in your `values.yaml`:

```yaml
postgresql:
  enabled: true # this installs Postgres
  primary:
    persistence:
      size: 1Gi # defines the size for $PGDATA

zendrite_config:
  global:
    server_name: "localhost"
```

Using this option, the `database.connection_string` will be set for you automatically.
