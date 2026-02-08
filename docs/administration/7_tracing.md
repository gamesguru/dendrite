---
title: Tracing
parent: Administration
has_toc: true
nav_order: 7
permalink: /administration/tracing
---

Dendrite supports distributed tracing using [OpenTelemetry](https://opentelemetry.io/).
Traces are exported via the OTLP gRPC protocol to any compatible backend, such as [Grafana Tempo](https://grafana.com/oss/tempo/), [Jaeger](https://www.jaegertracing.io/), or [SigNoz](https://signoz.io/).

## Configuration

Add or update the `tracing` section in your Dendrite configuration file:

```yaml
tracing:
  enabled: true
  endpoint: localhost:4317
  insecure: true
```

| Field      | Type   | Description                                                    |
|------------|--------|----------------------------------------------------------------|
| `enabled`  | bool   | Set to `true` to enable tracing. Default: `false`.             |
| `endpoint` | string | OTLP gRPC endpoint address, e.g. `localhost:4317`.             |
| `insecure` | bool   | Use an insecure (non-TLS) gRPC connection. Default: `false`.   |

When `enabled` is `false` (the default), no tracing infrastructure is initialised and there is no performance overhead.

## Example: Grafana Tempo

Run Tempo locally with Docker:

```bash
docker run -d --name tempo \
  -p 3200:3200 \
  -p 4317:4317 \
  grafana/tempo:latest \
  -config.file=/etc/tempo/tempo.yaml
```

Then configure Dendrite to send traces to it:

```yaml
tracing:
  enabled: true
  endpoint: localhost:4317
  insecure: true
```

Consult the [Tempo documentation](https://grafana.com/docs/tempo/latest/) for full setup instructions, including connecting Grafana for trace visualization.

## Migrating from Jaeger/OpenTracing

Previous versions of Dendrite used the now-deprecated Jaeger client library with OpenTracing.
If you are upgrading from that setup, you must update your configuration file.

The old format:

```yaml
tracing:
  enabled: true
  jaeger:
    serviceName: "Dendrite"
    sampler:
      type: const
      param: 1
    reporter:
      localAgentHostPort: "localhost:6831"
```

Must be replaced with:

```yaml
tracing:
  enabled: true
  endpoint: localhost:4317
  insecure: true
```

The `endpoint` should point to whatever OTLP-compatible backend you are using.
If you are continuing to use Jaeger, ensure it accepts OTLP gRPC on port 4317 (supported since Jaeger v1.35, enabled by default in recent versions).
The old Jaeger agent UDP port (6831) is no longer used.
