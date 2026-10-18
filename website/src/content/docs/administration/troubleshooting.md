---
title: Troubleshooting
description: Common issues and debugging tips for Zendrite
---

If your Zendrite installation is acting strangely, there are a few things you should check before seeking help.

## 1. Logs

Zendrite, by default, will log all warnings and errors to stdout, in addition to any other locations configured in the `zendrite.yaml` configuration file.
Often there will be clues in the logs.

You can increase this log level to the more verbose `debug` level if necessary by adding this to the config and restarting Zendrite:

```yaml
logging:
  - type: std
    level: debug
```

Look specifically for lines that contain `level=error` or `level=warning`.

## 2. Federation tester

If you are experiencing problems federating with other homeservers, you should check that the [Federation Tester](https://federationtester.matrix.org) is passing for your server.

Common reasons that it may not pass include:

1. Incorrect DNS configuration;
2. Misconfigured DNS SRV entries or well-known files;
3. Invalid TLS/SSL certificates;
4. Reverse proxy configuration issues (if applicable).

Correct any errors if shown and re-run the federation tester to check the results.

## 3. System time

Matrix relies heavily on TLS which requires the system time to be correct.
If the clock drifts then you may find that federation no works reliably (or at all) and clients may struggle to connect to your Zendrite server.

Ensure that your system time is correct and consider syncing to a reliable NTP source.

## 4. Database connections

If you are using the PostgreSQL database, you should ensure that Zendrite's configured number of database connections does not exceed the maximum allowed by PostgreSQL.

Open your `postgresql.conf` configuration file and check the value of `max_connections` (which is typically `100` by default).
Then open your `zendrite.yaml` configuration file and ensure that in the `global.database` section, `max_open_conns` does not exceed that number.

## 5. File descriptors

Zendrite requires a sufficient number of file descriptors for every connection it makes to a remote server, every connection to the database engine and every file it is reading or writing to at a given time (media, logs etc).
We recommend ensuring that the limit is no lower than 65535 for Zendrite.

Zendrite will check at startup if there are a sufficient number of available descriptors.
If there aren't, you will see a log lines like this:

```text
level=warning msg="IMPORTANT: Process file descriptor limit is currently 65535, it is recommended to raise the limit for Zendrite to at least 65535 to avoid issues"
```

Follow the [Optimisation](/administration/optimisation) instructions to correct the available number of file descriptors.

## 6. STUN/TURN Server tester

If you are experiencing problems with VoIP or video calls, you should check that Zendrite is able to successfully connect your TURN server using [Matrix VoIP Tester](https://test.voip.librepush.net/).
This can highlight any issues that the server may encounter so that you can begin the troubleshooting process.

## 7. Delayed events with healthy probes

Zendrite processes the events of each component in order, one message at a time.
A single slow message, such as a state update for a room with tens of thousands of members, therefore delays every later event, including those of unrelated rooms.
Health probes and reads of existing history keep working throughout, so a delay of this kind does not show up as an unhealthy server.

The following Prometheus metrics expose it, labelled by the durable consumer name such as `ZendriteSyncAPIRoomServerConsumerPull`:

| Metric | Meaning |
| --- | --- |
| `zendrite_jetstream_consumer_processing_started_timestamp_seconds` | Unix time the consumer started its current message, or `0` while idle. |
| `zendrite_jetstream_consumer_processing_message_timestamp_seconds` | Unix publication time of the message being processed, or `0` while idle. |
| `zendrite_jetstream_consumer_processing_seconds` | Histogram of the time spent on each message. |
| `zendrite_jetstream_consumer_message_age_seconds` | Histogram of the time each message waited between publication and the start of its processing. |
| `zendrite_jetstream_consumer_pending_messages` | Messages queued behind the last one delivered. |
| `zendrite_jetstream_consumer_last_ack_timestamp_seconds` | Unix time of the last acknowledged message. |

The two histograms only record a message once it is finished, so use the timestamp gauges to see a message that is still being processed.
For example, this alert fires when a consumer has spent more than a minute on one message:

```yaml
- alert: ZendriteConsumerBusy
  expr: |
    zendrite_jetstream_consumer_processing_started_timestamp_seconds > 0
    and time() - zendrite_jetstream_consumer_processing_started_timestamp_seconds > 60
```

The sync API also logs `Wrote a large sync API state update` at the `info` level, with the time spent preparing and writing, for each room event that adds or removes at least 1000 state events.
