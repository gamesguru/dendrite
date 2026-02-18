---
title: FAQ
description: Frequently asked questions about Zendrite
---

{% aside type="caution" title="Important" %}
Most of the content originated from [element-hq/zendrite](https://github.com/element-hq/zendrite) and might not be up-to-date anymore with respect to the many changes of this fork.
Please use with caution and/or report drifts in an issue/Pull Request.
{% /aside %}

## Why does Zendrite exist?

Zendrite aims to provide a matrix compatible server that has low resource usage compared to [Synapse](https://github.com/matrix-org/synapse).
It also aims to provide more flexibility when scaling either up or down.
Zendrite's code is also very easy to hack on which makes it suitable for experimenting with new matrix features such as peer-to-peer.

## Is Zendrite stable?

Mostly, although there are still bugs and missing features.
If you are a confident power user and you are happy to spend some time debugging things when they go wrong, then please try out Zendrite.

## Is Zendrite feature-complete?

No, although a good portion of the Matrix specification has been implemented.

## Is there a migration path from Synapse to Zendrite?

No, not at present.

## Can I use Zendrite with an existing Synapse database?

No, Zendrite has a very different database schema to Synapse and the two are not interchangeable.

## Can I configure which port Zendrite listens on?

Yes, use the cli flag `-http-bind-address`.

## I've installed Zendrite but federation isn't working

Check the [Federation Tester](https://federationtester.matrix.org).
You need at least:

- A valid DNS name
- A valid TLS certificate for that DNS name
- Either DNS SRV records or well-known files

## Whenever I try to connect from Element it says unable to connect to homeserver

Check that your zendrite instance is running.
Otherwise this is most likely due to a reverse proxy misconfiguration.

## Does Zendrite work with my favourite client?

It should do, although we are aware of some minor issues:

- **Element Android**: registration does not work, but logging in with an existing account does.
- **Hydrogen**: occasionally sync can fail due to gaps in the `since` parameter, but clearing the cache fixes this.

## Is there a public instance of Zendrite I can try out?

The Matrix.org Foundation runs [zendrite.matrix.org](https://zendrite.matrix.org).

{% aside type="caution" title="Important" %}
This instance is based on [element-hq/zendrite](https://github.com/element-hq/zendrite) and is not a valid test instance for this fork!
{% /aside %}

## Does Zendrite support Space Summaries?

Yes

## Does Zendrite support Threads?

Yes, to enable them [msc2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836) would need to be added to mscs configuration in order to support Threading.
Other MSCs are not currently supported.

```yaml
mscs:
  mscs:
    - msc2836
```

Please note that MSCs should be considered experimental and can result in significant usability issues when enabled.
If you'd like more details on how MSCs are ratified or the current status of MSCs, please see the [Matrix specification documentation](https://spec.matrix.org/proposals/) on the subject.

## Does Zendrite support push notifications?

Yes, Zendrite supports push notifications.
Configure them in the usual way in your Matrix client.

## Does Zendrite support application services/bridges?

Possibly - Zendrite does have some application service support but it is not well tested.
Please let us know by raising a issue if you try it and run into problems.

Bridges known to work:

- [Telegram](https://docs.mau.fi/bridges/python/telegram/index.html)
- [WhatsApp](https://docs.mau.fi/bridges/go/whatsapp/index.html)
- [Signal](https://docs.mau.fi/bridges/python/signal/index.html)
- [probably all other mautrix bridges](https://docs.mau.fi/bridges/)

Remember to add the config file(s) to the `app_service_api` section of the config file.

## Is it possible to prevent communication with the outside world?

Yes, you can do this by disabling federation - set `disable_federation` to `true` in the `global` section of the Zendrite configuration file.

## How can I migrate a room in order to change the internal ID?

This can be done by performing a room upgrade.
Use the command `/upgraderoom <version>` in Element to do this.

## How do I reset somebody's password on my server?

Use the admin endpoint [resetpassword](/administration/adminapi#post-_zendriteadminresetpassworduserid)

## Should I use PostgreSQL or SQLite for my databases?

Please use PostgreSQL wherever possible, especially if you are planning to run a homeserver that caters to more than a couple of users.

## What data needs to be kept if transferring/backing up Zendrite?

The list of files that need to be stored is:

- `matrix-key.pem`
- `zendrite.yaml`
- the postgres or sqlite DB
- the jetstream directory
- the media store
- the search index (although this can be regenerated)

Note that this list may be out of date.
We don't officially maintain instructions for migrations like this.

## How can I prepare enough storage for media caches?

This might be what you want: [matrix-media-repo](https://github.com/turt2live/matrix-media-repo)
We don't officially support this or any other dedicated media storage solutions.

## Zendrite is using a lot of CPU

Generally speaking, you should expect to see some CPU spikes, particularly if you are joining or participating in large rooms.
However, constant/sustained high CPU usage is not expected - if you are experiencing that, please open an issue and let us know what you were doing when the CPU usage shot up.
If you can take a [CPU profile](/development/profiling) then that would be a huge help too, as that will help us to understand where the CPU time is going.

## Zendrite is using a lot of RAM

As above with CPU usage, some memory spikes are expected if Zendrite is doing particularly heavy work at a given instance.
However, if it is using more RAM than you expect for a long time, that's probably not expected.
If you can take a [memory profile](/development/profiling) then that would be a huge help too, as that will help us to understand where the memory usage is happening.

## Do I need to generate the self-signed certificate if I'm going to use a reverse proxy?

No, if you already have a proper certificate from some provider, like Let's Encrypt, and use that on your reverse proxy, and the reverse proxy does TLS termination, then you're good and can use HTTP to the zendrite process.

## Zendrite is running out of PostgreSQL database connections

You may need to revisit the connection limit of your PostgreSQL server and/or make changes to the `max_connections` lines in your Zendrite configuration.
Be aware that each Zendrite component opens its own database connections and has its own connection limit, even in monolith mode!

## VOIP and Video Calls don't appear to work on Zendrite

There is likely an issue with your STUN/TURN configuration on the server.
If you believe your configuration to be correct, please see the [troubleshooting](/administration/troubleshooting) for troubleshooting recommendations.

## What is being reported when enabling phone-home statistics?

Phone-home statistics contain your server's domain name, some configuration information about your deployment and aggregated information about active users on your deployment.
They are sent to the endpoint URL configured in your Zendrite configuration file only.
The following is an example of the data that is sent:

```json
{
  "cpu_average": 0,
  "daily_active_users": 97,
  "daily_e2ee_messages": 0,
  "daily_messages": 0,
  "daily_sent_e2ee_messages": 0,
  "daily_sent_messages": 0,
  "daily_user_type_bridged": 2,
  "daily_user_type_native": 97,
  "database_engine": "Postgres",
  "database_server_version": "11.14 (Debian 11.14-0+deb10u1)",
  "federation_disabled": false,
  "go_arch": "amd64",
  "go_os": "linux",
  "go_version": "go1.16.13",
  "homeserver": "my.domain.com",
  "log_level": "trace",
  "memory_rss": 93452,
  "monolith": true,
  "monthly_active_users": 97,
  "nats_embedded": true,
  "nats_in_memory": true,
  "num_cpu": 8,
  "num_go_routine": 203,
  "r30v2_users_all": 0,
  "r30v2_users_android": 0,
  "r30v2_users_electron": 0,
  "r30v2_users_ios": 0,
  "r30v2_users_web": 0,
  "timestamp": 1651741851,
  "total_nonbridged_users": 97,
  "total_room_count": 0,
  "total_users": 99,
  "uptime_seconds": 30,
  "version": "0.8.2"
}
```
