---
title: MatrixRTC (voice and video calls)
description: Advertising a LiveKit focus server to clients so that Element Call works against Zendrite
---

MatrixRTC ([MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143)) is the transport used by Element Call for voice and video.
The media itself never passes through Zendrite: clients connect to a [LiveKit](https://livekit.io/) SFU, and obtain a token for it from [lk-jwt-service](https://github.com/element-hq/lk-jwt-service).
Zendrite's only job is to tell clients where that service lives.

You therefore need a working LiveKit SFU and lk-jwt-service deployment before any of this has an effect.
Their own documentation covers that part; this page only covers the Zendrite side.

## Configuration

Add each focus server to the `rtc_foci` list in the `global` section of the configuration file:

```yaml
global:
  # ...
  rtc_foci:
    - type: livekit
      livekit_service_url: "https://matrix.example.com/livekit/jwt"
```

`livekit_service_url` is the URL of **lk-jwt-service**, not of the LiveKit SFU.
Clients discover the SFU itself from the token that service hands out.

Zendrite validates the list at startup and refuses to start if a `livekit` focus has no service URL.

## What clients see

With at least one focus configured, Zendrite exposes it in two places:

- `/.well-known/matrix/client` gains an `org.matrix.msc4143.rtc_foci` key.
- `GET /_matrix/client/unstable/org.matrix.msc4143/rtc/transports` returns the same list to authenticated clients.

Both use the flat wire format that clients expect:

```json
"org.matrix.msc4143.rtc_foci": [
  {
    "type": "livekit",
    "livekit_service_url": "https://matrix.example.com/livekit/jwt"
  }
]
```

`GET /_matrix/client/versions` also advertises `org.matrix.msc4143` in `unstable_features`, but only while at least one focus is configured.
Without `rtc_foci`, none of the above is served and clients have no focus to discover.

## Call membership

Element Call announces who is in a call with `m.rtc.member` events that it sends as sticky events ([MSC4354](https://github.com/matrix-org/matrix-spec-proposals/pull/4354)).
Zendrite supports them without configuration and advertises `org.matrix.msc4354` in `unstable_features`, which Element Call checks before using them.
See [sticky events](/mscs/#sticky-events) for how they are delivered.

## Delayed events

Element Call keeps a "leave" call membership event scheduled on the server with [MSC4140](https://github.com/matrix-org/matrix-spec-proposals/pull/4140) delayed events, and restarts its timer every few seconds while the call runs.
If the client crashes or loses its connection, the server sends the leave event when the timer runs out, and the user drops out of the call for everyone else.
Without delayed events, such a user stays in the call until their membership expires.

Delayed events are disabled by default, as in Synapse.
Enable them by setting a maximum delay in the `client_api` section:

```yaml
client_api:
  delayed_events:
    max_event_delay_duration: 24h
    max_delayed_events_per_user: 100
    management_rate_limiting:
      enabled: true
      threshold: 20
      cooloff_ms: 20000
```

`max_event_delay_duration` is the longest delay a client may request; `0s` disables delayed events.
`max_delayed_events_per_user` caps how many events one user may have scheduled at once, and `0` also disables them.
`management_rate_limiting` limits the requests that restart, cancel, send or look up delayed events, per device or, for requests without an access token, per address.
A request takes one of `threshold` slots for `cooloff_ms` milliseconds, so the defaults above allow a burst of 20 requests and then one per second, which is what Element Call's documentation suggests for Synapse.
Scheduling an event is limited by the general `client_api.rate_limiting` instead.

See [MSC support](/mscs/#delayed-events) for the endpoints and their behaviour.

## Deprecated nested configuration

Earlier Zendrite versions documented a nested spelling:

```yaml
global:
  rtc_foci:
    - type: livekit
      livekit:
        service_url: "https://matrix.example.com/livekit/jwt"
```

That form is still accepted and produces the same flat output, so existing configurations keep working.
New deployments should use `livekit_service_url`.
