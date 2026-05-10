---
title: MSC Support
description: Implementation status of Matrix Spec Changes (MSCs) in Zendrite.
---

[MSCs](https://spec.matrix.org/proposals/) (Matrix Spec Changes) are proposals to extend or modify the Matrix protocol.
This page tracks the implementation status of notable MSCs in Zendrite.

Many MSCs listed here have since been merged into the [Matrix specification](https://spec.matrix.org/) proper.
They are listed by their original MSC number for reference.

## Sync

| MSC | Title | Status |
| --- | --- | --- |
| [MSC4186](https://github.com/matrix-org/matrix-spec-proposals/pull/4186) | Simplified Sliding Sync | Implemented (native, no proxy needed) |
| [MSC3575](https://github.com/matrix-org/matrix-spec-proposals/pull/3575) | Sliding Sync (v1) | Implemented (legacy, superseded by MSC4186) |

## Messaging

| MSC | Title | Status |
| --- | --- | --- |
| [MSC2675](https://github.com/matrix-org/matrix-spec-proposals/pull/2675) | Serverside aggregations of message relationships | Implemented |
| [MSC2676](https://github.com/matrix-org/matrix-spec-proposals/pull/2676) | Message editing | Implemented |
| [MSC2677](https://github.com/matrix-org/matrix-spec-proposals/pull/2677) | Reactions | Implemented |
| [MSC2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836) | Threading | Opt-in (`msc2836`) |
| [MSC2285](https://github.com/matrix-org/matrix-spec-proposals/pull/2285) | Private read receipts | Implemented |

## Rooms & Spaces

| MSC | Title | Status |
| --- | --- | --- |
| [MSC1772](https://github.com/matrix-org/matrix-spec-proposals/pull/1772) | Spaces | Implemented |
| [MSC2946](https://github.com/matrix-org/matrix-spec-proposals/pull/2946) | Spaces summary / room hierarchy | Implemented |
| [MSC3083](https://github.com/matrix-org/matrix-spec-proposals/pull/3083) | Restricted rooms (space-based membership) | Implemented |
| [MSC2403](https://github.com/matrix-org/matrix-spec-proposals/pull/2403) | Knocking | Implemented |
| [MSC3266](https://github.com/matrix-org/matrix-spec-proposals/pull/3266) | Room summary API | Implemented |
| [MSC3765](https://github.com/matrix-org/matrix-spec-proposals/pull/3765) | Rich text in room topics | Not implemented |

## Encryption & Security

| MSC | Title | Status |
| --- | --- | --- |
| [MSC3916](https://github.com/matrix-org/matrix-spec-proposals/pull/3916) | Authenticated media | Implemented |
| [MSC2732](https://github.com/matrix-org/matrix-spec-proposals/pull/2732) | OLM fallback keys | Implemented |
| [MSC3814](https://github.com/matrix-org/matrix-spec-proposals/pull/3814) | Dehydrated devices v2 | Opt-in (`msc3814`) |
| [MSC4115](https://github.com/matrix-org/matrix-spec-proposals/pull/4115) | Membership metadata on events | Opt-in (`msc4115`) |

## Federation

| MSC | Title | Status |
| --- | --- | --- |
| [MSC3706](https://github.com/matrix-org/matrix-spec-proposals/pull/3706) | Partial state in `/send_join` (faster joins) | Implemented |
| [MSC2444](https://github.com/matrix-org/matrix-spec-proposals/pull/2444) | Peeking over federation | Opt-in (`msc2444`) |
| [MSC2753](https://github.com/matrix-org/matrix-spec-proposals/pull/2753) | Peeking via `/sync` | Opt-in (`msc2753`) |

## Authentication

| MSC | Title | Status |
| --- | --- | --- |
| [MSC2918](https://github.com/matrix-org/matrix-spec-proposals/pull/2918) | Refresh tokens | Implemented |
| [MSC3861](https://github.com/matrix-org/matrix-spec-proposals/pull/3861) | Next-gen auth (OIDC) | Opt-in (`msc3861`) |

## VoIP

| MSC | Title | Status |
| --- | --- | --- |
| [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) | MatrixRTC transport discovery | Implemented |

## Opt-in MSCs

The following MSCs are not enabled by default and must be activated in the `mscs` section of the config file:

```yaml
mscs:
  mscs:
    - msc2836
    - msc2444
    - msc2753
    - msc3814
    - msc3861
    - msc4115
```

### MSC3814: Dehydrated Devices v2

MSC3814 allows clients to store a "dehydrated" device on the server so that encrypted to-device messages (e.g. key shares) can be delivered while the user is offline.
When the user signs in again, the client rehydrates the device and retrieves messages it missed.

**What changes when MSC3814 is enabled:**

- `PUT /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` — store a dehydrated device with its keys.
- `GET /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` — retrieve the current dehydrated device metadata.
- `DELETE /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` — remove the dehydrated device.
- `POST /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device/{deviceID}/events` — retrieve to-device events addressed to the dehydrated device.

**Configuration:**

```yaml
mscs:
  mscs:
    - msc3814
```

No additional configuration is needed beyond enabling the MSC.

### MSC3861: OIDC Delegated Authentication

MSC3861 delegates authentication to an external OpenID Connect (OIDC) provider such as [Matrix Authentication Service (MAS)](https://github.com/element-hq/matrix-authentication-service).
When enabled, Zendrite validates access tokens via OAuth 2.0 token introspection instead of managing passwords directly.

**What changes when MSC3861 is enabled:**

- Password-based registration and login are disabled.
- `GET /login` returns only the `m.login.sso` flow.
- `POST /login`, `/register`, `/account/password`, `/account/deactivate`, `/logout`, `/logout/all`, `/delete_devices`, and device modification endpoints return `403 M_FORBIDDEN`.
- `/.well-known/matrix/client` includes an `m.authentication` section with the OIDC issuer.
- New users are auto-provisioned on first token introspection.

**Configuration:**

```yaml
mscs:
  mscs:
    - msc3861
  msc3861:
    issuer: "https://auth.example.com/"
    client_id: "0000000000000000000ZENDRITE"
    client_secret: "secret"
    client_auth_method: "client_secret_basic"  # or "client_secret_post"
    admin_token: ""                            # optional: static token for admin API access
    account_management_url: ""                 # optional: URL for account management UI
    introspection_endpoint: ""                 # optional: defaults to {issuer}/oauth2/introspect
```

| Field | Required | Description |
| --- | --- | --- |
| `issuer` | Yes | The OIDC provider URL (e.g. your MAS instance). |
| `client_id` | Yes | OAuth 2.0 client ID registered with the OIDC provider for introspection. |
| `client_secret` | Yes | OAuth 2.0 client secret for introspection. |
| `client_auth_method` | No | Authentication method: `client_secret_basic` (default) or `client_secret_post`. |
| `admin_token` | No | A static bearer token that grants admin access, bypassing OIDC introspection. Useful for service-to-service calls. |
| `account_management_url` | No | URL where users can manage their account (shown in well-known response). |
| `introspection_endpoint` | No | Override the introspection endpoint URL. Defaults to `{issuer}/oauth2/introspect`. |
