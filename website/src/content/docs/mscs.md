---
title: MSC Support
description: Implementation status of Matrix Spec Changes (MSCs) in Dendrite.
---

[MSCs](https://spec.matrix.org/proposals/) (Matrix Spec Changes) are proposals to extend or modify the Matrix protocol.
This page tracks the implementation status of notable MSCs in Dendrite.

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
| [MSC3861](https://github.com/matrix-org/matrix-spec-proposals/pull/3861) | Next-gen auth (OIDC) | Not implemented |

## VoIP

| MSC | Title | Status |
| --- | --- | --- |
| [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) | MatrixRTC | Not implemented |

## Opt-in MSCs

The following MSCs are not enabled by default and must be activated in the `mscs` section of the config file:

```yaml
mscs:
  mscs:
    - msc2836
    - msc2444
    - msc2753
    - msc4115
```
