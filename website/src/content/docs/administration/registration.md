---
title: Enabling registration
description: Configuring user registration and reCAPTCHA verification
---

Enabling registration allows users to register their own user accounts on your Zendrite server using their Matrix client.
They will be able to choose their own username and password and log in.

Registration is controlled by the `registration_disabled` field in the `client_api` section of the configuration.
By default, `registration_disabled` is set to `true`, disabling registration.
If you want to enable registration, you should change this setting to `false`.

Zendrite supports several CAPTCHA providers for secondary verification: [reCAPTCHA](https://www.google.com/recaptcha/about/), [hCaptcha](https://www.hcaptcha.com/), and [ALTCHA](https://altcha.org/) (a self-hosted proof-of-work solution).

## reCAPTCHA verification

Zendrite supports reCAPTCHA as a secondary verification method.
If you want to enable registration, it is **highly recommended** to configure reCAPTCHA.
This will make it much more difficult for automated spam systems from registering accounts on your homeserver automatically.

You will need an API key from the [reCAPTCHA Admin Panel](https://www.google.com/recaptcha/admin).
Then configure the relevant details in the `client_api` section of the configuration:

```yaml
client_api:
  # ...
  registration_disabled: false
  recaptcha_public_key: "PUBLIC_KEY_HERE"
  recaptcha_private_key: "PRIVATE_KEY_HERE"
  enable_registration_captcha: true
  captcha_bypass_secret: ""
  recaptcha_siteverify_api: "https://www.google.com/recaptcha/api/siteverify"
```

## ALTCHA verification

[ALTCHA](https://altcha.org/) is an open-source, self-hosted CAPTCHA that uses proof-of-work instead of external API calls.
The server generates a cryptographic challenge, the client solves it by brute-force hashing, and the server verifies the solution locally using HMAC.
This means there are no third-party dependencies or privacy concerns.

First, generate a random HMAC key (at least 32 characters):

```bash
openssl rand -hex 32
```

Then configure the `client_api` section:

```yaml
client_api:
  # ...
  registration_disabled: false
  enable_registration_captcha: true
  captcha_provider: altcha
  altcha_hmac_key: "your-generated-hmac-key"
  altcha_max_number: 50000    # proof-of-work difficulty (default 50000)
  altcha_expiry: "5m"         # how long a challenge is valid (default 5m)
```

When using ALTCHA, you do not need to set `recaptcha_public_key`, `recaptcha_private_key`, or `recaptcha_siteverify_api`.

## Open registration

Zendrite does support open registration — that is, allowing users to create their own user accounts without any verification or secondary authentication.
However, it is **not recommended** to enable open registration, as this leaves your homeserver vulnerable to abuse by spammers or attackers, who create large numbers of user accounts on Matrix homeservers in order to send spam or abuse into the network.

It isn't possible to enable open registration in Zendrite in a single step.
If you try to disable the `registration_disabled` option without any secondary verification methods enabled (such as reCAPTCHA), Zendrite will log an error and fail to start.

## Call guests

Call guests let people without a Matrix account join calls on your homeserver by link, while normal registration stays closed.
They are meant for the standalone [Element Call](https://github.com/element-hq/element-call) app: when someone opens a call link without an account, Element Call registers a throwaway account with a random username and password, sets the display name the person typed, and joins the call's room.
With call guests enabled, such sign-ups succeed, but only as restricted call guest accounts that expire after a while.

Enable them in the `client_api` section:

```yaml
client_api:
  # ...
  # Required: every open sign-up becomes a call guest.
  registration_disabled: true
  call_guests:
    enabled: true
    # How long a call guest account lives, as a Go duration.
    lifetime: 24h
```

Call guests require `registration_disabled: true`, and Zendrite refuses to start otherwise.
While they are enabled, every account created through `/register` with `m.login.dummy`, or with the CAPTCHA if one is configured, is a call guest.
Call guest sign-up does not need a CAPTCHA or registration token, so the open registration check described below does not apply to it.
Accounts created with a registration token, the shared secret or an application service are normal accounts, as before.
A virtual host with `allow_registration: true` keeps creating normal accounts.
Call guests have nothing to do with the guest accounts of `kind=guest` registration, which `guests_disabled` controls.

Zendrite ignores the username a call guest asks for and generates the localpart itself: `call-guest-` followed by 12 random lowercase letters and digits, such as `@call-guest-4k2m9x0qa7bz:example.com`.
Call guests are therefore recognisable by the `call-guest-` prefix, and cannot take a username a member might want or one that looks trustworthy.
No other account may use the prefix: registration with a token, the shared secret or open registration refuses a `call-guest-` username, so a member can never look like a call guest.
Application service namespaces are unaffected.
The display name a call guest sets, which Element Call asks for, is shown as usual.

### What call guests can do

A call guest can do what a call needs and little else:

- sync, and read the messages, state and members of the rooms it joined;
- join a room by ID or alias, but only one this homeserver is already in, and only where the room's join rules let it in, so public rooms or rooms a member invited it to;
- leave and forget rooms;
- send messages, reactions, redactions and call membership events, and schedule [delayed events](/administration/matrixrtc/#delayed-events), as the room's power levels allow;
- change its own room membership, for example its display name in a room;
- upload, query and claim end-to-end encryption keys and send to-device messages;
- discover the MatrixRTC transports and request an OpenID token, which lk-jwt-service needs to admit it to the call;
- set its display name, manage its own devices, filters, account data, receipts, read markers and typing notifications, read its push rules, download media, and deactivate its own account.

### What call guests cannot do

Every other authenticated endpoint answers a call guest with `403 M_FORBIDDEN`.
In particular, a call guest cannot:

- create rooms, which includes direct messages;
- invite, kick, ban or unban anyone, or knock on a room;
- send `m.room.member` events about other users, or the state events that change who may join a room and what the room is: `m.room.create`, `m.room.power_levels`, `m.room.join_rules`, `m.room.history_visibility`, `m.room.guest_access`, `m.room.encryption`, `m.room.canonical_alias`, `m.room.aliases`, `m.room.third_party_invite`, `m.room.server_acl`, `m.room.tombstone`, `m.space.child` and `m.space.parent`, even where the room's power levels would allow it;
- join a room over federation, or resolve a room alias of another server;
- search the user directory, upload media, change room aliases or the room directory, upgrade rooms or report content;
- change its avatar or other profile fields, its password or its third-party identifiers, or set up key backup and pushers;
- use the admin APIs.

A call guest is not added to `auto_join_rooms`.

### Expiry

Every ten minutes, Zendrite deactivates the call guests whose accounts are older than `lifetime`.
Deactivation is the same as when a user deactivates their own account: the call guest leaves all its rooms, and its devices, pushers and delayed events are deleted.
A call that is still running when the account expires ends for that guest.
The sweep also runs while `call_guests.enabled` is `false`, so turning call guests off still ends the accounts created before.
Under [high availability](/administration/high-availability/), only the active replica runs it.

### Element Call and lk-jwt-service

Point the standalone Element Call app at your homeserver as its default server, and make sure [MatrixRTC](/administration/matrixrtc/) is set up, including delayed events.
Element Call creates call rooms with the `public_chat` preset, unlisted in the room directory, so a call guest can join them from the call link.

lk-jwt-service checks the OpenID token of every participant with their homeserver.
Call guests are local users of your homeserver, so if your homeserver is listed in lk-jwt-service's `LIVEKIT_FULL_ACCESS_HOMESERVERS`, call guests get full access too.
That means a call guest can not only join a call that is running, but also start one in any public room on your homeserver that it joins.

Call guests do not apply when [authentication is delegated](/administration/oidc/), because registration then happens in the authentication service.
Before you roll back to a Zendrite version without call guests, disable them and let the existing call guests expire, because older versions treat call guest accounts as normal users.
