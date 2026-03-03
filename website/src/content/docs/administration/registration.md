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
