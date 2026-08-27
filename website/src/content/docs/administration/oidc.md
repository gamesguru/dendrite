---
title: Delegated authentication (OIDC)
description: Delegating login, registration and account management to an OpenID Connect provider such as Matrix Authentication Service
---

Zendrite can hand authentication over to an external OpenID Connect (OIDC) provider instead of managing passwords itself.
This is the [MSC3861](https://github.com/matrix-org/matrix-spec-proposals/pull/3861) "next-generation auth" model, and it is what [Matrix Authentication Service (MAS)](https://github.com/element-hq/matrix-authentication-service) implements.

Once enabled, Zendrite stops being an identity store.
It validates every access token by asking the provider about it (RFC 7662 token introspection), and creates local accounts and devices on demand from the answer.

This is an opt-in feature and is disabled by default.

## Should you enable this?

Enable it if you want single sign-on across services, or if you need features that only exist in the OAuth 2.0 world, such as centrally revocable sessions and per-device scopes.

Do not enable it on an existing server with password accounts without reading [Migrating an existing server](#migrating-an-existing-server) first.
There is no automatic linking between existing local accounts and OIDC identities, and getting this wrong strands your users.

## Before you start

- A working OIDC provider that supports token introspection. MAS is the reference choice and the only one tested end to end.
- HTTPS on both the homeserver and the provider. Plain HTTP issuers are rejected at startup unless the host is `localhost`, `127.0.0.1` or `::1`.
- The public base URL of your homeserver, for example `https://matrix.example.com`.

## Step 1: register Zendrite as a client at the provider

Zendrite needs a confidential OAuth 2.0 client so it can introspect tokens.
With MAS, add it to the `clients` section of `config.yaml`:

```yaml
clients:
  - client_id: 0000000000000000000ZENDRITE
    client_auth_method: client_secret_basic
    client_secret: "a-long-random-secret"
    redirect_uris:
      - https://matrix.example.com/_matrix/client/v3/login/sso/callback
```

The `redirect_uris` entry is only needed if you want the legacy SSO compatibility layer, which lets clients without native OIDC support log in.
See [Client support](#client-support).
It must exactly match the callback URL Zendrite advertises, which is `{public_base_url}/_matrix/client/v3/login/sso/callback` unless you override it with `sso_callback_url`.

MAS also needs to reach Zendrite's admin API, which it uses to provision users and devices:

```yaml
matrix:
  homeserver: example.com
  endpoint: "http://localhost:8008"
  secret: "another-long-random-secret"
```

That `matrix.secret` is the value you put in Zendrite's `admin_token`.
Leaking it grants full admin access to the homeserver, so treat it like a root password.

## Step 2: configure Zendrite

```yaml
mscs:
  mscs:
    - msc3861
  msc3861:
    issuer: "https://auth.example.com/"
    client_id: "0000000000000000000ZENDRITE"
    client_secret: "a-long-random-secret"
    admin_token: "another-long-random-secret"
    account_management_url: "https://auth.example.com/account"
    public_base_url: "https://matrix.example.com"
    sso_redirect_allowlist:
      - "https://app.example.com/"
      - "element://"
```

`issuer` must be byte-for-byte what the provider advertises as `issuer` in its discovery document at `{issuer}/.well-known/openid-configuration`.
With MAS that is `http.issuer`, which defaults to `http.public_base`.
If the two differ, Zendrite logs a warning, refuses to trust the document and falls back to config-derived endpoint defaults.

Everything else is discovered automatically from the provider.
You only need the endpoint overrides below if your provider does not publish a discovery document, or publishes one you need to deviate from.

### Configuration reference

| Field | Required | Description |
| --- | --- | --- |
| `issuer` | Yes | The OIDC provider URL. Must use `https`, except for loopback hosts during development. |
| `client_id` | Yes | OAuth 2.0 client ID registered with the provider. |
| `client_secret` | Yes | OAuth 2.0 client secret used for introspection. |
| `client_auth_method` | No | `client_secret_basic` (default) or `client_secret_post`. Must match the client's registration at the provider. |
| `admin_token` | No | Static bearer token granting admin access without introspection. Required for the MAS admin API and for confirmed cross-signing resets. |
| `account_management_url` | No | Where users manage their account. Advertised in `/.well-known/matrix/client` and as `account_management_uri` in the auth metadata. |
| `account_management_actions` | No | Actions the account management URL supports, e.g. `["org.matrix.profile", "org.matrix.devices_list"]`. |
| `public_base_url` | No | Public base URL of the homeserver. Used to build client-facing URLs instead of trusting the request's `Host` header. Strongly recommended behind a reverse proxy. |
| `sso_redirect_allowlist` | No | `redirectUrl` targets accepted by the legacy SSO flow. See [Client support](#client-support). |
| `sso_callback_url` | No | Overrides the SSO callback URL. Only needed if it is not `{public_base_url}/_matrix/client/v3/login/sso/callback`. |
| `introspection_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/oauth2/introspect`. |
| `userinfo_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/userinfo`. |
| `authorization_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/oauth2/auth`. |
| `token_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/oauth2/token`. |
| `registration_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/oauth2/clients/register`. |
| `revocation_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/oauth2/revoke`. |
| `device_authorization_endpoint` | No | Overrides discovery. Defaults to the discovered value, then `{issuer}/oauth2/device/auth`. |
| `end_session_endpoint` | No | Advertised to clients for RP-initiated logout. Defaults to the discovered value. |
| `jwks_uri` | No | Upstream JWKS to proxy. Defaults to the discovered value, then `{issuer}/oauth2/keys`. |

## Step 3: reverse proxy

Set `public_base_url`.
Without it, Zendrite builds client-facing URLs from the incoming `Host` and `X-Forwarded-Proto` headers, which means a forged `Host` header can poison what gets advertised in cached auth metadata.

Make sure your proxy sets `X-Forwarded-Proto` correctly, and that it does not strip cookies on `/_matrix/client/v3/login/sso/*`.
The legacy SSO flow uses an `HttpOnly`, `SameSite=Lax` cookie to bind a login to the browser that started it.

## Step 4: verify

Restart Zendrite and check the discovery surface:

```sh
# Should list the issuer and the account management URL
curl -s https://matrix.example.com/.well-known/matrix/client | jq .m\\.authentication

# Should return the issuer
curl -s https://matrix.example.com/_matrix/client/v1/auth_issuer

# Should return the full authorization server metadata
curl -s https://matrix.example.com/_matrix/client/v1/auth_metadata

# Should return the provider's signing keys, proxied by Zendrite
curl -s https://matrix.example.com/_matrix/client/v1/auth_metadata/jwks
```

The JWKS endpoint is a homeserver-side proxy of the provider's keys.
It exists so browser clients can fetch them without depending on the provider's CORS policy.
Responses are cached for 15 minutes, and a stale copy is served if the provider becomes unreachable.

`/_matrix/client/versions` should now advertise `org.matrix.msc2965` and `org.matrix.msc2967` in `unstable_features`.

## Client support

Clients with native OIDC support, such as Element X, use the metadata above and talk to the provider directly.
Nothing else is required for them.

Clients without it, such as current Element Web and Element Desktop, use the legacy SSO compatibility layer that Zendrite provides:

1. The client opens `/_matrix/client/v3/login/sso/redirect?redirectUrl=...`.
2. Zendrite starts a PKCE authorization-code flow against the provider and sets a browser-binding cookie.
3. The provider redirects back to `/_matrix/client/v3/login/sso/callback`.
4. Zendrite exchanges the code, provisions the user, and redirects to `redirectUrl` with a `loginToken`.
5. The client redeems it via `POST /login` with `m.login.token` and receives the provider-issued access token, refresh token and device ID.

**You must configure `sso_redirect_allowlist` for this to work with a client hosted on a different origin.**
By default only redirect targets on the homeserver's own origin are accepted, and anything else is rejected with `M_INVALID_PARAM`.

This default is deliberate.
The login token handed to `redirectUrl` redeems into the provider-issued access *and refresh* tokens, so an unrestricted `redirectUrl` would let a single crafted link deliver a user's long-lived provider credentials to an attacker.
The browser-binding cookie does not help here, because the victim's own browser runs the whole flow and therefore holds the cookie.

Entries are matched as follows:

- `http` and `https` entries match on scheme, host and path prefix at a path boundary.
  `https://app.example.com` therefore does not match `https://app.example.com.evil.com/`, a different port, or `https://app.example.com@evil.example.com/`.
- Entries with any other scheme, such as the `element://` desktop deep link, are matched as plain string prefixes.

Keep the list as tight as you can.

## How accounts are provisioned

On the first token introspection for an unknown subject, Zendrite picks a localpart in this order:

1. The `email` claim, but **only** when the provider also reports `email_verified: true`. If the domain matches your server name, it is stripped, so `alice@example.com` becomes `@alice:example.com`.
2. The `username` claim.
3. The `sub` claim, for providers that expose no username.

The result is lowercased and sanitised to a valid Matrix localpart.
The mapping from the provider's `(issuer, sub)` to that localpart is then stored permanently, so a user's Matrix ID is stable even if they later change their email or display name at the provider.

If the derived localpart is already taken by an unrelated account, Zendrite falls back to the subject claim rather than attaching the OIDC identity to a pre-existing account.
If that is taken too, the login is refused instead of linking anything.

Device IDs come from the token's `urn:matrix:client:device:<id>` scope.
Tokens without a device scope share a fallback device called `OIDC`.

Accounts whose token carries the `urn:synapse:admin:*` scope are marked as server admins.

## Admin access

Setting `admin_token` does two things.

It registers the Synapse-compatible admin endpoints that MAS calls to manage users:

- `GET /_synapse/admin/v1/username_available`
- `GET|PUT /_synapse/admin/v1/users/{userId}`
- `PUT|DELETE /_synapse/admin/v1/users/{userId}/devices/{deviceId}`
- `DELETE /_synapse/admin/v1/users/{userId}/devices`
- `POST /_synapse/admin/v1/users/{userId}/_deactivate`
- `POST /_synapse/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia`

These are authenticated with the admin token as a bearer token, not with a user access token.
If `admin_token` is unset the routes are not registered at all.

It also lets the token itself be presented as an access token, which resolves to an admin device for `@admin:{server_name}`.
This is intended for service-to-service calls.

## Cross-signing resets

Replacing a user's cross-signing keys normally requires user-interactive auth, which under delegated authentication cannot be a password check.
Zendrite uses the `m.oauth` UIA type and deep-links the user to the account management UI.

When `admin_token` is configured, the reset only completes after the provider confirms it by calling `_allow_cross_signing_replacement_without_uia`.
The confirmation is single-use and valid for 10 minutes.

When `admin_token` is not configured there is no way for the provider to confirm anything, so Zendrite falls back to accepting a resubmitted challenge and logs a warning.
That fallback is materially weaker: anyone holding a valid access token can reset cross-signing keys.
Configure `admin_token`.

## What changes when this is enabled

| Area | Behaviour |
| --- | --- |
| `POST /login` | Only `m.login.token` is accepted. Everything else returns `403 M_FORBIDDEN`. |
| `GET /login` | Advertises `m.login.sso` (marked `oauth_aware_preferred`) and `m.login.token`. |
| `POST /register` | `403 M_FORBIDDEN`, except application-service registrations, which still work. |
| `/register/available` | `403 M_FORBIDDEN`. |
| `/account/password`, `/account/deactivate` | `403 M_FORBIDDEN`. Handled at the provider. |
| `/account/3pid/delete`, 3PID `requestToken` | `403 M_FORBIDDEN`. |
| `/logout`, `/logout/all`, `/delete_devices`, device modification | `403 M_FORBIDDEN`. Sessions are managed at the provider. |
| `/auth/{authType}/fallback/web` | `403 M_FORBIDDEN`. |
| `GET /capabilities` | Reports `m.change_password` and `m.3pid_changes` as disabled. |
| `POST /refresh` | Bridges to the provider's refresh token grant. |
| Expired tokens | `401 M_UNKNOWN_TOKEN` with `soft_logout: true`, so clients refresh instead of logging out. |
| `/.well-known/matrix/client` | Gains an `m.authentication` section with the issuer and account management URL. |

Introspection results are cached for 30 seconds, so a token revoked at the provider may keep working for up to that long.
Failed introspections are cached for 5 seconds.

## Migrating an existing server

Enabling MSC3861 on a server that already has password accounts is a one-way door, and Zendrite will not link the two worlds for you.

- Existing password sessions stop working. Every user has to log in again through the provider.
- An existing local account is **not** adopted by an OIDC identity that happens to derive the same localpart. Zendrite deliberately refuses to attach an OIDC subject to an unrelated pre-existing account, and falls back to a subject-derived localpart instead. In practice `@alice:example.com` will not become the OIDC user `alice` unless you seed the mapping yourself.
- Deactivated accounts are refused, rather than being silently reactivated by a matching token.

Plan the identity mapping before you flip this on, and test it on a staging server.

## Known limitations

The legacy SSO compatibility layer keeps its in-flight state in process memory: SSO sessions, the login tokens issued by the callback, and pending cross-signing reset confirmations.
The `/login/sso/redirect`, `/login/sso/callback` and `POST /login` requests belonging to one login must therefore all reach the same Zendrite process.
Running several instances behind a load balancer breaks the flow unless the balancer pins a client's requests to one instance.

Clients with native OIDC support are unaffected, since they never touch the compatibility layer.

## Troubleshooting

**Every device shows up as `OIDC`.**
The token carried no `urn:matrix:client:device:` scope, or introspection failed and Zendrite fell back to the UserInfo endpoint, which returns no scope.
Check the logs for an error about rejected client credentials.

**Logs show introspection falling back to userinfo.**
The provider rejected Zendrite's client credentials.
Check `client_id`, `client_secret` and that `client_auth_method` matches the client's registration.
Authentication still works in this state, but device IDs and admin scope are lost, so do not leave it running like this.

**SSO login fails with `M_INVALID_PARAM` mentioning `sso_redirect_allowlist`.**
The client's `redirectUrl` is not on the allowlist and is not on the homeserver's own origin.
See [Client support](#client-support).

**Logs warn that the discovered issuer does not match the configured issuer.**
`issuer` differs from what the provider advertises, commonly by a trailing slash or an `http` versus `https` mismatch.
Zendrite falls back to config-derived endpoints, which usually still work but skips discovery entirely.

**Clients are logged out instead of refreshing.**
Check that the provider returns a refresh token, and that the client actually supports `POST /refresh`.

**Admin endpoints return `403`.**
`admin_token` is unset, so the MAS admin routes were never registered.
