package config

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/sirupsen/logrus"
)

type MSCs struct {
	Matrix *Global `yaml:"-"`

	// The MSCs to enable. Supported MSCs include:
	// 'msc2444': Peeking over federation - https://github.com/matrix-org/matrix-doc/pull/2444
	// 'msc2753': Peeking via /sync - https://github.com/matrix-org/matrix-doc/pull/2753
	// 'msc2836': Threading - https://github.com/matrix-org/matrix-doc/pull/2836
	// 'msc3814': Dehydrated devices v2 - https://github.com/matrix-org/matrix-spec-proposals/pull/3814
	// 'msc3861': OIDC delegated authentication - https://github.com/matrix-org/matrix-spec-proposals/pull/3861
	// 'msc4115': Membership metadata on events - https://github.com/matrix-org/matrix-spec-proposals/pull/4115
	MSCs []string `yaml:"mscs"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	// MSC3861 configuration for OIDC delegated authentication.
	MSC3861 MSC3861Config `yaml:"msc3861"`
}

// MSC3861Config holds configuration for delegated OIDC authentication via MSC3861.
type MSC3861Config struct {
	// Issuer is the OIDC provider URL (e.g. the Matrix Authentication Service URL).
	// It must use https, except for localhost issuers during development.
	Issuer string `yaml:"issuer"`

	// ClientID is the OAuth 2.0 client ID used for token introspection.
	ClientID string `yaml:"client_id"`

	// ClientSecret is the OAuth 2.0 client secret used for token introspection.
	ClientSecret string `yaml:"client_secret"`

	// ClientAuthMethod is the authentication method used for introspection.
	// Supported values: "client_secret_basic" (default), "client_secret_post".
	ClientAuthMethod string `yaml:"client_auth_method"`

	// AdminToken is a static bearer token that grants admin-level access,
	// bypassing OIDC introspection. Useful for internal service-to-service calls.
	AdminToken string `yaml:"admin_token"`

	// AccountManagementURL is a URL to redirect users for account management
	// (e.g. password changes). Included in the well-known response.
	AccountManagementURL string `yaml:"account_management_url"`

	// IntrospectionEndpoint overrides the OIDC introspection endpoint URL.
	// If empty, defaults to {Issuer}/oauth2/introspect.
	IntrospectionEndpoint string `yaml:"introspection_endpoint"`

	// UserinfoEndpoint overrides the OIDC UserInfo endpoint URL.
	// If empty, defaults to {Issuer}/userinfo.
	UserinfoEndpoint string `yaml:"userinfo_endpoint"`

	// AuthorizationEndpoint overrides the OIDC authorization endpoint URL.
	// If empty, defaults to {Issuer}/oauth2/auth.
	AuthorizationEndpoint string `yaml:"authorization_endpoint"`

	// TokenEndpoint overrides the OIDC token endpoint URL.
	// If empty, defaults to {Issuer}/oauth2/token.
	TokenEndpoint string `yaml:"token_endpoint"`

	// RegistrationEndpoint overrides the OIDC dynamic client registration endpoint URL.
	// If empty, defaults to {Issuer}/oauth2/clients/register.
	RegistrationEndpoint string `yaml:"registration_endpoint"`

	// RevocationEndpoint overrides the OIDC token revocation endpoint URL.
	// If empty, defaults to {Issuer}/oauth2/revoke.
	RevocationEndpoint string `yaml:"revocation_endpoint"`

	// EndSessionEndpoint overrides the OIDC end session (RP-initiated logout)
	// endpoint URL advertised to clients.
	EndSessionEndpoint string `yaml:"end_session_endpoint"`

	// DeviceAuthorizationEndpoint overrides the OAuth 2.0 device authorization
	// endpoint URL (RFC 8628) advertised in the auth metadata. If empty, the
	// value discovered from the issuer is used, falling back to
	// {Issuer}/oauth2/device/auth.
	DeviceAuthorizationEndpoint string `yaml:"device_authorization_endpoint"`

	// AccountManagementActions lists the account management actions supported by
	// the AccountManagementURL, advertised as account_management_actions_supported
	// in the auth metadata (e.g. "org.matrix.profile", "org.matrix.devices_list").
	AccountManagementActions []string `yaml:"account_management_actions"`

	// JWKSURI overrides the JWKS URI advertised in authorization server metadata.
	// If empty, defaults to {Issuer}/oauth2/keys.
	JWKSURI string `yaml:"jwks_uri"`

	// SSOCallbackURL overrides the callback URL used for the legacy SSO compat
	// layer. If empty it is derived from the incoming request host as
	// {scheme}://{host}/_matrix/client/v3/login/sso/callback.
	SSOCallbackURL string `yaml:"sso_callback_url"`

	// PublicBaseURL is the public base URL of this homeserver (e.g.
	// https://matrix.example.com). When set it is used instead of the incoming
	// request's Host header to build URLs advertised to clients, such as the
	// JWKS proxy URL in the auth metadata and the SSO callback URL.
	PublicBaseURL string `yaml:"public_base_url"`

	// SSORedirectAllowlist limits the redirectUrl values accepted by the legacy
	// SSO compat layer. When empty the check is default-deny: only redirect
	// targets on the homeserver's own origin (PublicBaseURL when set, else the
	// incoming request's origin) are accepted. Entries with an http(s) scheme
	// are matched on scheme, host and path prefix; entries with any other
	// scheme (e.g. "element://") are matched as plain string prefixes.
	SSORedirectAllowlist []string `yaml:"sso_redirect_allowlist"`
}

// msc3861Endpoint returns the configured endpoint or the default built from the issuer.
func (c *MSC3861Config) msc3861Endpoint(configured, defaultPath string) string {
	if configured != "" {
		return configured
	}
	return strings.TrimSuffix(c.Issuer, "/") + defaultPath
}

// AuthorizationEndpointOrDefault returns the authorization endpoint to advertise.
func (c *MSC3861Config) AuthorizationEndpointOrDefault() string {
	return c.msc3861Endpoint(c.AuthorizationEndpoint, "/oauth2/auth")
}

// TokenEndpointOrDefault returns the token endpoint to advertise.
func (c *MSC3861Config) TokenEndpointOrDefault() string {
	return c.msc3861Endpoint(c.TokenEndpoint, "/oauth2/token")
}

// RegistrationEndpointOrDefault returns the dynamic client registration endpoint to advertise.
func (c *MSC3861Config) RegistrationEndpointOrDefault() string {
	return c.msc3861Endpoint(c.RegistrationEndpoint, "/oauth2/clients/register")
}

// RevocationEndpointOrDefault returns the token revocation endpoint to advertise.
func (c *MSC3861Config) RevocationEndpointOrDefault() string {
	return c.msc3861Endpoint(c.RevocationEndpoint, "/oauth2/revoke")
}

// JWKSURIOrDefault returns the JWKS URI to advertise.
func (c *MSC3861Config) JWKSURIOrDefault() string {
	return c.msc3861Endpoint(c.JWKSURI, "/oauth2/keys")
}

// IntrospectionEndpointOrDefault returns the token introspection endpoint to use.
func (c *MSC3861Config) IntrospectionEndpointOrDefault() string {
	return c.msc3861Endpoint(c.IntrospectionEndpoint, "/oauth2/introspect")
}

// UserinfoEndpointOrDefault returns the OIDC UserInfo endpoint to use.
func (c *MSC3861Config) UserinfoEndpointOrDefault() string {
	return c.msc3861Endpoint(c.UserinfoEndpoint, "/userinfo")
}

// DeviceAuthorizationEndpointOrDefault returns the device authorization
// endpoint to advertise.
func (c *MSC3861Config) DeviceAuthorizationEndpointOrDefault() string {
	return c.msc3861Endpoint(c.DeviceAuthorizationEndpoint, "/oauth2/device/auth")
}

// isLocalhostHost reports whether host is a loopback hostname. Plain-HTTP OIDC
// issuers are only permitted for these hosts, to support local development.
func isLocalhostHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func (c *MSCs) Defaults(opts DefaultOpts) {
	if opts.Generate {
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:mscs.db"
		}
	}
}

// Enabled returns true if the given msc is enabled. Should in the form 'msc12345'.
func (c *MSCs) Enabled(msc string) bool {
	for _, m := range c.MSCs {
		if m == msc {
			return true
		}
	}
	return false
}

func (c *MSCs) Verify(configErrs *ConfigErrors) {
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "mscs.database.connection_string", string(c.Database.ConnectionString))
	}
	if c.Enabled("msc3861") {
		checkNotEmpty(configErrs, "mscs.msc3861.issuer", c.MSC3861.Issuer)
		checkNotEmpty(configErrs, "mscs.msc3861.client_id", c.MSC3861.ClientID)
		checkNotEmpty(configErrs, "mscs.msc3861.client_secret", c.MSC3861.ClientSecret)

		if c.MSC3861.Issuer != "" {
			if u, err := url.Parse(c.MSC3861.Issuer); err != nil || u.Scheme == "" || u.Host == "" {
				configErrs.Add("mscs.msc3861.issuer must be a valid URL with scheme and host")
			} else if u.Scheme != "https" && !isLocalhostHost(u.Hostname()) {
				configErrs.Add("mscs.msc3861.issuer must use https; http is only allowed for localhost development issuers")
			}
		}

		// All non-empty endpoint overrides must be absolute URLs.
		endpoints := []struct {
			name  string
			value string
		}{
			{"introspection_endpoint", c.MSC3861.IntrospectionEndpoint},
			{"userinfo_endpoint", c.MSC3861.UserinfoEndpoint},
			{"authorization_endpoint", c.MSC3861.AuthorizationEndpoint},
			{"token_endpoint", c.MSC3861.TokenEndpoint},
			{"registration_endpoint", c.MSC3861.RegistrationEndpoint},
			{"revocation_endpoint", c.MSC3861.RevocationEndpoint},
			{"end_session_endpoint", c.MSC3861.EndSessionEndpoint},
			{"device_authorization_endpoint", c.MSC3861.DeviceAuthorizationEndpoint},
			{"jwks_uri", c.MSC3861.JWKSURI},
			{"sso_callback_url", c.MSC3861.SSOCallbackURL},
			{"account_management_url", c.MSC3861.AccountManagementURL},
			{"public_base_url", c.MSC3861.PublicBaseURL},
		}
		for _, ep := range endpoints {
			if ep.value == "" {
				continue
			}
			if u, err := url.Parse(ep.value); err != nil || u.Scheme == "" || u.Host == "" {
				configErrs.Add(fmt.Sprintf("mscs.msc3861.%s must be a valid absolute URL with scheme and host", ep.name))
			}
		}

		if c.MSC3861.ClientAuthMethod != "" {
			switch c.MSC3861.ClientAuthMethod {
			case "client_secret_basic", "client_secret_post":
				// valid
			default:
				configErrs.Add("mscs.msc3861.client_auth_method must be 'client_secret_basic' or 'client_secret_post'")
			}
		}

		if c.MSC3861.AdminToken == "" {
			logrus.Warn("mscs.msc3861.admin_token is empty; MAS admin API endpoints will reject all requests")
		}
		if c.MSC3861.AccountManagementURL == "" {
			logrus.Warn("mscs.msc3861.account_management_url is empty; account management discovery and m.oauth UIA (e.g. cross-signing reset) will not work")
		}
		if len(c.MSC3861.SSORedirectAllowlist) == 0 {
			// Without an allowlist the legacy SSO flow is default-deny, because
			// a redirectUrl pointing off-origin would hand the login token, and
			// with it the provider-issued access and refresh tokens, to whoever
			// crafted the link.
			logrus.Warn("mscs.msc3861.sso_redirect_allowlist is empty; the legacy SSO flow will only accept redirectUrl values on this homeserver's own origin. Configure it with the client origins and deep links you trust (e.g. [\"https://app.example.com/\", \"element://\"]) if clients need to be redirected elsewhere")
		}
	}
}
