package config

import (
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
)

type FederationAPI struct {
	Matrix *Global `yaml:"-"`

	// The database stores information used by the federation destination queues to
	// send transactions to remote servers.
	Database DatabaseOptions `yaml:"database,omitempty"`

	// Federation failure threshold. How many consecutive failures that we should
	// tolerate when sending federation requests to a specific server. The backoff
	// is exponential with 2**(x+7) seconds, starting at ~4 minutes and capping at ~6 days:
	// 1 = 256s (~4min), 2 = 512s (~8min), ..., 12+ = 524288s (~6 days).
	// The default value is 11 if not specified, giving roughly 2 weeks of retry attempts
	// before the server is blacklisted.
	FederationMaxRetries uint32 `yaml:"send_max_retries"`

	// The first backoff after a failed send lasts 2**x seconds, doubling with each
	// further failure. The default 8 gives ~4 minutes. Lower it only for tests,
	// which restart servers and expect delivery to resume quickly.
	FederationMinRetryExponent uint32 `yaml:"send_min_retry_exponent"`

	// How long a partial state resync (MSC3706) may keep failing to get usable state from
	// any server before the join is abandoned: the local users leave the room again.
	// Failures on our side do not count, and neither does a server that gives the IDs of
	// the state but then does not serve the events themselves. The time is kept in memory,
	// so a restart starts it again. 0, the default, retries until it works.
	PartialStateResyncGiveUpAfter time.Duration `yaml:"partial_state_resync_give_up_after"`

	// P2P Feature: Whether relaying to specific nodes should be enabled.
	// Defaults to false.
	// Note: Enabling relays introduces a huge startup delay, if you are not using
	// relays and have many servers to re-hydrate on start. Only enable this
	// if you are using relays!
	EnableRelays bool `yaml:"enable_relays"`

	// P2P Feature: How many consecutive failures that we should tolerate when
	// sending federation requests to a specific server until we should assume they
	// are offline. If we assume they are offline then we will attempt to send
	// messages to their relay server if we know of one that is appropriate.
	P2PFederationRetriesUntilAssumedOffline uint32 `yaml:"p2p_retries_until_assumed_offline"`

	// FederationDisableTLSValidation disables the validation of X.509 TLS certs
	// on remote federation endpoints. This is not recommended in production!
	DisableTLSValidation bool `yaml:"disable_tls_validation"`

	// DisableHTTPKeepalives prevents Zendrite from keeping HTTP connections
	// open for reuse for future requests. Connections will be closed quicker
	// but we may spend more time on TLS handshakes instead.
	DisableHTTPKeepalives bool `yaml:"disable_http_keepalives"`

	// Perspective keyservers, to use as a backup when direct key fetch
	// requests don't succeed
	KeyPerspectives KeyPerspectives `yaml:"key_perspectives"`

	// Should we prefer direct key fetches over perspective ones?
	PreferDirectFetch bool `yaml:"prefer_direct_fetch"`

	preferDirectFetchSet bool

	// Deny/Allow lists used for restricting request scopes.
	DenyNetworkCIDRs  []string `yaml:"deny_networks"`
	AllowNetworkCIDRs []string `yaml:"allow_networks"`
}

func (c *FederationAPI) UnmarshalYAML(unmarshal func(any) error) error {
	var keys map[string]any
	if err := unmarshal(&keys); err != nil {
		return err
	}

	type federationAPI FederationAPI
	raw := federationAPI(*c)
	if err := unmarshal(&raw); err != nil {
		return err
	}
	*c = FederationAPI(raw)
	_, c.preferDirectFetchSet = keys["prefer_direct_fetch"]
	return nil
}

func (c *FederationAPI) Defaults(opts DefaultOpts) {
	c.FederationMaxRetries = 11
	c.FederationMinRetryExponent = 8
	c.P2PFederationRetriesUntilAssumedOffline = 1
	c.DisableTLSValidation = false
	c.DisableHTTPKeepalives = false
	c.DenyNetworkCIDRs = []string{
		"127.0.0.1/8",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
		"169.254.0.0/16",
		"::1/128",
		"fe80::/64",
		"fc00::/7",
	}
	c.AllowNetworkCIDRs = []string{
		"0.0.0.0/0",
		"::/0",
	}
	if opts.Generate {
		c.KeyPerspectives = KeyPerspectives{
			{
				ServerName: "matrix.org",
				Keys: []KeyPerspectiveTrustKey{
					{
						KeyID:     "ed25519:auto",
						PublicKey: "Noi6WqcDj0QmPxCNQqgezwTlBKrfqehY1u2FyWP9uYw",
					},
					{
						KeyID:     "ed25519:a_RXGa",
						PublicKey: "l8Hft5qXKn1vfHrg3p4+W8gELQVo8N13JkluMfmn2sQ",
					},
				},
			},
		}
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:federationapi.db"
		}
	}
}

func (c *FederationAPI) Verify(configErrs *ConfigErrors) {
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "federation_api.database.connection_string", string(c.Database.ConnectionString))
	}
}

// The config for setting a proxy to use for server->server requests.
type Proxy struct {
	// Is the proxy enabled?
	Enabled bool `yaml:"enabled"`
	// The protocol for the proxy (http / https / socks5)
	Protocol string `yaml:"protocol"`
	// The host where the proxy is listening
	Host string `yaml:"host"`
	// The port on which the proxy is listening
	Port uint16 `yaml:"port"`
}

func (c *Proxy) Defaults() {
	c.Enabled = false
	c.Protocol = "http"
	c.Host = "localhost"
	c.Port = 8080
}

func (c *Proxy) Verify(configErrs *ConfigErrors) {
}

// KeyPerspectives are used to configure perspective key servers for
// retrieving server keys.
type KeyPerspectives []KeyPerspective

type KeyPerspective struct {
	// The server name of the perspective key server
	ServerName spec.ServerName `yaml:"server_name"`
	// Server keys for the perspective user, used to verify the
	// keys have been signed by the perspective server
	Keys []KeyPerspectiveTrustKey `yaml:"keys"`
}

type KeyPerspectiveTrustKey struct {
	// The key ID, e.g. ed25519:auto
	KeyID gomatrixserverlib.KeyID `yaml:"key_id"`
	// The public key in base64 unpadded format
	PublicKey string `yaml:"public_key"`
}
