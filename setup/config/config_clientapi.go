package config

import (
	"fmt"
	"time"
)

type ClientAPI struct {
	Matrix  *Global  `yaml:"-"`
	Derived *Derived `yaml:"-"` // TODO: Nuke Derived from orbit

	// If set disables new users from registering (except via shared
	// secrets)
	RegistrationDisabled bool `yaml:"registration_disabled"`

	// If set, requires users to submit a token during registration.
	// Tokens can be managed using admin API.
	RegistrationRequiresToken bool `yaml:"registration_requires_token"`

	// Enable registration without captcha verification or shared secret.
	// This option is populated by the -really-enable-open-registration
	// command line parameter as it is not recommended.
	OpenRegistrationWithoutVerificationEnabled bool `yaml:"-"`

	// If set, allows registration by anyone who also has the shared
	// secret, even if registration is otherwise disabled.
	RegistrationSharedSecret string `yaml:"registration_shared_secret"`

	// EmailValidationServer handles email verification when clients omit id_server.
	// Use a hostname listed in global.trusted_third_party_id_servers, without a scheme.
	// Zendrite registers with it as the server notices user to use its v2 API.
	EmailValidationServer string `yaml:"email_validation_server"`
	// If set, prevents guest accounts from being created. Only takes
	// effect if registration is enabled, otherwise guests registration
	// is forbidden either way.
	GuestsDisabled bool `yaml:"guests_disabled"`

	// Boolean stating whether catpcha registration is enabled
	// and required
	RecaptchaEnabled bool `yaml:"enable_registration_captcha"`
	// Recaptcha api.js Url, for compatible with hcaptcha.com, etc.
	RecaptchaApiJsUrl string `yaml:"recaptcha_api_js_url"`
	// Recaptcha div class for sitekey, for compatible with hcaptcha.com, etc.
	RecaptchaSitekeyClass string `yaml:"recaptcha_sitekey_class"`
	// Recaptcha form field, for compatible with hcaptcha.com, etc.
	RecaptchaFormField string `yaml:"recaptcha_form_field"`
	// This Home Server's ReCAPTCHA public key.
	RecaptchaPublicKey string `yaml:"recaptcha_public_key"`
	// This Home Server's ReCAPTCHA private key.
	RecaptchaPrivateKey string `yaml:"recaptcha_private_key"`
	// Secret used to bypass the captcha registration entirely
	RecaptchaBypassSecret string `yaml:"recaptcha_bypass_secret"`
	// HTTP API endpoint used to verify whether the captcha response
	// was successful
	RecaptchaSiteVerifyAPI string `yaml:"recaptcha_siteverify_api"`

	// CaptchaProvider selects which CAPTCHA backend to use:
	// "recaptcha" (default), "hcaptcha", or "altcha".
	CaptchaProvider string `yaml:"captcha_provider"`
	// AltchaHMACKey is the server-side HMAC secret used to sign and
	// verify ALTCHA proof-of-work challenges.
	AltchaHMACKey string `yaml:"altcha_hmac_key"`
	// AltchaMaxNumber controls the difficulty of the proof-of-work
	// challenge (default 50000).
	AltchaMaxNumber int64 `yaml:"altcha_max_number"`
	// AltchaExpiry is how long an ALTCHA challenge remains valid
	// (default "5m"). Parsed as a time.Duration string.
	AltchaExpiry string `yaml:"altcha_expiry"`

	// RefreshableAccessTokenLifetime is how long an access token issued with
	// a refresh token (MSC2918) remains valid. Clients ask for a refresh token
	// with `refresh_token: true` on /login and /register. The default is five
	// minutes, as in Synapse. Zero issues no refresh tokens and access tokens
	// that never expire.
	RefreshableAccessTokenLifetime time.Duration `yaml:"refreshable_access_token_lifetime"`

	// TURN options
	TURN TURN `yaml:"turn"`

	// Rate-limiting options
	RateLimiting RateLimiting `yaml:"rate_limiting"`

	// DelayedEvents configures MSC4140 delayed events.
	DelayedEvents DelayedEvents `yaml:"delayed_events"`

	// CallGuests configures restricted call guest accounts, which let people
	// without a Matrix account join calls by link while normal registration
	// stays closed.
	CallGuests CallGuests `yaml:"call_guests"`

	MSCs *MSCs `yaml:"-"`
}

func (c *ClientAPI) Defaults(opts DefaultOpts) {
	c.RegistrationSharedSecret = ""
	c.EmailValidationServer = ""
	c.RegistrationRequiresToken = false
	c.RecaptchaPublicKey = ""
	c.RecaptchaPrivateKey = ""
	c.RecaptchaEnabled = false
	c.RecaptchaBypassSecret = ""
	c.RecaptchaSiteVerifyAPI = ""
	c.CaptchaProvider = "recaptcha"
	c.AltchaHMACKey = ""
	c.AltchaMaxNumber = 50000
	c.AltchaExpiry = "5m"
	c.RefreshableAccessTokenLifetime = 5 * time.Minute //nolint:mnd
	c.RegistrationDisabled = true
	c.OpenRegistrationWithoutVerificationEnabled = false
	c.RateLimiting.Defaults()
	c.DelayedEvents.Defaults()
	c.CallGuests.Defaults()
}

func (c *ClientAPI) Verify(configErrs *ConfigErrors) {
	if c.EmailValidationServer != "" {
		trusted := false
		if c.Matrix != nil {
			for _, server := range c.Matrix.TrustedIDServers {
				if server == c.EmailValidationServer {
					trusted = true
					break
				}
			}
		}
		if !trusted {
			configErrs.Add("client_api.email_validation_server must be listed in global.trusted_third_party_id_servers")
		}
	}
	if c.RefreshableAccessTokenLifetime < 0 {
		configErrs.Add("client_api.refreshable_access_token_lifetime must not be negative")
	}
	c.TURN.Verify(configErrs)
	c.RateLimiting.Verify(configErrs)
	c.DelayedEvents.Verify(configErrs)
	c.CallGuests.Verify(configErrs, c.RegistrationDisabled)
	if c.RecaptchaEnabled {
		if c.CaptchaProvider == "altcha" {
			checkNotEmpty(configErrs, "client_api.altcha_hmac_key", c.AltchaHMACKey)
			if c.AltchaExpiry != "" {
				if _, err := time.ParseDuration(c.AltchaExpiry); err != nil {
					configErrs.Add(fmt.Sprintf("invalid duration for config key %q: %s", "client_api.altcha_expiry", c.AltchaExpiry))
				}
			}
		} else {
			if c.RecaptchaSiteVerifyAPI == "" {
				c.RecaptchaSiteVerifyAPI = "https://www.google.com/recaptcha/api/siteverify"
			}
			if c.RecaptchaApiJsUrl == "" {
				c.RecaptchaApiJsUrl = "https://www.google.com/recaptcha/api.js"
			}
			if c.RecaptchaFormField == "" {
				c.RecaptchaFormField = "g-recaptcha-response"
			}
			if c.RecaptchaSitekeyClass == "" {
				c.RecaptchaSitekeyClass = "g-recaptcha"
			}
			checkNotEmpty(configErrs, "client_api.recaptcha_public_key", c.RecaptchaPublicKey)
			checkNotEmpty(configErrs, "client_api.recaptcha_private_key", c.RecaptchaPrivateKey)
			checkNotEmpty(configErrs, "client_api.recaptcha_siteverify_api", c.RecaptchaSiteVerifyAPI)
			checkNotEmpty(configErrs, "client_api.recaptcha_sitekey_class", c.RecaptchaSitekeyClass)
		}
	}
	// Ensure there is any spam counter measure when enabling registration.
	// Call guest sign-up needs registration_disabled, so it never reaches this
	// rule: it is deliberately allowed with only m.login.dummy, because call
	// guests are restricted and expire.
	if !c.RegistrationDisabled && !c.OpenRegistrationWithoutVerificationEnabled && !c.RecaptchaEnabled && !c.RegistrationRequiresToken {
		configErrs.Add(
			"You have tried to enable open registration without any secondary verification methods " +
				"(such as reCAPTCHA). By enabling open registration, you are SIGNIFICANTLY " +
				"increasing the risk that your server will be used to send spam or abuse, and may result in " +
				"your server being banned from some rooms. If you are ABSOLUTELY CERTAIN you want to do this, " +
				"start Zendrite with the -really-enable-open-registration command line flag. Otherwise, you " +
				"should set the registration_disabled option in your Zendrite config.",
		)
	}
}

// DelayedEvents configures MSC4140 delayed events: events that a client
// schedules for the server to send later unless the client restarts or
// cancels them first. Element Call relies on them to end the call membership
// of a client that disappears.
type DelayedEvents struct {
	// MaxDelay is the longest delay a client may request. Zero, the default
	// as in Synapse, disables scheduling delayed events.
	MaxDelay time.Duration `yaml:"max_event_delay_duration"`

	// MaxPerUser is how many delayed events one user may have scheduled at
	// once.
	MaxPerUser int `yaml:"max_delayed_events_per_user"`

	// ManagementRateLimiting limits the requests that restart, cancel, send
	// or look up delayed events, per device or, without an access token, per
	// address. Element Call restarts its delayed events every few seconds.
	ManagementRateLimiting RateLimiting `yaml:"management_rate_limiting"`
}

// Enabled reports whether clients may schedule delayed events.
func (d *DelayedEvents) Enabled() bool {
	return d.MaxDelay > 0 && d.MaxPerUser > 0
}

func (d *DelayedEvents) Defaults() {
	d.MaxDelay = 0
	d.MaxPerUser = 100
	// A burst of 20 requests, then one per second, as Element Call's
	// documentation suggests for Synapse.
	d.ManagementRateLimiting = RateLimiting{Enabled: true, Threshold: 20, CooloffMS: 20000} //nolint:mnd
}

func (d *DelayedEvents) Verify(configErrs *ConfigErrors) {
	if d.MaxDelay < 0 {
		configErrs.Add("client_api.delayed_events.max_event_delay_duration must not be negative")
	}
	checkPositive(configErrs, "client_api.delayed_events.max_delayed_events_per_user", int64(d.MaxPerUser))
	if d.ManagementRateLimiting.Enabled {
		checkPositive(configErrs, "client_api.delayed_events.management_rate_limiting.threshold", d.ManagementRateLimiting.Threshold)
		checkPositive(configErrs, "client_api.delayed_events.management_rate_limiting.cooloff_ms", d.ManagementRateLimiting.CooloffMS)
	}
}

// CallGuests configures restricted call guest accounts. While enabled, every
// open sign-up through /register creates a call guest: an account that can
// only join rooms this server is already in, take part in calls and chat
// there, and that is deactivated once its lifetime has passed. Normal
// accounts still come from shared-secret registration, registration tokens
// and application services.
type CallGuests struct {
	// Enable turns call guest sign-up on. It requires registration_disabled,
	// so that open sign-ups never create normal accounts.
	Enable bool `yaml:"enabled"`

	// Lifetime is how long a call guest account lives before it is
	// deactivated.
	Lifetime time.Duration `yaml:"lifetime"`
}

// Enabled reports whether open sign-ups create call guests.
func (g *CallGuests) Enabled() bool {
	return g.Enable
}

func (g *CallGuests) Defaults() {
	g.Enable = false
	g.Lifetime = 24 * time.Hour //nolint:mnd
}

func (g *CallGuests) Verify(configErrs *ConfigErrors, registrationDisabled bool) {
	if g.Lifetime <= 0 {
		configErrs.Add("client_api.call_guests.lifetime must be positive")
	}
	if g.Enable && !registrationDisabled {
		configErrs.Add("client_api.call_guests.enabled requires client_api.registration_disabled: " +
			"while call guests are enabled every open sign-up creates a call guest, so open registration of normal accounts cannot be enabled as well")
	}
}

type TURN struct {
	// TODO Guest Support
	// Whether or not guests can request TURN credentials
	// AllowGuests bool `yaml:"turn_allow_guests"`
	// How long the authorization should last
	UserLifetime string `yaml:"turn_user_lifetime"`
	// The list of TURN URIs to pass to clients
	URIs []string `yaml:"turn_uris"`

	// Authorization via Shared Secret
	// The shared secret from coturn
	SharedSecret string `yaml:"turn_shared_secret"`

	// Authorization via Static Username & Password
	// Hardcoded Username and Password
	Username string `yaml:"turn_username"`
	Password string `yaml:"turn_password"`
}

func (c *TURN) Verify(configErrs *ConfigErrors) {
	value := c.UserLifetime
	if value != "" {
		if _, err := time.ParseDuration(value); err != nil {
			configErrs.Add(fmt.Sprintf("invalid duration for config key %q: %s", "client_api.turn.turn_user_lifetime", value))
		}
	}
}

type RateLimiting struct {
	// Is rate limiting enabled or disabled?
	Enabled bool `yaml:"enabled"`

	// How many "slots" a user can occupy sending requests to a rate-limited
	// endpoint before we apply rate-limiting
	Threshold int64 `yaml:"threshold"`

	// The cooloff period in milliseconds after a request before the "slot"
	// is freed again
	CooloffMS int64 `yaml:"cooloff_ms"`

	// A list of users that are exempt from rate limiting, i.e. if you want
	// to run Mjolnir or other bots.
	ExemptUserIDs []string `yaml:"exempt_user_ids"`
}

func (r *RateLimiting) Verify(configErrs *ConfigErrors) {
	if r.Enabled {
		checkPositive(configErrs, "client_api.rate_limiting.threshold", r.Threshold)
		checkPositive(configErrs, "client_api.rate_limiting.cooloff_ms", r.CooloffMS)
	}
}

func (r *RateLimiting) Defaults() {
	r.Enabled = true
	r.Threshold = 5
	r.CooloffMS = 500
}
