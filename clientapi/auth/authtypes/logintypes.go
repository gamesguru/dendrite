package authtypes

// LoginType are specified by http://matrix.org/docs/spec/client_server/r0.2.0.html#login-types
type LoginType string

// The relevant login types implemented in Zendrite.
const (
	LoginTypePassword           = "m.login.password"
	LoginTypeDummy              = "m.login.dummy"
	LoginTypeSharedSecret       = "org.matrix.login.shared_secret"
	LoginTypeRecaptcha          = "m.login.recaptcha"
	LoginTypeAltcha             = "org.codefloe.altcha"
	LoginTypeApplicationService = "m.login.application_service"
	LoginTypeToken              = "m.login.token"
	LoginTypeSSO                = "m.login.sso"
	LoginTypeRegistrationToken  = "m.login.registration_token"
	// LoginTypeOAuth is the m.oauth UIA type used with the OAuth 2.0 API,
	// currently only valid on /keys/device_signing/upload.
	LoginTypeOAuth = "m.oauth"
)
