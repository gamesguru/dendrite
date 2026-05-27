// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/goccy/go-yaml"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
)

// keyIDRegexp defines allowable characters in Key IDs.
var keyIDRegexp = regexp.MustCompile("^ed25519:[a-zA-Z0-9_]+$")

// Version is the current version of the config format.
// This will change whenever we make breaking changes to the config format.
const Version = 2

// Zendrite contains all the config used by a zendrite process.
// Relative paths are resolved relative to the current working directory.
type Zendrite struct {
	// The version of the configuration file.
	// If the version in a file doesn't match the current zendrite config
	// version then we can give a clear error message telling the user
	// to update their config file to the current version.
	// The version of the file should only be different if there has
	// been a breaking change to the config file format.
	Version int `yaml:"version"`

	Global        Global        `yaml:"global"`
	AppServiceAPI AppServiceAPI `yaml:"app_service_api"`
	ClientAPI     ClientAPI     `yaml:"client_api"`
	FederationAPI FederationAPI `yaml:"federation_api"`
	KeyServer     KeyServer     `yaml:"key_server"`
	MediaAPI      MediaAPI      `yaml:"media_api"`
	RoomServer    RoomServer    `yaml:"room_server"`
	SyncAPI       SyncAPI       `yaml:"sync_api"`
	UserAPI       UserAPI       `yaml:"user_api"`
	RelayAPI      RelayAPI      `yaml:"relay_api"`

	MSCs MSCs `yaml:"mscs"`

	// The config for tracing the zendrite servers.
	Tracing struct {
		// Set to true to enable tracer hooks. If false, no tracing is set up.
		Enabled bool `yaml:"enabled"`
		// OTLP gRPC endpoint, e.g. "localhost:4317".
		Endpoint string `yaml:"endpoint"`
		// Use insecure gRPC connection (no TLS).
		Insecure bool `yaml:"insecure"`
	} `yaml:"tracing"`

	// The config for logging informations. Each hook will be added to logrus.
	Logging []LogrusHook `yaml:"logging"`

	// Any information derived from the configuration options for later use.
	Derived Derived `yaml:"-"`
}

// TODO: Kill Derived
type Derived struct {
	Registration struct {
		// Flows is a slice of flows, which represent one possible way that the client can authenticate a request.
		// http://matrix.org/docs/spec/HEAD/client_server/r0.3.0.html#user-interactive-authentication-api
		// As long as the generated flows only rely on config file options,
		// we can generate them on startup and store them until needed
		Flows []authtypes.Flow `json:"flows"`

		// Params that need to be returned to the client during
		// registration in order to complete registration stages.
		Params map[string]any `json:"params"`
	}

	// Application services parsed from their config files
	// The paths of which were given above in the main config file
	ApplicationServices []ApplicationService

	// Meta-regexes compiled from all exclusive application service
	// Regexes.
	//
	// When a user registers, we check that their username does not match any
	// exclusive application service namespaces
	ExclusiveApplicationServicesUsernameRegexp *regexp.Regexp
	// When a user creates a room alias, we check that it isn't already
	// reserved by an application service
	ExclusiveApplicationServicesAliasRegexp *regexp.Regexp
	// Note: An Exclusive Regex for room ID isn't necessary as we aren't blocking
	// servers from creating RoomIDs in exclusive application service namespaces
}

// A Path on the filesystem.
type Path string

// A DataSource for opening a postgresql database using pgx.
type DataSource string

func (d DataSource) IsSQLite() bool {
	return strings.HasPrefix(string(d), "file:")
}

func (d DataSource) IsPostgres() bool {
	// commented line may not always be true?
	// return strings.HasPrefix(string(d), "postgres:")
	return !d.IsSQLite()
}

// FileSizeBytes is a file size in bytes.
type FileSizeBytes int64

// ThumbnailSize contains a single thumbnail size configuration.
type ThumbnailSize struct {
	// Maximum width of the thumbnail image
	Width int `yaml:"width"`
	// Maximum height of the thumbnail image
	Height int `yaml:"height"`
	// ResizeMethod is one of crop or scale.
	// crop scales to fill the requested dimensions and crops the excess.
	// scale scales to fit the requested dimensions and one dimension may be smaller than requested.
	ResizeMethod string `yaml:"method,omitempty"`
}

// LogrusHook represents a single logrus hook. At this point, only parsing and
// verification of the proper values for type and level are done.
// Validity/integrity checks on the parameters are done when configuring logrus.
type LogrusHook struct {
	// The type of hook, currently only "file" is supported.
	Type string `yaml:"type"`

	// The level of the logs to produce. Will output only this level and above.
	Level string `yaml:"level"`

	// The parameters for this hook.
	Params map[string]any `yaml:"params"`
}

// ConfigErrors stores problems encountered when parsing a config file.
// It implements the error interface.
type ConfigErrors []string

// Load a yaml config file for a server run as multiple processes or as a monolith.
// Checks the config to ensure that it is valid.
func Load(configPath string) (*Zendrite, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	basePath, err := filepath.Abs(".")
	if err != nil {
		return nil, err
	}
	// Pass the current working directory and os.ReadFile so that they can
	// be mocked in the tests
	return loadConfig(basePath, configData, os.ReadFile)
}

func loadConfig(
	basePath string,
	configData []byte,
	readFile func(string) ([]byte, error),
) (*Zendrite, error) {
	var c Zendrite
	c.Defaults(DefaultOpts{
		Generate:       false,
		SingleDatabase: true,
	})

	var err error
	if err = yaml.Unmarshal(configData, &c); err != nil {
		return nil, err
	}

	if err = c.check(); err != nil {
		return nil, err
	}

	privateKeyPath := absPath(basePath, c.Global.PrivateKeyPath)
	if c.Global.KeyID, c.Global.PrivateKey, err = LoadMatrixKey(privateKeyPath, readFile); err != nil {
		return nil, fmt.Errorf("failed to load private_key: %w", err)
	}

	for _, v := range c.Global.VirtualHosts {
		if v.KeyValidityPeriod == 0 {
			v.KeyValidityPeriod = c.Global.KeyValidityPeriod
		}
		if v.PrivateKeyPath == "" || v.PrivateKey == nil || v.KeyID == "" {
			v.KeyID = c.Global.KeyID
			v.PrivateKey = c.Global.PrivateKey
			continue
		}
		privateKeyPath := absPath(basePath, v.PrivateKeyPath)
		if v.KeyID, v.PrivateKey, err = LoadMatrixKey(privateKeyPath, readFile); err != nil {
			return nil, fmt.Errorf("failed to load private_key for virtualhost %s: %w", v.ServerName, err)
		}
	}

	for _, key := range c.Global.OldVerifyKeys {
		switch {
		case key.PrivateKeyPath != "":
			var oldPrivateKeyData []byte
			oldPrivateKeyPath := absPath(basePath, key.PrivateKeyPath)
			oldPrivateKeyData, err = readFile(oldPrivateKeyPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read %q: %w", oldPrivateKeyPath, err)
			}

			// NOTSPEC: Ordinarily we should enforce key ID formatting, but since there are
			// a number of private keys out there with non-compatible symbols in them due
			// to lack of validation in Synapse, we won't enforce that for old verify keys.
			keyID, privateKey, perr := readKeyPEM(oldPrivateKeyPath, oldPrivateKeyData, false)
			if perr != nil {
				return nil, fmt.Errorf("failed to parse %q: %w", oldPrivateKeyPath, perr)
			}

			key.KeyID = keyID
			key.PrivateKey = privateKey
			pubKey, ok := privateKey.Public().(ed25519.PublicKey)
			if !ok {
				return nil, fmt.Errorf("failed to convert public key to ed25519.PublicKey")
			}
			key.PublicKey = spec.Base64Bytes(pubKey)

		case key.KeyID == "":
			return nil, fmt.Errorf("'key_id' must be specified if 'public_key' is specified")

		case len(key.PublicKey) == ed25519.PublicKeySize:
			continue

		case len(key.PublicKey) > 0:
			return nil, fmt.Errorf("the supplied 'public_key' is the wrong length")

		default:
			return nil, fmt.Errorf("either specify a 'private_key' path or supply both 'public_key' and 'key_id'")
		}
	}

	c.MediaAPI.AbsBasePath = Path(absPath(basePath, c.MediaAPI.BasePath))

	// Generate data from config options
	err = c.Derive()
	if err != nil {
		return nil, err
	}

	c.Wiring()
	return &c, nil
}

func LoadMatrixKey(privateKeyPath string, readFile func(string) ([]byte, error)) (gomatrixserverlib.KeyID, ed25519.PrivateKey, error) {
	privateKeyData, err := readFile(privateKeyPath)
	if err != nil {
		return "", nil, err
	}
	return readKeyPEM(privateKeyPath, privateKeyData, true)
}

// Derive generates data that is derived from various values provided in
// the config file.
func (config *Zendrite) Derive() error {
	// Determine registrations flows based off config values

	config.Derived.Registration.Params = make(map[string]any)

	// TODO: Add email auth type
	// TODO: Add MSISDN auth type

	switch {
	case config.ClientAPI.RecaptchaEnabled && config.ClientAPI.CaptchaProvider == "altcha":
		// Use a vendor-specific login type so Matrix clients that
		// handle m.login.recaptcha natively (e.g. Element) don't try
		// to render a Google reCAPTCHA widget and instead open the
		// auth fallback page.
		config.Derived.Registration.Flows = []authtypes.Flow{
			{Stages: []authtypes.LoginType{authtypes.LoginTypeAltcha}},
		}
	case config.ClientAPI.RecaptchaEnabled:
		config.Derived.Registration.Params[authtypes.LoginTypeRecaptcha] = map[string]string{"public_key": config.ClientAPI.RecaptchaPublicKey}
		config.Derived.Registration.Flows = []authtypes.Flow{
			{Stages: []authtypes.LoginType{authtypes.LoginTypeRecaptcha}},
		}
	case config.ClientAPI.RegistrationRequiresToken:
		config.Derived.Registration.Flows = []authtypes.Flow{
			{Stages: []authtypes.LoginType{authtypes.LoginTypeRegistrationToken}},
		}
	default:
		config.Derived.Registration.Flows = []authtypes.Flow{
			{Stages: []authtypes.LoginType{authtypes.LoginTypeDummy}},
		}
	}

	// Load application service configuration files
	if err := loadAppServices(&config.AppServiceAPI, &config.Derived); err != nil {
		return err
	}

	return nil
}

type DefaultOpts struct {
	Generate       bool
	SingleDatabase bool
}

// SetDefaults sets default config values if they are not explicitly set.
func (c *Zendrite) Defaults(opts DefaultOpts) {
	c.Version = Version

	c.Global.Defaults(opts)
	c.ClientAPI.Defaults(opts)
	c.FederationAPI.Defaults(opts)
	c.KeyServer.Defaults(opts)
	c.MediaAPI.Defaults(opts)
	c.RoomServer.Defaults(opts)
	c.SyncAPI.Defaults(opts)
	c.UserAPI.Defaults(opts)
	c.AppServiceAPI.Defaults(opts)
	c.RelayAPI.Defaults(opts)
	c.MSCs.Defaults(opts)
	c.Wiring()
}

func (c *Zendrite) Verify(configErrs *ConfigErrors) {
	type verifiable interface {
		Verify(configErrs *ConfigErrors)
	}
	for _, c := range []verifiable{
		&c.Global, &c.ClientAPI, &c.FederationAPI,
		&c.KeyServer, &c.MediaAPI, &c.RoomServer,
		&c.SyncAPI, &c.UserAPI,
		&c.AppServiceAPI, &c.RelayAPI, &c.MSCs,
	} {
		c.Verify(configErrs)
	}
}

func (c *Zendrite) Wiring() {
	c.Global.JetStream.Matrix = &c.Global
	c.ClientAPI.Matrix = &c.Global
	c.FederationAPI.Matrix = &c.Global
	c.KeyServer.Matrix = &c.Global
	c.MediaAPI.Matrix = &c.Global
	c.RoomServer.Matrix = &c.Global
	c.SyncAPI.Matrix = &c.Global
	c.UserAPI.Matrix = &c.Global
	c.AppServiceAPI.Matrix = &c.Global
	c.RelayAPI.Matrix = &c.Global
	c.MSCs.Matrix = &c.Global

	c.ClientAPI.Derived = &c.Derived
	c.AppServiceAPI.Derived = &c.Derived
	c.ClientAPI.MSCs = &c.MSCs
	c.UserAPI.MSCs = &c.MSCs
}

// Error returns a string detailing how many errors were contained within a
// configErrors type.
func (errs ConfigErrors) Error() string {
	if len(errs) == 1 {
		return errs[0]
	}
	return fmt.Sprintf(
		"%s (and %d other problems)", errs[0], len(errs)-1,
	)
}

// Add appends an error to the list of errors in this configErrors.
// It is a wrapper to the builtin append and hides pointers from
// the client code.
// This method is safe to use with an uninitialized configErrors because
// if it is nil, it will be properly allocated.
func (errs *ConfigErrors) Add(str string) {
	*errs = append(*errs, str)
}

// checkNotEmpty verifies the given value is not empty in the configuration.
// If it is, adds an error to the list.
func checkNotEmpty(configErrs *ConfigErrors, key, value string) {
	if value == "" {
		configErrs.Add(fmt.Sprintf("missing config key %q", key))
	}
}

// checkPositive verifies the given value is positive (zero included)
// in the configuration. If it is not, adds an error to the list.
func checkPositive(configErrs *ConfigErrors, key string, value int64) {
	if value < 0 {
		configErrs.Add(fmt.Sprintf("invalid value for config key %q: %d", key, value))
	}
}

// checkLogging verifies the parameters logging.* are valid.
func (config *Zendrite) checkLogging(configErrs *ConfigErrors) {
	for _, logrusHook := range config.Logging {
		checkNotEmpty(configErrs, "logging.type", logrusHook.Type)
		checkNotEmpty(configErrs, "logging.level", logrusHook.Level)
	}
}

// check returns an error type containing all errors found within the config
// file.
func (config *Zendrite) check() error { // monolithic
	var configErrs ConfigErrors

	if config.Version != Version {
		configErrs.Add(fmt.Sprintf(
			"config version is %d, expected %d - this means that the format of the configuration "+
				"file has changed in some significant way, so please revisit the sample config "+
				"and ensure you are not missing any important options that may have been added "+
				"or changed recently!",
			config.Version, Version,
		))
		return configErrs
	}

	config.checkLogging(&configErrs)

	// Due to how Golang manages its interface types, this condition is not redundant.
	// In order to get the proper behavior, it is necessary to return an explicit nil
	// and not a nil configErrors.
	// This is because the following equalities hold:
	// error(nil) == nil
	// error(configErrors(nil)) != nil
	if configErrs != nil {
		return configErrs
	}
	return nil
}

// absPath returns the absolute path for a given relative or absolute path.
func absPath(dir string, path Path) string {
	if filepath.IsAbs(string(path)) {
		// filepath.Join cleans the path so we should clean the absolute paths as well for consistency.
		return filepath.Clean(string(path))
	}
	return filepath.Join(dir, string(path))
}

func readKeyPEM(path string, data []byte, enforceKeyIDFormat bool) (gomatrixserverlib.KeyID, ed25519.PrivateKey, error) {
	for {
		var keyBlock *pem.Block
		keyBlock, data = pem.Decode(data)
		if data == nil {
			return "", nil, fmt.Errorf("no matrix private key PEM data in %q", path)
		}
		if keyBlock == nil {
			return "", nil, fmt.Errorf("keyBlock is nil %q", path)
		}
		if keyBlock.Type == "MATRIX PRIVATE KEY" {
			keyID := keyBlock.Headers["Key-ID"]
			if keyID == "" {
				return "", nil, fmt.Errorf("missing key ID in PEM data in %q", path)
			}
			if !strings.HasPrefix(keyID, "ed25519:") {
				return "", nil, fmt.Errorf("key ID %q doesn't start with \"ed25519:\" in %q", keyID, path)
			}
			if enforceKeyIDFormat && !keyIDRegexp.MatchString(keyID) {
				return "", nil, fmt.Errorf("key ID %q in %q contains illegal characters (use a-z, A-Z, 0-9 and _ only)", keyID, path)
			}
			_, privKey, err := ed25519.GenerateKey(bytes.NewReader(keyBlock.Bytes))
			if err != nil {
				return "", nil, err
			}
			return gomatrixserverlib.KeyID(keyID), privKey, nil
		}
	}
}

// SetupTracing configures OpenTelemetry tracing using the supplied configuration.
func (config *Zendrite) SetupTracing() (closer io.Closer, err error) {
	if !config.Tracing.Enabled {
		return io.NopCloser(bytes.NewReader([]byte{})), nil
	}

	ctx := context.Background()

	dialOpts := []grpc.DialOption{}
	if config.Tracing.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(config.Tracing.Endpoint),
		otlptracegrpc.WithDialOption(dialOpts...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)

	return &tracerProviderCloser{tp: tp}, nil
}

// tracerProviderCloser wraps a TracerProvider so it satisfies io.Closer.
type tracerProviderCloser struct {
	tp *sdktrace.TracerProvider
}

func (c *tracerProviderCloser) Close() error {
	return c.tp.Shutdown(context.Background())
}
