// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package config

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestMSC3861Verify_MissingIssuer(t *testing.T) {
	t.Parallel()
	mscs := &MSCs{
		Matrix: &Global{},
		MSCs:   []string{"msc3861"},
		Database: DatabaseOptions{
			ConnectionString: "file:mscs.db",
		},
		MSC3861: MSC3861Config{
			ClientID:     "client-id",
			ClientSecret: "client-secret",
		},
	}
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)

	found := false
	for _, err := range *configErrs {
		if strings.Contains(err, "mscs.msc3861.issuer") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected config error about missing issuer")
	}
}

func TestMSC3861Verify_MissingClientID(t *testing.T) {
	t.Parallel()
	mscs := &MSCs{
		Matrix: &Global{},
		MSCs:   []string{"msc3861"},
		Database: DatabaseOptions{
			ConnectionString: "file:mscs.db",
		},
		MSC3861: MSC3861Config{
			Issuer:       "https://auth.example.com",
			ClientSecret: "client-secret",
		},
	}
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)

	found := false
	for _, err := range *configErrs {
		if strings.Contains(err, "mscs.msc3861.client_id") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected config error about missing client_id")
	}
}

func TestMSC3861Verify_MissingClientSecret(t *testing.T) {
	t.Parallel()
	mscs := &MSCs{
		Matrix: &Global{},
		MSCs:   []string{"msc3861"},
		Database: DatabaseOptions{
			ConnectionString: "file:mscs.db",
		},
		MSC3861: MSC3861Config{
			Issuer:   "https://auth.example.com",
			ClientID: "client-id",
		},
	}
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)

	found := false
	for _, err := range *configErrs {
		if strings.Contains(err, "mscs.msc3861.client_secret") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected config error about missing client_secret")
	}
}

func TestMSC3861Verify_Valid(t *testing.T) {
	t.Parallel()
	mscs := &MSCs{
		Matrix: &Global{},
		MSCs:   []string{"msc3861"},
		Database: DatabaseOptions{
			ConnectionString: "file:mscs.db",
		},
		MSC3861: MSC3861Config{
			Issuer:       "https://auth.example.com",
			ClientID:     "client-id",
			ClientSecret: "client-secret",
		},
	}
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)

	for _, err := range *configErrs {
		if strings.Contains(err, "msc3861") {
			t.Errorf("unexpected MSC3861 config error: %s", err)
		}
	}
}

func TestMSC3861Verify_Disabled(t *testing.T) {
	t.Parallel()
	mscs := &MSCs{
		Matrix: &Global{},
		MSCs:   []string{},
		Database: DatabaseOptions{
			ConnectionString: "file:mscs.db",
		},
		MSC3861: MSC3861Config{},
	}
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)

	for _, err := range *configErrs {
		if strings.Contains(err, "msc3861") {
			t.Errorf("unexpected MSC3861 config error when disabled: %s", err)
		}
	}
}

// msc3861VerifyErrorContains reports whether Verify produced an MSC3861 error
// containing the given substring.
func msc3861VerifyErrorContains(mscs *MSCs, substr string) bool {
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)
	for _, err := range *configErrs {
		if strings.Contains(err, substr) {
			return true
		}
	}
	return false
}

func msc3861VerifyTestConfig(issuer string) *MSCs {
	return &MSCs{
		Matrix: &Global{},
		MSCs:   []string{"msc3861"},
		Database: DatabaseOptions{
			ConnectionString: "file:mscs.db",
		},
		MSC3861: MSC3861Config{
			Issuer:       issuer,
			ClientID:     "client-id",
			ClientSecret: "client-secret",
		},
	}
}

func TestMSC3861Verify_IssuerRequiresHTTPS(t *testing.T) {
	t.Parallel()
	if !msc3861VerifyErrorContains(msc3861VerifyTestConfig("http://auth.example.com"), "mscs.msc3861.issuer must use https") {
		t.Error("expected config error for a plain-http issuer")
	}
	if msc3861VerifyErrorContains(msc3861VerifyTestConfig("https://auth.example.com"), "mscs.msc3861.issuer") {
		t.Error("unexpected config error for an https issuer")
	}
}

func TestMSC3861Verify_IssuerLocalhostHTTPAllowed(t *testing.T) {
	t.Parallel()
	for _, issuer := range []string{
		"http://localhost:8080",
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
	} {
		if msc3861VerifyErrorContains(msc3861VerifyTestConfig(issuer), "mscs.msc3861.issuer") {
			t.Errorf("unexpected config error for localhost issuer %q", issuer)
		}
	}
}

func TestMSC3861Verify_EndpointOverridesMustBeAbsoluteURLs(t *testing.T) {
	t.Parallel()
	mscs := msc3861VerifyTestConfig("https://auth.example.com")
	mscs.MSC3861.IntrospectionEndpoint = "not-a-url"
	if !msc3861VerifyErrorContains(mscs, "mscs.msc3861.introspection_endpoint must be a valid absolute URL") {
		t.Error("expected config error for a non-URL introspection_endpoint")
	}

	mscs = msc3861VerifyTestConfig("https://auth.example.com")
	mscs.MSC3861.SSOCallbackURL = "/relative/path"
	if !msc3861VerifyErrorContains(mscs, "mscs.msc3861.sso_callback_url must be a valid absolute URL") {
		t.Error("expected config error for a relative sso_callback_url")
	}

	mscs = msc3861VerifyTestConfig("https://auth.example.com")
	mscs.MSC3861.PublicBaseURL = "matrix.example.com"
	if !msc3861VerifyErrorContains(mscs, "mscs.msc3861.public_base_url must be a valid absolute URL") {
		t.Error("expected config error for a schemeless public_base_url")
	}

	mscs = msc3861VerifyTestConfig("https://auth.example.com")
	mscs.MSC3861.PublicBaseURL = "https://matrix.example.com"
	mscs.MSC3861.TokenEndpoint = "https://auth.example.com/custom/token"
	if msc3861VerifyErrorContains(mscs, "msc3861") {
		t.Error("unexpected config error for valid absolute URLs")
	}
}

// An empty sso_redirect_allowlist is not a config error, but it must warn:
// the legacy SSO flow is default-deny in that case and only accepts redirect
// targets on the homeserver's own origin.
func TestMSC3861Verify_EmptySSORedirectAllowlistWarnsButIsValid(t *testing.T) {
	mscs := msc3861VerifyTestConfig("https://auth.example.com")

	var logged bytes.Buffer
	restore := captureWarnings(&logged)
	configErrs := &ConfigErrors{}
	mscs.Verify(configErrs)
	restore()

	for _, err := range *configErrs {
		if strings.Contains(err, "sso_redirect_allowlist") {
			t.Errorf("unexpected config error for an empty sso_redirect_allowlist: %s", err)
		}
	}
	if !strings.Contains(logged.String(), "mscs.msc3861.sso_redirect_allowlist is empty") {
		t.Errorf("expected a startup warning about the empty allowlist, got: %s", logged.String())
	}
	if !strings.Contains(logged.String(), "own origin") {
		t.Errorf("expected the warning to explain the same-origin default, got: %s", logged.String())
	}

	// A configured allowlist must not warn.
	mscs = msc3861VerifyTestConfig("https://auth.example.com")
	mscs.MSC3861.SSORedirectAllowlist = []string{"https://app.example.com/"}
	logged.Reset()
	restore = captureWarnings(&logged)
	mscs.Verify(&ConfigErrors{})
	restore()
	if strings.Contains(logged.String(), "sso_redirect_allowlist") {
		t.Errorf("unexpected allowlist warning when one is configured: %s", logged.String())
	}
}

// captureWarnings redirects logrus output into buf until the returned function
// is called. The tests using it cannot run in parallel, since logrus output is
// global.
func captureWarnings(buf *bytes.Buffer) func() {
	prevOut := logrus.StandardLogger().Out
	prevFormatter := logrus.StandardLogger().Formatter
	logrus.SetOutput(buf)
	logrus.SetFormatter(&logrus.TextFormatter{DisableColors: true, DisableTimestamp: true})
	return func() {
		logrus.SetOutput(prevOut)
		logrus.SetFormatter(prevFormatter)
	}
}

func TestMSCs_Enabled(t *testing.T) {
	t.Parallel()
	mscs := &MSCs{
		MSCs: []string{"msc2836", "msc3861"},
	}
	if !mscs.Enabled("msc3861") {
		t.Error("expected msc3861 to be enabled")
	}
	if !mscs.Enabled("msc2836") {
		t.Error("expected msc2836 to be enabled")
	}
	if mscs.Enabled("msc9999") {
		t.Error("expected msc9999 to not be enabled")
	}
}
