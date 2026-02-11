// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package config

import (
	"strings"
	"testing"
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
