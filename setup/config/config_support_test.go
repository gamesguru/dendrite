// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import (
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSupportVerify(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		support Support
		wantErr int
	}{
		{name: "unset", support: Support{}},
		{name: "page only", support: Support{SupportPage: "https://example.org/support"}},
		{name: "spec example", support: Support{
			Contacts: []SupportContact{
				{MatrixID: "@admin:example.org", EmailAddress: "admin@example.org", Role: "m.role.admin"},
				{EmailAddress: "security@example.org", Role: "m.role.security"},
			},
		}},
		{name: "namespaced role", support: Support{Contacts: []SupportContact{{EmailAddress: "billing@example.org", Role: "com.example.billing"}}}},
		{name: "page without scheme", support: Support{SupportPage: "example.org/support"}, wantErr: 1},
		{name: "page with other scheme", support: Support{SupportPage: "ftp://example.org/support"}, wantErr: 1},
		{name: "contact without address", support: Support{Contacts: []SupportContact{{Role: "m.role.admin"}}}, wantErr: 1},
		{name: "invalid matrix ID", support: Support{Contacts: []SupportContact{{MatrixID: "admin", Role: "m.role.admin"}}}, wantErr: 1},
		{name: "invalid email", support: Support{Contacts: []SupportContact{{EmailAddress: "Admin <admin@example.org>", Role: "m.role.admin"}}}, wantErr: 1},
		{name: "missing role", support: Support{Contacts: []SupportContact{{EmailAddress: "admin@example.org"}}}, wantErr: 1},
		{name: "reserved role", support: Support{Contacts: []SupportContact{{EmailAddress: "admin@example.org", Role: "m.role.billing"}}}, wantErr: 1},
		{name: "unnamespaced role", support: Support{Contacts: []SupportContact{{EmailAddress: "admin@example.org", Role: "billing"}}}, wantErr: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var errs ConfigErrors
			tc.support.Verify(&errs)
			assert.Len(t, errs, tc.wantErr, errs)
		})
	}
}

func TestSupportYAML(t *testing.T) {
	t.Parallel()

	var global Global
	require.NoError(t, yaml.Unmarshal([]byte(`
support:
  contacts:
    - matrix_id: "@admin:example.org"
      role: m.role.admin
  support_page: https://example.org/support
`), &global))
	assert.True(t, global.Support.Enabled())
	assert.Equal(t, Support{
		Contacts:    []SupportContact{{MatrixID: "@admin:example.org", Role: "m.role.admin"}},
		SupportPage: "https://example.org/support",
	}, global.Support)
}
