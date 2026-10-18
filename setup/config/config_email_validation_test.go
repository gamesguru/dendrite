package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmailValidationServer(t *testing.T) {
	for _, tc := range []struct {
		name, server string
		valid        bool
	}{
		{"disabled", "", true},
		{"trusted", "id.example", true},
		{"untrusted", "other.example", false},
		{"URL instead of hostname", "https://id.example", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Zendrite{}
			cfg.Defaults(DefaultOpts{Generate: true, SingleDatabase: true})
			cfg.Global.TrustedIDServers = []string{"id.example"}
			cfg.ClientAPI.EmailValidationServer = tc.server
			var errs ConfigErrors
			cfg.ClientAPI.Verify(&errs)
			if tc.valid {
				require.Empty(t, errs)
			} else {
				require.Contains(t, errs, "client_api.email_validation_server must be listed in global.trusted_third_party_id_servers")
			}
		})
	}
}
