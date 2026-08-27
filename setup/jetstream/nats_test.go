package jetstream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"codefloe.com/pat-s/zendrite/setup/config"
)

func TestNATSConnectOptions(t *testing.T) {
	t.Parallel()

	t.Run("disables TLS validation when configured", func(t *testing.T) {
		t.Parallel()
		cfg := &config.JetStream{DisableTLSValidation: true}
		opts, err := natsConnectOptions(nil, cfg, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var nopts natsclient.Options
		for _, opt := range opts {
			if err := opt(&nopts); err != nil {
				t.Fatalf("applying option: %v", err)
			}
		}

		if !nopts.Secure {
			t.Fatal("expected Secure to be true")
		}
		if nopts.TLSConfig == nil || !nopts.TLSConfig.InsecureSkipVerify {
			t.Fatal("expected InsecureSkipVerify to be true")
		}
	})

	t.Run("missing credentials file returns a clear error", func(t *testing.T) {
		t.Parallel()
		cfg := &config.JetStream{
			Addresses:   []string{"localhost:4222"},
			Credentials: config.Path("/does/not/exist.creds"),
		}
		_, err := natsConnectOptions(nil, cfg, nil)
		if err == nil {
			t.Fatal("expected an error for missing credentials file")
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("invalid credentials file returns an error", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.creds")
		if err := os.WriteFile(path, []byte("not a valid creds file"), 0o600); err != nil {
			t.Fatalf("writing creds file: %v", err)
		}

		cfg := &config.JetStream{
			Addresses:   []string{"localhost:4222"},
			Credentials: config.Path(path),
		}
		_, err := natsConnectOptions(nil, cfg, nil)
		if err == nil {
			t.Fatal("expected an error for invalid credentials file")
		}
	})

	t.Run("credentials are re-read on each callback for rotation", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "rotate.creds")

		jwt1 := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
		jwt2 := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI5ODc2NTQzMjEwIiwibmFtZSI6IkphbmUgRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.cBjXUf2e2fC1cWz9n4v0x8m3kL7pQrStUvWxyZ12345"
		seed1 := newUserSeed(t)
		seed2 := newUserSeed(t)

		writeCredsFile(t, path, jwt1, seed1)

		cfg := &config.JetStream{
			Addresses:   []string{"localhost:4222"},
			Credentials: config.Path(path),
		}
		opts, err := natsConnectOptions(nil, cfg, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var nopts natsclient.Options
		for _, opt := range opts {
			if err := opt(&nopts); err != nil {
				t.Fatalf("applying option: %v", err)
			}
		}
		if nopts.UserJWT == nil || nopts.SignatureCB == nil {
			t.Fatal("expected JWT and signature callbacks to be set")
		}

		gotJWT, err := nopts.UserJWT()
		if err != nil {
			t.Fatalf("first UserJWT call failed: %v", err)
		}
		if gotJWT != jwt1 {
			t.Fatalf("expected first JWT %q, got %q", jwt1, gotJWT)
		}
		sig1, err := nopts.SignatureCB([]byte("challenge"))
		if err != nil {
			t.Fatalf("first SignatureCB call failed: %v", err)
		}
		if len(sig1) == 0 {
			t.Fatal("expected non-empty first signature")
		}

		// Simulate a credential rotation by overwriting the file.
		writeCredsFile(t, path, jwt2, seed2)

		gotJWT, err = nopts.UserJWT()
		if err != nil {
			t.Fatalf("second UserJWT call failed: %v", err)
		}
		if gotJWT != jwt2 {
			t.Fatalf("expected second JWT %q, got %q", jwt2, gotJWT)
		}
		sig2, err := nopts.SignatureCB([]byte("challenge"))
		if err != nil {
			t.Fatalf("second SignatureCB call failed: %v", err)
		}
		if len(sig2) == 0 {
			t.Fatal("expected non-empty second signature")
		}

		// The signatures should be different because the underlying seed changed.
		if string(sig1) == string(sig2) {
			t.Fatal("expected signatures to differ after credential rotation")
		}

		// Sanity-check that the new seed is actually being used by verifying the
		// second signature against the key derived from the rotated seed.
		kp2, err := nkeys.FromSeed([]byte(seed2))
		if err != nil {
			t.Fatalf("loading second seed: %v", err)
		}
		if err := kp2.Verify([]byte("challenge"), sig2); err != nil {
			t.Fatalf("second signature did not verify with rotated key: %v", err)
		}
	})
}

func TestConnectToNATSWithURLCredentials(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	opts := &natsserver.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		Username:   "zendrite",
		Password:   "secret",
		JetStream:  true,
		StoreDir:   dir,
		NoLog:      true,
		NoSigs:     true,
		SyncAlways: true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("creating NATS server: %v", err)
	}
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(time.Second * 5) {
		t.Fatal("NATS server did not start in time")
	}

	url := fmt.Sprintf("nats://zendrite:secret@%s", srv.Addr().String())
	t.Run("connects with username and password in URL", func(t *testing.T) {
		t.Parallel()
		nc, err := natsclient.Connect(url, natsclient.MaxReconnects(0))
		if err != nil {
			t.Fatalf("connecting with URL credentials: %v", err)
		}
		defer nc.Close()
	})

	t.Run("rejects wrong password", func(t *testing.T) {
		t.Parallel()
		badURL := fmt.Sprintf("nats://zendrite:wrong@%s", srv.Addr().String())
		nc, err := natsclient.Connect(badURL, natsclient.MaxReconnects(0))
		if err == nil {
			nc.Close()
			t.Fatal("expected authentication error for wrong password")
		}
		if !errors.Is(err, natsclient.ErrAuthorization) {
			t.Fatalf("expected ErrAuthorization, got %v", err)
		}
	})
}

func TestIsAuthError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"authorization", natsclient.ErrAuthorization, true},
		{"auth expired", natsclient.ErrAuthExpired, true},
		{"auth revoked", natsclient.ErrAuthRevoked, true},
		{"account auth expired", natsclient.ErrAccountAuthExpired, true},
		{"other error", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isAuthError(tc.err); got != tc.want {
				t.Fatalf("isAuthError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// newUserSeed creates a new user nkey seed for use in test credentials.
func newUserSeed(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("creating user nkey: %v", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatalf("getting seed: %v", err)
	}
	return string(seed)
}

// writeCredsFile writes a NATS .creds file for use in tests.
func writeCredsFile(t *testing.T, path, jwt, seed string) {
	t.Helper()
	content := fmt.Sprintf(`-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------

*************************************************************
`, jwt, seed)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing creds file: %v", err)
	}
}
