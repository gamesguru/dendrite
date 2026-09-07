// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// TestPerformAccountReactivation covers the full deactivate/reactivate cycle
// plus the error and idempotence semantics MAS relies on.
func TestPerformAccountReactivation(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// The introspection server is never called by these tests; a plain
		// 500 responder is enough to satisfy makeTestUserAPI.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
		defer closeDB()

		serverName := userAPI.Config.Matrix.ServerName

		createAccount := func(t *testing.T, localpart string) {
			t.Helper()
			var res api.PerformAccountCreationResponse
			if err := userAPI.PerformAccountCreation(context.Background(), &api.PerformAccountCreationRequest{
				AccountType: api.AccountTypeUser,
				Localpart:   localpart,
				ServerName:  serverName,
				Password:    "test-password",
				OnConflict:  api.ConflictUpdate,
			}, &res); err != nil {
				t.Fatalf("failed to create account: %v", err)
			}
		}

		deactivated := func(t *testing.T, localpart string) bool {
			t.Helper()
			ok, err := userAPI.IsAccountDeactivated(context.Background(), localpart, serverName)
			if err != nil {
				t.Fatalf("IsAccountDeactivated failed: %v", err)
			}
			return ok
		}

		t.Run("DeactivateThenReactivate", func(t *testing.T) {
			localpart := "reactivate-cycle"
			createAccount(t, localpart)

			var deactRes api.PerformAccountDeactivationResponse
			if err := userAPI.PerformAccountDeactivation(context.Background(), &api.PerformAccountDeactivationRequest{
				Localpart:  localpart,
				ServerName: serverName,
			}, &deactRes); err != nil {
				t.Fatalf("deactivation failed: %v", err)
			}
			if !deactivated(t, localpart) {
				t.Fatal("account should be deactivated after PerformAccountDeactivation")
			}

			// Password login is refused while deactivated.
			acc, err := userAPI.DB.GetAccountByPassword(context.Background(), localpart, serverName, "test-password")
			if err == nil && acc != nil {
				t.Fatal("GetAccountByPassword should fail for a deactivated account")
			}

			var reactRes api.PerformAccountReactivationResponse
			if err := userAPI.PerformAccountReactivation(context.Background(), &api.PerformAccountReactivationRequest{
				Localpart:  localpart,
				ServerName: serverName,
			}, &reactRes); err != nil {
				t.Fatalf("reactivation failed: %v", err)
			}
			if !reactRes.AccountReactivated {
				t.Fatal("expected AccountReactivated to be true")
			}
			if deactivated(t, localpart) {
				t.Fatal("account should be active after PerformAccountReactivation")
			}

			// Password login works again after reactivation.
			acc, err = userAPI.DB.GetAccountByPassword(context.Background(), localpart, serverName, "test-password")
			if err != nil {
				t.Fatalf("GetAccountByPassword should succeed after reactivation: %v", err)
			}
			if acc == nil {
				t.Fatal("expected account after reactivation")
			}
		})

		t.Run("UnknownUserReturnsNoRows", func(t *testing.T) {
			var res api.PerformAccountReactivationResponse
			err := userAPI.PerformAccountReactivation(context.Background(), &api.PerformAccountReactivationRequest{
				Localpart:  "never-existed",
				ServerName: serverName,
			}, &res)
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("expected sql.ErrNoRows for unknown user, got %v", err)
			}
			if res.AccountReactivated {
				t.Fatal("AccountReactivated must be false for unknown user")
			}
		})

		t.Run("IdempotentOnActiveAccount", func(t *testing.T) {
			localpart := "reactivate-active"
			createAccount(t, localpart)

			var res api.PerformAccountReactivationResponse
			if err := userAPI.PerformAccountReactivation(context.Background(), &api.PerformAccountReactivationRequest{
				Localpart:  localpart,
				ServerName: serverName,
			}, &res); err != nil {
				t.Fatalf("reactivation of an active account should not error: %v", err)
			}
			if deactivated(t, localpart) {
				t.Fatal("account must remain active")
			}
		})

		t.Run("ForeignServerNameRejected", func(t *testing.T) {
			var res api.PerformAccountReactivationResponse
			err := userAPI.PerformAccountReactivation(context.Background(), &api.PerformAccountReactivationRequest{
				Localpart:  "reactivate-foreign",
				ServerName: spec.ServerName("example.org"),
			}, &res)
			if err == nil || !strings.Contains(err.Error(), "not locally configured") {
				t.Fatalf("expected foreign server name to be rejected, got %v", err)
			}
		})
	})
}
