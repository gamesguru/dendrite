// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

func TestDeactivateExpiredCallGuests(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
		defer closeDB()
		ctx := context.Background()
		serverName := userAPI.Config.Matrix.ServerName

		create := func(localpart string, accountType api.AccountType) string {
			t.Helper()
			var res api.PerformAccountCreationResponse
			if err := userAPI.PerformAccountCreation(ctx, &api.PerformAccountCreationRequest{
				AccountType: accountType,
				Localpart:   localpart,
				ServerName:  serverName,
				Password:    "test-password",
				OnConflict:  api.ConflictAbort,
			}, &res); err != nil {
				t.Fatalf("failed to create account: %v", err)
			}
			return res.Account.Localpart
		}
		login := func(localpart string) {
			t.Helper()
			var res api.PerformDeviceCreationResponse
			if err := userAPI.PerformDeviceCreation(ctx, &api.PerformDeviceCreationRequest{
				Localpart:   localpart,
				ServerName:  serverName,
				AccessToken: "token-" + localpart,
			}, &res); err != nil {
				t.Fatalf("failed to create device: %v", err)
			}
		}
		deactivated := func(localpart string) bool {
			t.Helper()
			ok, err := userAPI.IsAccountDeactivated(ctx, localpart, serverName)
			if err != nil {
				t.Fatalf("IsAccountDeactivated failed: %v", err)
			}
			return ok
		}

		expired := create("expired", api.AccountTypeCallGuest)
		login(expired)
		user := create("user", api.AccountTypeUser)
		admin := create("admin", api.AccountTypeAdmin)
		guest := create("", api.AccountTypeGuest)
		createdBy := time.Now()
		time.Sleep(20 * time.Millisecond)
		fresh := create("fresh", api.AccountTypeCallGuest)

		// Nothing is a day old yet.
		n, err := userAPI.DeactivateExpiredCallGuests(ctx, 24*time.Hour, time.Now())
		if err != nil || n != 0 {
			t.Fatalf("got %d, %v; want nothing deactivated", n, err)
		}

		// An hour after the first accounts were made, only the call guest
		// among them has expired.
		now := createdBy.Add(time.Hour + 5*time.Millisecond)
		n, err = userAPI.DeactivateExpiredCallGuests(ctx, time.Hour, now)
		if err != nil || n != 1 {
			t.Fatalf("got %d, %v; want one call guest deactivated", n, err)
		}
		if !deactivated(expired) {
			t.Fatal("the expired call guest is still active")
		}
		for _, localpart := range []string{fresh, user, admin, guest} {
			if deactivated(localpart) {
				t.Fatalf("%q was deactivated", localpart)
			}
		}
		var devices api.QueryDevicesResponse
		if err = userAPI.QueryDevices(ctx, &api.QueryDevicesRequest{UserID: "@" + expired + ":" + string(serverName)}, &devices); err != nil {
			t.Fatal(err)
		}
		if len(devices.Devices) != 0 {
			t.Fatalf("the expired call guest kept its devices: %+v", devices.Devices)
		}

		// A second sweep finds nothing left to do.
		n, err = userAPI.DeactivateExpiredCallGuests(ctx, time.Hour, now)
		if err != nil || n != 0 {
			t.Fatalf("got %d, %v; want nothing deactivated", n, err)
		}
	})
}
