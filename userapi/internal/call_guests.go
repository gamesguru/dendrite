// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/userapi/api"
)

// callGuestSweepBatch is how many expired call guests one query returns.
const callGuestSweepBatch = 100

// DeactivateExpiredCallGuests deactivates every call guest account created
// more than lifetime before now, through the same path as a user
// deactivating their own account: it leaves the account's rooms and deletes
// its devices, pushers and delayed events. It returns how many accounts it
// deactivated. Deactivated accounts are not selected again, so a repeated or
// concurrent sweep does no harm.
func (a *UserInternalAPI) DeactivateExpiredCallGuests(ctx context.Context, lifetime time.Duration, now time.Time) (int, error) {
	before := now.Add(-lifetime)
	deactivated := 0
	for {
		accounts, err := a.DB.GetActiveAccountsCreatedBefore(ctx, api.AccountTypeCallGuest, before, callGuestSweepBatch)
		if err != nil {
			return deactivated, fmt.Errorf("a.DB.GetActiveAccountsCreatedBefore: %w", err)
		}
		failed := 0
		for i := range accounts {
			if ctx.Err() != nil {
				return deactivated, ctx.Err()
			}
			res := api.PerformAccountDeactivationResponse{}
			if err = a.PerformAccountDeactivation(ctx, &api.PerformAccountDeactivationRequest{
				Localpart:  accounts[i].Localpart,
				ServerName: accounts[i].ServerName,
			}, &res); err != nil {
				failed++
				logrus.WithError(err).WithField("user_id", accounts[i].UserID).Error("Failed to deactivate expired call guest")
				continue
			}
			deactivated++
		}
		// A failed account would be selected again; leave it for the next
		// sweep rather than retrying it in a loop.
		if len(accounts) < callGuestSweepBatch || failed > 0 {
			return deactivated, nil
		}
	}
}
