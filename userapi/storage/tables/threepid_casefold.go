// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables

import (
	"context"
	"database/sql"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
)

// FoldEmailThreePIDs case-folds the email addresses stored before MSC2265,
// which only match lookups once folded. An address whose folded form is
// already stored, for another account or as a second entry of the same one,
// is left as it is and logged, since only an administrator can tell which
// association to keep.
func FoldEmailThreePIDs(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT threepid FROM userapi_threepids WHERE medium = 'email'`)
	if err != nil {
		return err
	}
	stored := map[string]bool{}
	var addresses []string
	for rows.Next() {
		var address string
		if err = rows.Scan(&address); err != nil {
			_ = rows.Close()
			return err
		}
		stored[address] = true
		addresses = append(addresses, address)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		return err
	}

	for _, address := range addresses {
		folded := authtypes.CanonicalThreePIDAddress("email", address)
		if folded == address {
			continue
		}
		if stored[folded] {
			logrus.WithField("address", address).Warn("Not case-folding a stored email address whose folded form is already in use; remove one of the two associations")
			continue
		}
		if _, err = tx.ExecContext(ctx,
			`UPDATE userapi_threepids SET threepid = $1 WHERE threepid = $2 AND medium = 'email'`, folded, address,
		); err != nil {
			return err
		}
		delete(stored, address)
		stored[folded] = true
	}
	return nil
}
