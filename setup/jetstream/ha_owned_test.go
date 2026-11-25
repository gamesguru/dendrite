// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package jetstream

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

type invalidOwner struct{}

func (invalidOwner) Generation() int64 { return 1 }
func (invalidOwner) Identity() string  { return "former" }
func (invalidOwner) Valid() error      { return errors.New("ownership lost") }
func (invalidOwner) Retired() bool     { return true }

// A former owner must be refused before the message is touched; the nil message
// would panic if the fence let the call through.
func TestOwnedAckAndProgressRefuseFormerOwner(t *testing.T) {
	var msg *nats.Msg
	require.ErrorContains(t, AckOwned(t.Context(), invalidOwner{}, msg), "ownership lost")
	require.ErrorContains(t, ProgressOwned(invalidOwner{}, msg), "ownership lost")
}
