// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package httputil

import (
	"net/http"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
)

func TestRestrictedJoinResponse(t *testing.T) {
	for _, code := range []spec.MatrixErrorCode{api.ErrorUnableToAuthoriseJoin, api.ErrorUnableToGrantJoin} {
		res := MatrixErrorResponse(spec.MatrixError{ErrCode: code, Err: "try another server"})
		require.NotNil(t, res)
		require.Equal(t, http.StatusBadRequest, res.Code)
		matrixErr, ok := res.JSON.(spec.MatrixError)
		require.True(t, ok)
		require.Equal(t, code, matrixErr.ErrCode)
	}
}
