// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package httputil

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

func TestParseTSParamOnlyForAppServices(t *testing.T) {
	appService := &userapi.Device{UserID: "@bridge:test", AppserviceID: "bridge", AccountType: userapi.AccountTypeAppService}
	user := &userapi.Device{UserID: "@alice:test", AccountType: userapi.AccountTypeUser}

	ts, err := ParseTSParam(httptest.NewRequest("PUT", "/?ts=1000", nil), appService)
	require.NoError(t, err)
	require.Equal(t, time.UnixMilli(1000), ts)

	_, err = ParseTSParam(httptest.NewRequest("PUT", "/?ts=x", nil), appService)
	require.Error(t, err)

	// Other users cannot backdate events; the parameter is ignored.
	for _, device := range []*userapi.Device{user, nil} {
		ts, err = ParseTSParam(httptest.NewRequest("PUT", "/?ts=1000", nil), device)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), ts, time.Minute)
	}
}
