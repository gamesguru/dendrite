// Copyright 2017-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"

	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/internal"
)

type version struct {
	Server           server          `json:"server"`
	UnstableFeatures map[string]bool `json:"unstable_features,omitempty"`
}

type server struct {
	Version string `json:"version"`
	Name    string `json:"name"`
}

// Version returns the server version. msc4521 advertises the MSC4521 digest
// profile when this build serves it and the deployment enabled it.
func Version(msc4521 bool) util.JSONResponse {
	v := &version{
		Server: server{
			Name:    "Zendrite",
			Version: internal.VersionString(),
		},
	}
	if msc4521 {
		v.UnstableFeatures = map[string]bool{
			"tk.nutra.msc4521.digest.algebraic_v1": true,
			"tk.nutra.msc0501.reconciliation":      true,
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: v,
	}
}
