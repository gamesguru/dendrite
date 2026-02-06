//go:build pinecone && !elementweb

// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package embed

import "codefloe.com/pat-s/dendrite/internal/httputil"

func Embed(_ *httputil.Router, _ int, _ string) {
}
