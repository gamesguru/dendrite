// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"net/http"
	"time"

	altcha "github.com/altcha-org/altcha-lib-go"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// ServeAltchaChallenge generates a proof-of-work challenge using the
// ALTCHA protocol and writes it as JSON to the response.
func ServeAltchaChallenge(w http.ResponseWriter, req *http.Request, cfg *config.ClientAPI) {
	const defaultExpiry = 5 * time.Minute
	expiry := defaultExpiry
	if cfg.AltchaExpiry != "" {
		if d, err := time.ParseDuration(cfg.AltchaExpiry); err == nil {
			expiry = d
		}
	}
	expires := time.Now().Add(expiry)

	challenge, err := altcha.CreateChallenge(altcha.ChallengeOptions{
		HMACKey:   cfg.AltchaHMACKey,
		MaxNumber: cfg.AltchaMaxNumber,
		Expires:   &expires,
	})
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to create ALTCHA challenge")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(challenge); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to write ALTCHA challenge response")
	}
}

// validateAltcha verifies an ALTCHA proof-of-work solution locally.
// The response parameter is the Base64-encoded payload from the client.
func validateAltcha(cfg *config.ClientAPI, response string) error {
	if !cfg.RecaptchaEnabled {
		return ErrCaptchaDisabled
	}
	if response == "" {
		return ErrMissingResponse
	}

	ok, err := altcha.VerifySolution(response, cfg.AltchaHMACKey, true)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInvalidCaptcha
	}
	return nil
}
