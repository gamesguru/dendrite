// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Package msc4155 implements the invite filtering rules from MSC4155.
package msc4155

import (
	"context"
	"encoding/json"
	"path"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// AccountDataType is the global account data event holding invite rules.
const AccountDataType = "org.matrix.msc4155.invite_permission_config"

// Rules is the account data content for MSC4155.
type Rules struct {
	AllowedUsers   []string `json:"allowed_users,omitempty"`
	IgnoredUsers   []string `json:"ignored_users,omitempty"`
	BlockedUsers   []string `json:"blocked_users,omitempty"`
	AllowedServers []string `json:"allowed_servers,omitempty"`
	IgnoredServers []string `json:"ignored_servers,omitempty"`
	BlockedServers []string `json:"blocked_servers,omitempty"`
}

// AccountDataQuerier is the subset of the user API needed to read invite rules.
type AccountDataQuerier interface {
	QueryAccountData(ctx context.Context, req *userapi.QueryAccountDataRequest, res *userapi.QueryAccountDataResponse) error
}

func matches(patterns []string, value string) bool {
	for _, pattern := range patterns {
		if match, err := path.Match(pattern, value); err == nil && match {
			return true
		}
	}
	return false
}

// Disposition reports whether an invite to invitedUser from sender should be
// blocked or silently ignored. An explicit user allow overrides user and
// server deny rules; a user block overrides a server allow.
func Disposition(ctx context.Context, accountData AccountDataQuerier, invitedUser spec.UserID, sender string) (blocked, ignored bool, err error) {
	if accountData == nil {
		return false, false, nil
	}

	var response userapi.QueryAccountDataResponse
	if err := accountData.QueryAccountData(ctx, &userapi.QueryAccountDataRequest{
		UserID:   invitedUser.String(),
		DataType: AccountDataType,
	}, &response); err != nil {
		return false, false, err
	}
	raw := response.GlobalAccountData[AccountDataType]
	if len(raw) == 0 || string(raw) == "null" {
		return false, false, nil
	}

	var rules Rules
	if err := json.Unmarshal(raw, &rules); err != nil {
		return false, false, err
	}

	_, senderServer, err := gomatrixserverlib.SplitID('@', sender)
	if err != nil {
		return false, false, err
	}

	if matches(rules.AllowedUsers, sender) {
		return false, false, nil
	}
	if matches(rules.BlockedUsers, sender) {
		return true, false, nil
	}
	if matches(rules.AllowedServers, string(senderServer)) {
		return false, false, nil
	}
	if matches(rules.BlockedServers, string(senderServer)) {
		return true, false, nil
	}
	if matches(rules.IgnoredUsers, sender) || matches(rules.IgnoredServers, string(senderServer)) {
		return false, true, nil
	}
	return false, false, nil
}
