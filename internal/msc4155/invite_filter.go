// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Package msc4155 implements the invite filtering rules from MSC4155.
package msc4155

import (
	"context"
	"encoding/json"
	"net"
	"path"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

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
// blocked or silently ignored. Any user rule overrides every server rule.
func Disposition(ctx context.Context, accountData AccountDataQuerier, invitedUser spec.UserID, sender string) (blocked, ignored bool, err error) {
	rules, err := loadRules(ctx, accountData, invitedUser)
	if err != nil || rules == nil {
		return false, false, err
	}

	_, senderServer, err := gomatrixserverlib.SplitID('@', sender)
	if err != nil {
		return false, false, err
	}

	// MSC4155: the first matching rule wins, users before servers and allow
	// before ignore before block.
	if matches(rules.AllowedUsers, sender) {
		return false, false, nil
	}
	if matches(rules.IgnoredUsers, sender) {
		return false, true, nil
	}
	if matches(rules.BlockedUsers, sender) {
		return true, false, nil
	}
	return serverRules(rules, senderServer)
}

// ServerDisposition applies only the server rules, for an invite whose sender
// cannot be resolved to a user ID but whose origin server is known.
func ServerDisposition(ctx context.Context, accountData AccountDataQuerier, invitedUser spec.UserID, server spec.ServerName) (blocked, ignored bool, err error) {
	rules, err := loadRules(ctx, accountData, invitedUser)
	if err != nil || rules == nil {
		return false, false, err
	}
	return serverRules(rules, server)
}

// serverRules matches server globs against the server name without its port.
func serverRules(rules *Rules, server spec.ServerName) (blocked, ignored bool, err error) {
	host := string(server)
	if h, _, splitErr := net.SplitHostPort(host); splitErr == nil {
		host = h
	}
	if matches(rules.AllowedServers, host) {
		return false, false, nil
	}
	if matches(rules.IgnoredServers, host) {
		return false, true, nil
	}
	if matches(rules.BlockedServers, host) {
		return true, false, nil
	}
	return false, false, nil
}

func loadRules(ctx context.Context, accountData AccountDataQuerier, invitedUser spec.UserID) (*Rules, error) {
	if accountData == nil {
		return nil, nil
	}
	var response userapi.QueryAccountDataResponse
	if err := accountData.QueryAccountData(ctx, &userapi.QueryAccountDataRequest{
		UserID:   invitedUser.String(),
		DataType: AccountDataType,
	}, &response); err != nil {
		return nil, err
	}
	raw := response.GlobalAccountData[AccountDataType]
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var rules Rules
	if err := json.Unmarshal(raw, &rules); err != nil {
		// The content is user-controlled and a retry cannot fix it, so treat
		// it as if no rules were configured.
		logrus.WithError(err).WithField("user_id", invitedUser.String()).Warn("MSC4155: ignoring malformed invite rules")
		return nil, nil
	}
	return &rules, nil
}
