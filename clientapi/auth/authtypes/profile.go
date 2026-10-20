// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package authtypes

import (
	"encoding/json"

	"github.com/gamesguru/gomatrixserverlib"
)

// Profile field names with their own storage and semantics. Every other field
// is a custom field (MSC4133) and is kept in Profile.Fields.
const (
	ProfileFieldDisplayName = "displayname"
	ProfileFieldAvatarURL   = "avatar_url"
)

// MaxProfileSize is the largest a profile may be, measured as canonical JSON
// over every field including the display name and avatar URL (MSC4133).
const MaxProfileSize = 64 * 1024

// Profile represents the profile for a Matrix account.
type Profile struct {
	Localpart   string `json:"local_part"`
	ServerName  string `json:"server_name,omitempty"` // NOTSPEC: only set by Pinecone user provider
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
	// Fields holds the custom profile fields (MSC4133) by name, excluding
	// the display name and avatar URL.
	Fields map[string]json.RawMessage `json:"fields,omitempty"`
}

// ClientFields returns the profile as served to clients and over federation:
// every custom field, and the display name and avatar URL when they are set.
func (p *Profile) ClientFields() map[string]any {
	fields := make(map[string]any, len(p.Fields)+2)
	for name, value := range p.Fields {
		fields[name] = value
	}
	if p.DisplayName != "" {
		fields[ProfileFieldDisplayName] = p.DisplayName
	}
	if p.AvatarURL != "" {
		fields[ProfileFieldAvatarURL] = p.AvatarURL
	}
	return fields
}

// Size returns the size of the profile in canonical JSON, the measure that
// MaxProfileSize applies to.
func (p *Profile) Size() (int, error) {
	raw, err := json.Marshal(p.ClientFields())
	if err != nil {
		return 0, err
	}
	canonical, err := gomatrixserverlib.CanonicalJSON(raw)
	if err != nil {
		return 0, err
	}
	return len(canonical), nil
}

// FullyQualifiedProfile represents the profile for a Matrix account.
type FullyQualifiedProfile struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
}
