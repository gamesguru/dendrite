// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import (
	"fmt"
	"net/mail"
	"net/url"
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

// Support is the response of /.well-known/matrix/support (MSC1929).
type Support struct {
	// Contacts are ways to reach the server administrators.
	Contacts []SupportContact `json:"contacts,omitempty" yaml:"contacts,omitempty"`
	// SupportPage is the URL of a page helping users of this homeserver.
	SupportPage string `json:"support_page,omitempty" yaml:"support_page,omitempty"`
}

// SupportContact is one way to reach a server administrator.
type SupportContact struct {
	// MatrixID may be an account on another homeserver, so the
	// administrator can be reached while this one is down.
	MatrixID     string `json:"matrix_id,omitempty" yaml:"matrix_id,omitempty"`
	EmailAddress string `json:"email_address,omitempty" yaml:"email_address,omitempty"`
	// Role is m.role.admin, m.role.security or a namespaced identifier.
	Role string `json:"role" yaml:"role"`
}

// Enabled reports whether /.well-known/matrix/support has anything to serve.
func (c *Support) Enabled() bool {
	return len(c.Contacts) > 0 || c.SupportPage != ""
}

func (c *Support) Verify(configErrs *ConfigErrors) {
	if c.SupportPage != "" {
		u, err := url.Parse(c.SupportPage)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			configErrs.Add(fmt.Sprintf("global.support.support_page %q must be an http or https URL", c.SupportPage))
		}
	}
	for i, contact := range c.Contacts {
		if contact.MatrixID == "" && contact.EmailAddress == "" {
			configErrs.Add(fmt.Sprintf("global.support.contacts[%d] needs matrix_id or email_address", i))
		}
		if contact.MatrixID != "" {
			if _, err := spec.NewUserID(contact.MatrixID, false); err != nil {
				configErrs.Add(fmt.Sprintf("global.support.contacts[%d].matrix_id %q is not a valid user ID", i, contact.MatrixID))
			}
		}
		if contact.EmailAddress != "" {
			if addr, err := mail.ParseAddress(contact.EmailAddress); err != nil || addr.Address != contact.EmailAddress {
				configErrs.Add(fmt.Sprintf("global.support.contacts[%d].email_address %q is not a valid email address", i, contact.EmailAddress))
			}
		}
		if !validSupportRole(contact.Role) {
			configErrs.Add(fmt.Sprintf("global.support.contacts[%d].role %q must be m.role.admin, m.role.security or a namespaced identifier", i, contact.Role))
		}
	}
}

// validSupportRole accepts the two spec roles and namespaced identifiers
// such as com.example.billing; other m. Roles are reserved.
func validSupportRole(role string) bool {
	switch {
	case role == "m.role.admin" || role == "m.role.security":
		return true
	case role == "" || strings.HasPrefix(role, "m."):
		return false
	default:
		return strings.Contains(role, ".")
	}
}
