// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import (
	"fmt"
	"net/url"
	"regexp"
)

// RegistrationPolicy is a policy document that new users must accept.
type RegistrationPolicy struct {
	// ID identifies the policy, such as terms_of_service.
	ID string `yaml:"id"`
	// Version of the policy document, such as 1.2.
	Version string `yaml:"version"`
	// Translations maps language codes, such as en or fr, to the document
	// in that language.
	Translations map[string]PolicyTranslation `yaml:"translations"`
}

// PolicyTranslation is a policy document in one language.
type PolicyTranslation struct {
	Name string `yaml:"name" json:"name"`
	URL  string `yaml:"url" json:"url"`
}

// opaqueIDRegex is the spec's opaque identifier grammar.
var opaqueIDRegex = regexp.MustCompile(`^[A-Za-z0-9._~-]{1,255}$`)

// RegistrationTermsParams returns the params of the m.login.terms stage:
// the policies keyed by ID, each with its version and translations.
func RegistrationTermsParams(policies []RegistrationPolicy) map[string]any {
	byID := make(map[string]any, len(policies))
	for _, policy := range policies {
		entry := map[string]any{"version": policy.Version}
		for lang, translation := range policy.Translations {
			entry[lang] = translation
		}
		byID[policy.ID] = entry
	}
	return map[string]any{"policies": byID}
}

func verifyRegistrationTerms(configErrs *ConfigErrors, policies []RegistrationPolicy) {
	seen := make(map[string]bool, len(policies))
	for i, policy := range policies {
		prefix := fmt.Sprintf("client_api.registration_terms[%d]", i)
		if !opaqueIDRegex.MatchString(policy.ID) {
			configErrs.Add(fmt.Sprintf("%s.id %q must be 1 to 255 characters of A-Z, a-z, 0-9, ., _, ~ and -", prefix, policy.ID))
		} else if seen[policy.ID] {
			configErrs.Add(fmt.Sprintf("%s.id %q is used twice", prefix, policy.ID))
		}
		seen[policy.ID] = true
		if !opaqueIDRegex.MatchString(policy.Version) {
			configErrs.Add(fmt.Sprintf("%s.version %q must be 1 to 255 characters of A-Z, a-z, 0-9, ., _, ~ and -", prefix, policy.Version))
		}
		if len(policy.Translations) == 0 {
			configErrs.Add(prefix + ".translations needs at least one language")
		}
		for lang, translation := range policy.Translations {
			// "version" is a key of its own in the policy object.
			if lang == "" || lang == "version" {
				configErrs.Add(fmt.Sprintf("%s.translations has an invalid language %q", prefix, lang))
			}
			if translation.Name == "" {
				configErrs.Add(fmt.Sprintf("%s.translations.%s.name is required", prefix, lang))
			}
			u, err := url.Parse(translation.URL)
			if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
				configErrs.Add(fmt.Sprintf("%s.translations.%s.url %q must be an http or https URL", prefix, lang, translation.URL))
			}
		}
	}
}
