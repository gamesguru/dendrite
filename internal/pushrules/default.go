package pushrules

import "github.com/gamesguru/gomatrixserverlib/spec"

// DefaultAccountRuleSets is the complete set of default push rules
// for an account.
func DefaultAccountRuleSets(localpart string, serverName spec.ServerName) *AccountRuleSets {
	return &AccountRuleSets{
		Global: *DefaultGlobalRuleSet(localpart, serverName),
	}
}

// DefaultGlobalRuleSet returns the default ruleset for a given (fully
// qualified) MXID.
func DefaultGlobalRuleSet(localpart string, serverName spec.ServerName) *RuleSet {
	return &RuleSet{
		Override:  defaultOverrideRules("@" + localpart + ":" + string(serverName)),
		Content:   defaultContentRules(localpart),
		Underride: defaultUnderrideRules,
	}
}

// UpgradeDefaultRules supplies default rules introduced after the stored
// rules were created. Existing rules, actions, enablement and custom
// ordering are left intact.
func UpgradeDefaultRules(rules *RuleSet, localpart string, serverName spec.ServerName) {
	AddMentionRules(rules, localpart, serverName)
	for _, rule := range rules.Override {
		if rule.RuleID == MRuleSuppressEdits {
			return
		}
	}
	// The spec places it after every other default override rule, and user
	// rules of the same kind take precedence over server defaults.
	rules.Override = append(rules.Override, mRuleSuppressEditsDefinition())
}

// AddMentionRules supplies newly introduced defaults when reading stored rules.
// Existing rules, actions, enablement and custom ordering are left intact.
// New rules inherit the corresponding legacy preference on the first upgrade.
func AddMentionRules(rules *RuleSet, localpart string, serverName spec.ServerName) {
	for _, addition := range []struct {
		rule   *Rule
		legacy string
	}{
		{mRuleIsUserMentionDefinition("@" + localpart + ":" + string(serverName)), MRuleContainsDisplayName},
		{mRuleIsRoomMentionDefinition(), MRuleRoomNotif},
	} {
		exists := false
		for _, rule := range rules.Override {
			if rule.RuleID == addition.rule.RuleID {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		index := len(rules.Override)
		for i, rule := range rules.Override {
			if rule.RuleID == addition.legacy {
				addition.rule.Enabled = rule.Enabled
				addition.rule.Actions = rule.Actions
				index = i
				break
			}
			if index == len(rules.Override) && rule.Default && (rule.RuleID == MRuleTombstone || rule.RuleID == MRuleReaction || rule.RuleID == MRuleRoomACLs) {
				index = i
			}
		}
		rules.Override = append(rules.Override, nil)
		copy(rules.Override[index+1:], rules.Override[index:])
		rules.Override[index] = addition.rule
	}
}
