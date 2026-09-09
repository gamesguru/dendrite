package pushrules

import "codefloe.com/pat-s/gomatrixserverlib/spec"

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
