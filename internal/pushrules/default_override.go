package pushrules

import "encoding/json"

func defaultOverrideRules(userID string) []*Rule {
	return []*Rule{
		&mRuleMasterDefinition,
		&mRuleSuppressNoticesDefinition,
		mRuleInviteForMeDefinition(userID),
		&mRuleMemberEventDefinition,
		mRuleIsUserMentionDefinition(userID),
		mRuleIsRoomMentionDefinition(),
		&mRuleContainsDisplayNameDefinition,
		&mRuleRoomNotifDefinition,
		&mRuleTombstoneDefinition,
		&mRuleReactionDefinition,
		&mRuleACLsDefinition,
		mRuleSuppressEditsDefinition(),
	}
}

const (
	MRuleIsUserMention       = ".m.rule.is_user_mention"
	MRuleIsRoomMention       = ".m.rule.is_room_mention"
	MRuleMaster              = ".m.rule.master"
	MRuleSuppressNotices     = ".m.rule.suppress_notices"
	MRuleInviteForMe         = ".m.rule.invite_for_me"
	MRuleMemberEvent         = ".m.rule.member_event"
	MRuleContainsDisplayName = ".m.rule.contains_display_name"
	MRuleTombstone           = ".m.rule.tombstone"
	MRuleRoomNotif           = ".m.rule.roomnotif"
	MRuleReaction            = ".m.rule.reaction"
	MRuleRoomACLs            = ".m.rule.room.server_acl"
	MRuleSuppressEdits       = ".m.rule.suppress_edits"
)

var (
	mRuleMasterDefinition = Rule{
		RuleID:  MRuleMaster,
		Default: true,
		Enabled: false,
		Actions: []*Action{},
	}
	mRuleSuppressNoticesDefinition = Rule{
		RuleID:  MRuleSuppressNotices,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "content.msgtype",
				Pattern: pointer("m.notice"),
			},
		},
		Actions: []*Action{},
	}
	mRuleMemberEventDefinition = Rule{
		RuleID:  MRuleMemberEvent,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.member"),
			},
		},
		Actions: []*Action{},
	}
	mRuleContainsDisplayNameDefinition = Rule{
		RuleID:     MRuleContainsDisplayName,
		Default:    true,
		Enabled:    true,
		Conditions: []*Condition{{Kind: ContainsDisplayNameCondition}},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: SoundTweak,
				Value: "default",
			},
			{
				Kind:  SetTweakAction,
				Tweak: HighlightTweak,
			},
		},
	}
	mRuleTombstoneDefinition = Rule{
		RuleID:  MRuleTombstone,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.tombstone"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "state_key",
				Pattern: pointer(""),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: HighlightTweak,
			},
		},
	}
	mRuleACLsDefinition = Rule{
		RuleID:  MRuleRoomACLs,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.server_acl"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "state_key",
				Pattern: pointer(""),
			},
		},
		Actions: []*Action{},
	}
	mRuleRoomNotifDefinition = Rule{
		RuleID:  MRuleRoomNotif,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "content.body",
				Pattern: pointer("@room"),
			},
			{
				Kind: SenderNotificationPermissionCondition,
				Key:  "room",
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: HighlightTweak,
			},
		},
	}
	mRuleReactionDefinition = Rule{
		RuleID:  MRuleReaction,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.reaction"),
			},
		},
		Actions: []*Action{},
	}
)

func mRuleInviteForMeDefinition(userID string) *Rule {
	return &Rule{
		RuleID:  MRuleInviteForMe,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.member"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "content.membership",
				Pattern: pointer("invite"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "state_key",
				Pattern: pointer(userID),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: SoundTweak,
				Value: "default",
			},
		},
	}
}

func mRuleIsUserMentionDefinition(userID string) *Rule {
	value, err := json.Marshal(userID)
	if err != nil {
		panic(err)
	}
	return &Rule{
		RuleID: MRuleIsUserMention, Default: true, Enabled: true,
		Conditions: []*Condition{{Kind: EventPropertyContainsCondition, Key: `content.m\.mentions.user_ids`, Value: value}},
		Actions:    []*Action{{Kind: NotifyAction}, {Kind: SetTweakAction, Tweak: SoundTweak, Value: "default"}, {Kind: SetTweakAction, Tweak: HighlightTweak}},
	}
}

// mRuleSuppressEditsDefinition stops event replacements from notifying, as
// added in v1.9 of the spec. It is the last default override rule.
func mRuleSuppressEditsDefinition() *Rule {
	return &Rule{
		RuleID: MRuleSuppressEdits, Default: true, Enabled: true,
		Conditions: []*Condition{{
			Kind:  EventPropertyIsCondition,
			Key:   `content.m\.relates_to.rel_type`,
			Value: json.RawMessage(`"m.replace"`),
		}},
		Actions: []*Action{},
	}
}

func mRuleIsRoomMentionDefinition() *Rule {
	return &Rule{
		RuleID: MRuleIsRoomMention, Default: true, Enabled: true,
		Conditions: []*Condition{{Kind: EventPropertyIsCondition, Key: `content.m\.mentions.room`, Value: json.RawMessage(`true`)}, {Kind: SenderNotificationPermissionCondition, Key: "room"}},
		Actions:    []*Action{{Kind: NotifyAction}, {Kind: SetTweakAction, Tweak: HighlightTweak}},
	}
}
