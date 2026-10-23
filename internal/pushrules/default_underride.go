package pushrules

const (
	MRuleCall                  = ".m.rule.call"
	MRuleEncryptedRoomOneToOne = ".m.rule.encrypted_room_one_to_one"
	MRuleRoomOneToOne          = ".m.rule.room_one_to_one"
	MRuleMessage               = ".m.rule.message"
	MRuleEncrypted             = ".m.rule.encrypted"
	MRulePollStartOneToOne     = ".org.matrix.msc3930.rule.poll_start_one_to_one"
	MRulePollEndOneToOne       = ".org.matrix.msc3930.rule.poll_end_one_to_one"
	MRulePollStart             = ".org.matrix.msc3930.rule.poll_start"
	MRulePollEnd               = ".org.matrix.msc3930.rule.poll_end"
)

var defaultUnderrideRules = []*Rule{
	&mRulePollStartOneToOneDefinition,
	&mRulePollEndOneToOneDefinition,
	&mRulePollStartDefinition,
	&mRulePollEndDefinition,
	&mRuleCallDefinition,
	&mRuleRoomOneToOneDefinition,
	&mRuleEncryptedRoomOneToOneDefinition,
	&mRuleMessageDefinition,
	&mRuleEncryptedDefinition,
}

var (
	mRulePollStartOneToOneDefinition = Rule{
		RuleID:  MRulePollStartOneToOne,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{Kind: RoomMemberCountCondition, Is: "2"},
			{Kind: EventMatchCondition, Key: "type", Pattern: pointer("org.matrix.msc3381.poll.start")},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{Kind: SetTweakAction, Tweak: SoundTweak, Value: "default"},
		},
	}
	mRulePollEndOneToOneDefinition = Rule{
		RuleID:  MRulePollEndOneToOne,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{Kind: RoomMemberCountCondition, Is: "2"},
			{Kind: EventMatchCondition, Key: "type", Pattern: pointer("org.matrix.msc3381.poll.end")},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{Kind: SetTweakAction, Tweak: SoundTweak, Value: "default"},
		},
	}
	mRulePollStartDefinition = Rule{
		RuleID:  MRulePollStart,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{Kind: EventMatchCondition, Key: "type", Pattern: pointer("org.matrix.msc3381.poll.start")},
		},
		Actions: []*Action{{Kind: NotifyAction}},
	}
	mRulePollEndDefinition = Rule{
		RuleID:  MRulePollEnd,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{Kind: EventMatchCondition, Key: "type", Pattern: pointer("org.matrix.msc3381.poll.end")},
		},
		Actions: []*Action{{Kind: NotifyAction}},
	}
	mRuleCallDefinition = Rule{
		RuleID:  MRuleCall,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.call.invite"),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: SoundTweak,
				Value: "ring",
			},
		},
	}
	mRuleEncryptedRoomOneToOneDefinition = Rule{
		RuleID:  MRuleEncryptedRoomOneToOne,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind: RoomMemberCountCondition,
				Is:   "2",
			},
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.encrypted"),
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
	mRuleRoomOneToOneDefinition = Rule{
		RuleID:  MRuleRoomOneToOne,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind: RoomMemberCountCondition,
				Is:   "2",
			},
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.message"),
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
	mRuleMessageDefinition = Rule{
		RuleID:  MRuleMessage,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.message"),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
		},
	}
	mRuleEncryptedDefinition = Rule{
		RuleID:  MRuleEncrypted,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.room.encrypted"),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
		},
	}
)
