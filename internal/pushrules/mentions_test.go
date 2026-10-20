package pushrules

import (
	"encoding/json"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/google/go-cmp/cmp"
)

type mentionContext struct {
	fakeEvaluationContext
	permitted bool
}

func (c mentionContext) HasPowerLevel(_ spec.SenderID, key string) (bool, error) {
	return c.permitted && key == "room", nil
}

func TestMentions(t *testing.T) {
	for _, tc := range []struct {
		name, content, want string
		permitted           bool
	}{
		{"user", `{"m.mentions":{"user_ids":["@alice:example.com"]}}`, MRuleIsUserMention, false},
		{"other user", `{"body":"alice Dear User @room","m.mentions":{"user_ids":["@bob:example.com"]}}`, MRuleMessage, true},
		{"exact user", `{"m.mentions":{"user_ids":["@alice:example.com.extra"]}}`, MRuleMessage, false},
		{"room allowed", `{"m.mentions":{"room":true}}`, MRuleIsRoomMention, true},
		{"room denied", `{"m.mentions":{"room":true}}`, MRuleMessage, false},
		{"room false", `{"m.mentions":{"room":false}}`, MRuleMessage, true},
		{"room string", `{"m.mentions":{"room":"true"}}`, MRuleMessage, true},
		{"empty mentions", `{"body":"alice Dear User @room","m.mentions":{}}`, MRuleMessage, true},
		{"null mentions", `{"body":"alice Dear User @room","m.mentions":null}`, MRuleMessage, true},
		{"legacy display name", `{"body":"Dear User"}`, MRuleContainsDisplayName, false},
		{"legacy localpart", `{"body":"alice"}`, MRuleContainsUserName, false},
		{"legacy room", `{"body":"@room"}`, MRuleRoomNotif, true},
		{"legacy room denied", `{"body":"@room"}`, MRuleMessage, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := mustEventFromJSON(t, `{"room_id":"!room:example.com","type":"m.room.message","content":`+tc.content+`}`)
			rule, err := NewRuleSetEvaluator(mentionContext{fakeEvaluationContext{3}, tc.permitted}, DefaultGlobalRuleSet("alice", "example.com")).MatchEvent(event, UserIDForSender)
			if err != nil {
				t.Fatal(err)
			}
			if rule == nil || rule.RuleID != tc.want {
				t.Fatalf("got %+v, want %s", rule, tc.want)
			}
		})
	}
}

func TestPropertyConditions(t *testing.T) {
	for _, tc := range []struct {
		name, key, value, content string
		kind                      ConditionKind
		want                      bool
	}{
		{"escaped dot", `content.m\.mentions.room`, `true`, `{"m.mentions":{"room":true}}`, EventPropertyIsCondition, true},
		{"escaped slash", `content.a\\b`, `"yes"`, `{"a\\b":"yes"}`, EventPropertyIsCondition, true},
		{"nested", `content.a.b`, `1`, `{"a":{"b":1}}`, EventPropertyIsCondition, true},
		{"null", `content.a`, `null`, `{"a":null}`, EventPropertyIsCondition, true},
		{"absent", `content.a`, `null`, `{}`, EventPropertyIsCondition, false},
		{"missing value", `content.a`, ``, `{"a":null}`, EventPropertyIsCondition, false},
		{"typed", `content.a`, `true`, `{"a":1}`, EventPropertyIsCondition, false},
		{"case sensitive", `content.a`, `"A"`, `{"a":"a"}`, EventPropertyIsCondition, false},
		{"no glob", `content.a`, `"a*"`, `{"a":"abc"}`, EventPropertyIsCondition, false},
		{"compound", `content.a`, `{}`, `{"a":{}}`, EventPropertyIsCondition, false},
		{"fraction", `content.a`, `1.5`, `{"a":1.5}`, EventPropertyIsCondition, false},
		{"out of range", `content.a`, `9007199254740992`, `{"a":9007199254740992}`, EventPropertyIsCondition, false},
		{"array null", `content.a`, `null`, `{"a":[{},[],null]}`, EventPropertyContainsCondition, true},
		{"array integer", `content.a`, `2`, `{"a":["2",2]}`, EventPropertyContainsCondition, true},
		{"array typed", `content.a`, `2`, `{"a":["2"]}`, EventPropertyContainsCondition, false},
		{"not array", `content.a`, `"abc"`, `{"a":"abc"}`, EventPropertyContainsCondition, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cond := &Condition{Kind: tc.kind, Key: tc.key, Value: json.RawMessage(tc.value)}
			event := mustEventFromJSON(t, `{"room_id":"!room:a","content":`+tc.content+`}`)
			got, err := conditionMatches(cond, event, fakeEvaluationContext{3})
			if err != nil || got != tc.want {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestAddMentionRules(t *testing.T) {
	rules := DefaultGlobalRuleSet("alice", "example.com")
	var old []*Rule
	for _, rule := range rules.Override {
		if rule.RuleID == MRuleIsUserMention || rule.RuleID == MRuleIsRoomMention {
			continue
		}
		clone := *rule
		if rule.RuleID == MRuleContainsDisplayName {
			clone.Enabled = false
			clone.Actions = []*Action{{Kind: DontNotifyAction}}
		}
		old = append(old, &clone)
	}
	custom := &Rule{RuleID: "custom", Enabled: true, Actions: []*Action{{Kind: NotifyAction}}}
	rules.Override = append([]*Rule{custom}, old...)
	AddMentionRules(rules, "alice", "example.com")
	if rules.Override[0] != custom {
		t.Fatal("custom rule moved")
	}
	for _, rule := range rules.Override {
		if rule.RuleID == MRuleIsUserMention && (rule.Enabled || rule.Actions[0].Kind != DontNotifyAction) {
			t.Fatal("lost legacy preferences")
		}
	}
	before, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	AddMentionRules(rules, "alice", "example.com")
	after, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(string(before), string(after)); diff != "" {
		t.Fatalf("not idempotent: %s", diff)
	}
}

func TestMentionsKeepCustomKeywords(t *testing.T) {
	rules := DefaultGlobalRuleSet("alice", "example.com")
	rules.Content = append([]*Rule{{RuleID: "keyword", Enabled: true, Pattern: pointer("cake"), Actions: []*Action{{Kind: NotifyAction}}}}, rules.Content...)
	event := mustEventFromJSON(t, `{"room_id":"!room:a","type":"m.room.message","content":{"body":"cake","m.mentions":{}}}`)
	rule, err := NewRuleSetEvaluator(fakeEvaluationContext{3}, rules).MatchEvent(event, UserIDForSender)
	if err != nil {
		t.Fatal(err)
	}
	if rule == nil || rule.RuleID != "keyword" {
		t.Fatalf("got %+v, want custom keyword", rule)
	}
}

func TestPropertyConditionValidation(t *testing.T) {
	for _, kind := range []ConditionKind{EventPropertyIsCondition, EventPropertyContainsCondition} {
		for _, value := range []string{`null`, `true`, `false`, `0`, `""`} {
			cond := &Condition{Kind: kind, Key: "content.a", Value: json.RawMessage(value)}
			if errs := validateCondition(cond); len(errs) != 0 {
				t.Fatal(errs)
			}
			data, err := json.Marshal(cond)
			if err != nil {
				t.Fatal(err)
			}
			var decoded Condition
			if err = json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if string(decoded.Value) != value {
				t.Fatalf("value %s lost in %s", value, data)
			}
		}
		for _, value := range []string{"", `[]`, `{}`, `0.5`} {
			if errs := validateCondition(&Condition{Kind: kind, Key: "content.a", Value: json.RawMessage(value)}); len(errs) == 0 {
				t.Fatalf("accepted %q", value)
			}
		}
	}
}

func TestEventMatchEscapedProperty(t *testing.T) {
	event := mustEventFromJSON(t, `{"room_id":"!room:a","content":{"a.b":"text"}}`)
	got, err := patternMatches(`content.a\.b`, "text", event)
	if err != nil || !got {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestSuppressEdits(t *testing.T) {
	for _, tc := range []struct {
		name, event, want string
	}{
		{"edit", `{"type":"m.room.message","content":{"body":"* hi","m.new_content":{"body":"hi"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`, MRuleSuppressEdits},
		{"encrypted edit", `{"type":"m.room.encrypted","content":{"ciphertext":"x","m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`, MRuleSuppressEdits},
		// An edit that newly mentions the user still notifies them.
		{"edit with mention", `{"type":"m.room.message","content":{"body":"* hi","m.mentions":{"user_ids":["@alice:example.com"]},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`, MRuleIsUserMention},
		{"thread reply", `{"type":"m.room.message","content":{"body":"hi","m.relates_to":{"rel_type":"m.thread","event_id":"$root"}}}`, MRuleMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var event map[string]any
			if err := json.Unmarshal([]byte(tc.event), &event); err != nil {
				t.Fatal(err)
			}
			event["room_id"] = "!room:example.com"
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			rule, err := NewRuleSetEvaluator(mentionContext{fakeEvaluationContext{3}, false}, DefaultGlobalRuleSet("alice", "example.com")).MatchEvent(mustEventFromJSON(t, string(raw)), UserIDForSender)
			if err != nil {
				t.Fatal(err)
			}
			if rule == nil || rule.RuleID != tc.want {
				t.Fatalf("got %+v, want %s", rule, tc.want)
			}
			if tc.want == MRuleSuppressEdits && len(rule.Actions) != 0 {
				t.Fatalf("suppress_edits must not notify, got %+v", rule.Actions)
			}
		})
	}
}

func TestUpgradeDefaultRulesAddsSuppressEdits(t *testing.T) {
	rules := DefaultGlobalRuleSet("alice", "example.com")
	custom := &Rule{RuleID: "custom", Enabled: true, Actions: []*Action{{Kind: NotifyAction}}}
	stored := []*Rule{}
	for _, rule := range rules.Override {
		if rule.RuleID != MRuleSuppressEdits {
			stored = append(stored, rule)
		}
	}
	stored = append(stored, custom)
	rules.Override = stored
	UpgradeDefaultRules(rules, "alice", "example.com")
	last := rules.Override[len(rules.Override)-1]
	if last.RuleID != MRuleSuppressEdits || rules.Override[len(rules.Override)-2] != custom {
		t.Fatalf("suppress_edits must follow the stored rules, got %s", last.RuleID)
	}
	// A user who disabled the rule keeps it disabled.
	last.Enabled = false
	before, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	UpgradeDefaultRules(rules, "alice", "example.com")
	after, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(string(before), string(after)); diff != "" {
		t.Fatalf("not idempotent: %s", diff)
	}
}
