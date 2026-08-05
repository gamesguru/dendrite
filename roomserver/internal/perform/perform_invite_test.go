package perform

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestPseudoIDInviteSignatureKeyIDs(t *testing.T) {
	federationKey := ed25519.NewKeyFromSeed([]byte{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
	})
	roomKey := ed25519.NewKeyFromSeed([]byte{
		32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
		16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
	})
	inviteeRoomKey := ed25519.NewKeyFromSeed([]byte{
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	})

	federationKeyID := gomatrixserverlib.KeyID("ed25519:federation")
	roomKeyID := gomatrixserverlib.KeyID("ed25519:1")
	inviter, err := spec.NewUserID("@alice:example.com", true)
	if err != nil {
		t.Fatal(err)
	}
	roomID, err := spec.NewRoomID("!room:example.com")
	if err != nil {
		t.Fatal(err)
	}

	inviterSenderID := spec.SenderIDFromPseudoIDKey(roomKey)
	mapping := &gomatrixserverlib.MXIDMapping{
		UserRoomKey: inviterSenderID,
		UserID:      inviter.String(),
	}
	if err = mapping.Sign(inviter.Domain(), federationKeyID, federationKey); err != nil {
		t.Fatal(err)
	}
	if _, ok := mapping.Signatures[inviter.Domain()][federationKeyID]; !ok {
		t.Fatalf("expected mxid_mapping signature from %s with %s", inviter.Domain(), federationKeyID)
	}

	stateKey := string(spec.SenderIDFromPseudoIDKey(inviteeRoomKey))
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(inviterSenderID),
		RoomID:   roomID.String(),
		Type:     spec.MRoomMember,
		StateKey: &stateKey,
	}
	if err = proto.SetContent(gomatrixserverlib.MemberContent{
		Membership:  spec.Invite,
		MXIDMapping: mapping,
	}); err != nil {
		t.Fatal(err)
	}

	builder := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionPseudoIDs).NewEventBuilderFromProtoEvent(&proto)
	inviteEvent, err := builder.Build(time.Unix(0, 0), spec.ServerName(inviterSenderID), roomKeyID, roomKey)
	if err != nil {
		t.Fatal(err)
	}

	var eventJSON struct {
		Signatures map[string]map[gomatrixserverlib.KeyID]json.RawMessage `json:"signatures"`
	}
	if err = json.Unmarshal(inviteEvent.JSON(), &eventJSON); err != nil {
		t.Fatal(err)
	}
	if _, ok := eventJSON.Signatures[string(inviterSenderID)][roomKeyID]; !ok {
		t.Fatalf("expected invite event signature from %s with %s", inviterSenderID, roomKeyID)
	}
	if _, ok := eventJSON.Signatures[string(inviterSenderID)][federationKeyID]; ok {
		t.Fatalf("did not expect invite event signature from %s with federation key ID %s", inviterSenderID, federationKeyID)
	}
}
