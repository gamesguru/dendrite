package routing

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

func TestSyncPrunedOverallStatus(t *testing.T) {
	tests := []struct {
		name, output, state, want string
		dryRun                    bool
	}{
		{name: "both applied", output: "updated_output", state: "updated_state", want: "both"},
		{name: "output only", output: "updated_output", state: "not_present", want: "updated_output"},
		{name: "state only", output: "not_present", state: "updated_state", want: "updated_state"},
		{name: "neither present", output: "not_present", state: "not_present", want: "not_present"},
		{name: "dry run both", output: "would_update_output", state: "would_update_state", want: "would_update_both", dryRun: true},
		{name: "redacted", output: "updated_output", state: "redacted", want: "updated_output"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := syncPrunedOverallStatus(tt.output, tt.state, tt.dryRun); got != tt.want {
				t.Fatalf("syncPrunedOverallStatus(%q, %q, %t) = %q, want %q", tt.output, tt.state, tt.dryRun, got, tt.want)
			}
		})
	}
}

type prunedEventRoomserver struct {
	api.SyncRoomserverAPI
	event *rstypes.HeaderedEvent
}

func (r prunedEventRoomserver) QueryEventsByID(_ context.Context, _ *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
	res.Events = []*rstypes.HeaderedEvent{r.event}
	return nil
}

func TestAdminSyncPrunedReportsRoomserverPruned(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	event := room.CreateEvent(t, alice, "m.room.message", map[string]string{"body": "full"})
	prunedJSON, err := sjson.SetRawBytes(event.JSON(), "content", []byte(`{}`))
	require.NoError(t, err)
	roomVersion, err := gomatrixserverlib.GetRoomVersion(event.Version())
	require.NoError(t, err)
	pdu, err := roomVersion.NewEventFromUntrustedJSON(prunedJSON)
	require.NoError(t, err)
	require.True(t, pdu.Redacted())

	request := httptest.NewRequest("POST", "/_zendrite/admin/syncPruned", strings.NewReader(`{"dry_run":true,"event_ids":["`+event.EventID()+`"]}`))
	response := AdminSyncPruned(request, nil, prunedEventRoomserver{event: &rstypes.HeaderedEvent{PDU: pdu}}, nil)
	require.Equal(t, 200, response.Code)
	encoded, err := json.Marshal(response.JSON)
	require.NoError(t, err)
	var body struct {
		Results map[string]syncPrunedResult `json:"results"`
	}
	require.NoError(t, json.Unmarshal(encoded, &body))
	require.Equal(t, "not_refetched", body.Results[event.EventID()].Status)
}
