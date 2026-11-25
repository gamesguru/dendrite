package routing

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
)

type fakeRefetchPrunedRS struct {
	roomserverAPI.ClientRoomserverAPI
	calls   int
	dryRun  bool
	events  []roomserverAPI.AdminRefetchEvent
	results map[string]string
}

func (f *fakeRefetchPrunedRS) PerformAdminRefetchEvents(_ context.Context, events []roomserverAPI.AdminRefetchEvent, dryRun bool) map[string]string {
	f.calls++
	f.dryRun = dryRun
	f.events = events
	return f.results
}

func TestAdminRefetchPrunedValidatesRequest(t *testing.T) {
	tooManyServers := `{"dry_run":true,"servers":["a","b","c","d","e","f","g","h","i"],"events":[{"room_id":"!r:example.org","event_id":"$e"}]}`
	for _, body := range []string{
		`{`,
		`{"events":[]}`,
		`{"events":[{"event_id":"$e"}]}`,
		tooManyServers,
	} {
		t.Run(body, func(t *testing.T) {
			fake := &fakeRefetchPrunedRS{}
			req := httptest.NewRequest("POST", "/_zendrite/admin/refetchPruned", strings.NewReader(body))
			res := AdminRefetchPruned(req, nil, fake)
			require.Equal(t, 400, res.Code)
			require.Zero(t, fake.calls)
		})
	}
}

func TestAdminRefetchPrunedDryRunReportsResults(t *testing.T) {
	fake := &fakeRefetchPrunedRS{results: map[string]string{"$event": "would_refetch"}}
	req := httptest.NewRequest("POST", "/_zendrite/admin/refetchPruned", strings.NewReader(
		`{"dry_run":true,"servers":["matrix.org"],"events":[{"room_id":"!r:example.org","event_id":"$event"}]}`,
	))
	res := AdminRefetchPruned(req, nil, fake)
	require.Equal(t, 200, res.Code)
	require.Equal(t, 1, fake.calls)
	require.True(t, fake.dryRun)
	require.Len(t, fake.events, 1)
	require.Equal(t, []spec.ServerName{"matrix.org"}, fake.events[0].Servers)
	var body struct {
		DryRun  bool              `json:"dry_run"`
		Totals  map[string]int    `json:"totals"`
		Results map[string]string `json:"results"`
	}
	encoded, err := json.Marshal(res.JSON)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(encoded, &body))
	require.True(t, body.DryRun)
	require.Equal(t, map[string]int{"would_refetch": 1}, body.Totals)
	require.Equal(t, map[string]string{"$event": "would_refetch"}, body.Results)
}
