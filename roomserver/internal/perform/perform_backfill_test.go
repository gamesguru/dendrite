package perform

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	federationAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// stateLookupFedAPI fails every state lookup with a per-server error.
type stateLookupFedAPI struct {
	federationAPI.RoomserverFederationAPI
	errs map[spec.ServerName]error
}

func (f stateLookupFedAPI) LookupStateIDs(
	_ context.Context, _, s spec.ServerName, _, _ string,
) (gomatrixserverlib.StateIDResponse, error) {
	return nil, f.errs[s]
}

func (f stateLookupFedAPI) LookupState(
	_ context.Context, _, s spec.ServerName, _, _ string, _ gomatrixserverlib.RoomVersion,
) (gomatrixserverlib.StateResponse, error) {
	return nil, f.errs[s]
}

// noLocalEventsDB makes the local event lookup fail so StateBeforeEvent falls
// through to federation.
type noLocalEventsDB struct{ storage.Database }

func (noLocalEventsDB) EventNIDs(context.Context, []string) (map[string]types.EventMetadata, error) {
	return nil, errors.New("no local events")
}

func TestBackfillStateLookupFailuresAreRecorded(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	// Two prev events force a /state_ids request instead of rolling state forward.
	ev, err := verImpl.NewEventFromTrustedJSON([]byte(
		`{"type":"m.room.message","room_id":"!r:test","sender":"@a:test","content":{},"origin_server_ts":1,"depth":2,`+
			`"prev_events":["$p1","$p2"],"auth_events":[]}`), false)
	require.NoError(t, err)

	servers := []spec.ServerName{"flaky", "gone"}
	cases := []struct {
		name      string
		errs      map[spec.ServerName]error
		wantCodes []int
		retryable bool
	}{
		{
			name: "transient then not found",
			errs: map[spec.ServerName]error{
				"flaky": spec.HTTPError{Code: http.StatusInternalServerError},
				"gone":  spec.HTTPError{Code: http.StatusNotFound},
			},
			wantCodes: []int{http.StatusInternalServerError, http.StatusNotFound},
			retryable: true,
		},
		{
			// If recording ever turned a definitive answer into a retryable
			// one, pagination would never see the end of history.
			name: "not found on every server",
			errs: map[spec.ServerName]error{
				"flaky": spec.HTTPError{Code: http.StatusNotFound},
				"gone":  spec.HTTPError{Code: http.StatusNotFound},
			},
			wantCodes: []int{http.StatusNotFound, http.StatusNotFound},
			retryable: false,
		},
	}

	lookups := map[string]func(*backfillRequester) error{
		"StateIDsBeforeEvent": func(r *backfillRequester) error {
			_, err := r.StateIDsBeforeEvent(context.Background(), ev)
			return err
		},
		"StateBeforeEvent": func(r *backfillRequester) error {
			_, err := r.StateBeforeEvent(context.Background(), gomatrixserverlib.RoomVersionV10, ev, []string{"$s"})
			return err
		},
	}

	for lookupName, lookup := range lookups {
		for _, tc := range cases {
			t.Run(lookupName+"/"+tc.name, func(t *testing.T) {
				r := newBackfillRequester(
					noLocalEventsDB{}, stateLookupFedAPI{errs: tc.errs},
					nil, "origin", nil, nil, nil, gomatrixserverlib.RoomVersionV10,
				)
				r.servers = servers

				err := lookup(r)
				// The last server's error is returned; earlier ones must still
				// count towards classification.
				var httpErr spec.HTTPError
				require.ErrorAs(t, err, &httpErr)
				require.Equal(t, tc.wantCodes[len(tc.wantCodes)-1], httpErr.Code)

				require.Len(t, r.backfillErrors, len(tc.wantCodes))
				for i, wantCode := range tc.wantCodes {
					var recorded spec.HTTPError
					require.ErrorAs(t, r.backfillErrors[i], &recorded)
					require.Equal(t, wantCode, recorded.Code, "recorded error %d", i)
				}
				require.Equal(t, tc.retryable, backfillFailuresRetryable(r.backfillErrors))
			})
		}
	}
}

func TestBackfillFailuresRetryable(t *testing.T) {
	notFound := spec.HTTPError{Code: http.StatusNotFound}
	cases := []struct {
		name string
		errs []error
		want bool
	}{
		{"no server responses", nil, true},
		{"not found", []error{notFound}, false},
		{"not found from every server", []error{notFound, notFound}, false},
		{"wrapped not found", []error{fmt.Errorf("backfill: %w", notFound)}, false},
		{"forbidden", []error{spec.HTTPError{Code: http.StatusForbidden}}, true},
		{"bad request", []error{spec.HTTPError{Code: http.StatusBadRequest}}, true},
		{"server error", []error{spec.HTTPError{Code: http.StatusInternalServerError}}, true},
		{"rate limited", []error{spec.HTTPError{Code: http.StatusTooManyRequests}}, true},
		{"mixed definitive and transient", []error{notFound, spec.HTTPError{Code: http.StatusBadGateway}}, true},
		{"network failure", []error{context.DeadlineExceeded}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backfillFailuresRetryable(tc.errs); got != tc.want {
				t.Fatalf("backfillFailuresRetryable(%v) = %v, want %v", tc.errs, got, tc.want)
			}
		})
	}
}

func TestPrioritiseBackfillFromEventIDsPrefersMostServers(t *testing.T) {
	fromEventIDs := []string{"$one", "$two", "$three"}
	serverCounts := map[string][]spec.ServerName{
		"$one":   {"hs1"},
		"$two":   {"hs1", "hs2"},
		"$three": {},
	}

	got := prioritiseBackfillFromEventIDs(
		context.Background(),
		"!room:test",
		fromEventIDs,
		func(_ context.Context, _, eventID string) []spec.ServerName {
			return serverCounts[eventID]
		},
	)

	if len(got) != len(fromEventIDs) {
		t.Fatalf("expected %d from-event IDs, got %d", len(fromEventIDs), len(got))
	}
	if got[0] != "$two" || got[1] != "$one" || got[2] != "$three" {
		t.Fatalf("expected best from-event first and others in order, got %v", got)
	}
}

func TestPrioritiseBackfillFromEventIDsKeepsOrderWhenNoBetterCandidate(t *testing.T) {
	fromEventIDs := []string{"$one", "$two"}

	got := prioritiseBackfillFromEventIDs(
		context.Background(),
		"!room:test",
		fromEventIDs,
		func(_ context.Context, _, eventID string) []spec.ServerName {
			if eventID == "$one" {
				return []spec.ServerName{"hs1"}
			}
			return []spec.ServerName{"hs2"}
		},
	)

	if got[0] != "$one" || got[1] != "$two" {
		t.Fatalf("expected original ordering to be preserved, got %v", got)
	}
}
