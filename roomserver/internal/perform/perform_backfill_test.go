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

func (f stateLookupFedAPI) LookupStateIDs(_ context.Context, _, s spec.ServerName, _, _ string) (gomatrixserverlib.StateIDResponse, error) {
	return nil, f.errs[s]
}

func (f stateLookupFedAPI) LookupState(_ context.Context, _, s spec.ServerName, _, _ string, _ gomatrixserverlib.RoomVersion) (gomatrixserverlib.StateResponse, error) {
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

	fed := stateLookupFedAPI{errs: map[spec.ServerName]error{
		"flaky": spec.HTTPError{Code: http.StatusInternalServerError},
		"gone":  spec.HTTPError{Code: http.StatusNotFound},
	}}
	servers := []spec.ServerName{"flaky", "gone"}
	newRequester := func() *backfillRequester {
		r := newBackfillRequester(noLocalEventsDB{}, fed, nil, "origin", nil, nil, nil, gomatrixserverlib.RoomVersionV10)
		r.servers = servers
		return r
	}

	for name, lookup := range map[string]func(*backfillRequester) error{
		"StateIDsBeforeEvent": func(r *backfillRequester) error {
			_, err := r.StateIDsBeforeEvent(context.Background(), ev)
			return err
		},
		"StateBeforeEvent": func(r *backfillRequester) error {
			_, err := r.StateBeforeEvent(context.Background(), gomatrixserverlib.RoomVersionV10, ev, []string{"$s"})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRequester()
			err := lookup(r)
			// The last server's 404 is the returned error, but the earlier 500 must
			// still count, so the failure stays retryable.
			var httpErr spec.HTTPError
			require.ErrorAs(t, err, &httpErr)
			require.Equal(t, http.StatusNotFound, httpErr.Code)
			require.Len(t, r.backfillErrors, 2)
			require.True(t, backfillFailuresRetryable(r.backfillErrors))
			require.False(t, backfillFailuresRetryable([]error{err}), "the 404 alone would be terminal")
		})
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

type fakeStateIDResponse struct{}

func (fakeStateIDResponse) GetStateEventIDs() []string { return nil }
func (fakeStateIDResponse) GetAuthEventIDs() []string  { return nil }

type fakeStateResponse struct{}

func (fakeStateResponse) GetAuthEvents() gomatrixserverlib.EventJSONs  { return nil }
func (fakeStateResponse) GetStateEvents() gomatrixserverlib.EventJSONs { return nil }

// fakeStateLookupAPI returns a per-server error for both state lookups.
type fakeStateLookupAPI struct {
	federationAPI.RoomserverFederationAPI
	errs map[spec.ServerName]error
}

func (f *fakeStateLookupAPI) LookupStateIDs(
	_ context.Context, _, s spec.ServerName, _, _ string,
) (gomatrixserverlib.StateIDResponse, error) {
	if err := f.errs[s]; err != nil {
		return nil, err
	}
	return fakeStateIDResponse{}, nil
}

func (f *fakeStateLookupAPI) LookupState(
	_ context.Context, _, s spec.ServerName, _, _ string, _ gomatrixserverlib.RoomVersion,
) (gomatrixserverlib.StateResponse, error) {
	if err := f.errs[s]; err != nil {
		return nil, err
	}
	return fakeStateResponse{}, nil
}

// failingEventNIDsDB makes ProvideEventsContext fall through to federation.
type failingEventNIDsDB struct {
	storage.Database
}

func (failingEventNIDsDB) EventNIDs(_ context.Context, _ []string) (map[string]types.EventMetadata, error) {
	return nil, errors.New("database unavailable")
}

func TestStateLookupFailuresAreRecorded(t *testing.T) {
	roomVer := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	event, err := roomVer.NewEventFromTrustedJSON([]byte(`{
		"event_id": "$target",
		"room_id": "!room:test",
		"sender": "@alice:test",
		"type": "m.room.message",
		"content": {"body": "x"},
		"depth": 5,
		"origin_server_ts": 1,
		"prev_events": ["$prev"],
		"auth_events": []
	}`), false)
	if err != nil {
		t.Fatalf("failed to build event: %v", err)
	}

	notFound := spec.HTTPError{Code: http.StatusNotFound}
	serverError := spec.HTTPError{Code: http.StatusInternalServerError}

	lookups := map[string]func(*backfillRequester, context.Context) error{
		"StateIDsBeforeEvent": func(b *backfillRequester, ctx context.Context) error {
			_, err := b.StateIDsBeforeEvent(ctx, event)
			return err
		},
		"StateBeforeEvent": func(b *backfillRequester, ctx context.Context) error {
			_, err := b.StateBeforeEvent(ctx, gomatrixserverlib.RoomVersionV10, event, []string{"$a"})
			return err
		},
	}

	cases := []struct {
		name  string
		errs  map[spec.ServerName]error
		want  []error
		retry bool
	}{
		{"transient then not found", map[spec.ServerName]error{"hs1": serverError, "hs2": notFound}, []error{serverError, notFound}, true},
		{"not found on every server", map[spec.ServerName]error{"hs1": notFound, "hs2": notFound}, []error{notFound, notFound}, false},
	}

	for lookupName, lookup := range lookups {
		for _, tc := range cases {
			t.Run(lookupName+"/"+tc.name, func(t *testing.T) {
				b := &backfillRequester{
					db:                      failingEventNIDsDB{},
					fsAPI:                   &fakeStateLookupAPI{errs: tc.errs},
					virtualHost:             "origin.test",
					servers:                 []spec.ServerName{"hs1", "hs2"},
					eventIDToBeforeStateIDs: make(map[string][]string),
					eventIDMap:              make(map[string]gomatrixserverlib.PDU),
				}
				lastErr := lookup(b, context.Background())
				if lastErr == nil {
					t.Fatal("expected a lookup error")
				}
				if len(b.backfillErrors) != len(tc.want) {
					t.Fatalf("recorded %d errors, want %d: %v", len(b.backfillErrors), len(tc.want), b.backfillErrors)
				}
				for i, want := range tc.want {
					var wantHTTP, gotHTTP spec.HTTPError
					if !errors.As(want, &wantHTTP) || !errors.As(b.backfillErrors[i], &gotHTTP) {
						t.Fatalf("error %d = %v, want %v", i, b.backfillErrors[i], want)
					}
					if gotHTTP.Code != wantHTTP.Code {
						t.Fatalf("error %d code = %d, want %d", i, gotHTTP.Code, wantHTTP.Code)
					}
				}
				if got := backfillFailuresRetryable(b.backfillErrors); got != tc.retry {
					t.Fatalf("backfillFailuresRetryable = %v, want %v", got, tc.retry)
				}
			})
		}
	}
}
