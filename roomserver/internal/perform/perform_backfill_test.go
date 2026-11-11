package perform

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

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
