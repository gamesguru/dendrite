package internal

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/test"
)

func TestFederationRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want time.Duration
	}{
		{"delay", `{"retry_after_ms":2500}`, 2500 * time.Millisecond},
		{"missing", `{"errcode":"M_UNKNOWN","error":"Server is unavailable"}`, time.Minute},
		{"empty", "", time.Minute},
		{"invalid JSON", "<html>Busy</html>", time.Minute},
		{"zero", `{"retry_after_ms":0}`, time.Minute},
		{"negative", `{"retry_after_ms":-10}`, time.Minute},
		{"fractional", `{"retry_after_ms":1.5}`, time.Minute},
		{"string", `{"retry_after_ms":"2500"}`, time.Minute},
		{"null", `{"retry_after_ms":null}`, time.Minute},
		{"duration overflow", `{"retry_after_ms":9223372036854775807}`, time.Minute},
		{"integer overflow", `{"retry_after_ms":9223372036854775808}`, time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{
				spec.HTTPError{Code: 429, Contents: []byte(tc.body)},
				gomatrix.HTTPError{Code: 429, Contents: []byte(tc.body)},
			} {
				require.Equal(t, tc.want, federationRetryAfter(fmt.Errorf("wrapped: %w", err)))
			}
		})
	}
}

type rateLimitedHierarchyClient struct {
	fclient.FederationClient
	calls int
}

func (c *rateLimitedHierarchyClient) RoomHierarchy(_ context.Context, _, _ spec.ServerName, _ string, _ bool) (fclient.RoomHierarchyResponse, error) {
	c.calls++
	return fclient.RoomHierarchyResponse{}, spec.HTTPError{Code: 429, Contents: []byte(`{"errcode":"M_UNKNOWN","error":"Server is unavailable"}`)}
}

func TestHierarchyRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stats := statistics.NewStatistics(test.NewInMemoryFederationDatabase(), 1, 1, false)
		client := &rateLimitedHierarchyClient{}
		a := &FederationInternalAPI{statistics: &stats, federation: client}
		checkLimited := func(err error) {
			t.Helper()
			var fedErr *api.FederationClientError
			require.ErrorAs(t, err, &fedErr)
			require.Equal(t, http.StatusTooManyRequests, fedErr.Code)
			require.Positive(t, fedErr.RetryAfter)
			require.False(t, fedErr.Blacklisted)
		}
		_, err := a.RoomHierarchies(t.Context(), "local", "matrix.org", "!first:matrix.org", false)
		checkLimited(err)
		_, err = a.RoomHierarchies(t.Context(), "local", "matrix.org", "!second:matrix.org", false)
		checkLimited(err)
		require.Equal(t, 1, client.calls, "a different room must respect the destination cooldown")
		_, err = a.IsBlacklistedOrBackingOff("matrix.org")
		checkLimited(err)
		_, err = a.RoomHierarchies(t.Context(), "local", "other.test", "!third:other.test", false)
		checkLimited(err)
		require.Equal(t, 2, client.calls, "another destination remains reachable")
		time.Sleep(time.Minute)
		_, err = a.RoomHierarchies(t.Context(), "local", "matrix.org", "!second:matrix.org", false)
		checkLimited(err)
		require.Equal(t, 3, client.calls, "retry once the cooldown expires")
		s := stats.ForServer("matrix.org")
		require.Nil(t, s.BackoffInfo())
		require.False(t, s.Blacklisted(), "repeated 429s must not blacklist the server")
	})
}

func TestFederationRateLimitAcrossRequestPaths(t *testing.T) {
	for _, source := range []string{"regular", "foreground", "bulk"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stats := statistics.NewStatistics(test.NewInMemoryFederationDatabase(), 1, 1, false)
				a := &FederationInternalAPI{statistics: &stats}
				paths := map[string]func(spec.ServerName, func() (any, error)) (any, error){
					"regular": func(s spec.ServerName, fn func() (any, error)) (any, error) {
						return a.doFederationRequest(s, fn, true)
					},
					"foreground": func(s spec.ServerName, fn func() (any, error)) (any, error) {
						return a.doFederationRequest(s, fn, false)
					},
					"bulk": a.doFederationRequestNoStats,
				}
				_, err := paths[source]("remote.test", func() (any, error) {
					return nil, spec.HTTPError{Code: 429, Contents: []byte(`{"retry_after_ms":5000}`)}
				})
				require.Error(t, err)
				calls := 0
				success := func() (any, error) { calls++; return "ok", nil }
				for _, path := range paths {
					_, err = path("remote.test", success)
					var fedErr *api.FederationClientError
					require.ErrorAs(t, err, &fedErr)
					require.Equal(t, http.StatusTooManyRequests, fedErr.Code)
					require.Equal(t, 5*time.Second, fedErr.RetryAfter)
				}
				require.Zero(t, calls)
				time.Sleep(5 * time.Second)
				for _, path := range paths {
					_, err = path("remote.test", success)
					require.NoError(t, err)
				}
				require.Equal(t, 3, calls)
			})
		})
	}
}

func TestFederationRateLimitConcurrentResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := &FederationInternalAPI{}
		var wg sync.WaitGroup
		for i := 1; i <= 50; i++ {
			wg.Go(func() {
				assert.NotNil(t, a.recordRateLimit("remote.test", spec.HTTPError{Code: 429, Contents: fmt.Appendf(nil, `{"retry_after_ms":%d}`, i*1000)}))
				assert.NotNil(t, a.rateLimitError("remote.test"))
			})
		}
		wg.Wait()
		require.Equal(t, 50*time.Second, a.rateLimitError("remote.test").RetryAfter)
	})
}

func TestFederationRateLimitSurvivesInflightSuccess(t *testing.T) {
	stats := statistics.NewStatistics(test.NewInMemoryFederationDatabase(), 8, 3, false)
	a := &FederationInternalAPI{statistics: &stats}
	_, err := a.doFederationRequest("remote.test", func() (any, error) {
		// Another request receives a 429 before this successful response arrives.
		require.NotNil(t, a.recordRateLimit("remote.test", spec.HTTPError{Code: 429}))
		return nil, nil
	}, false)
	require.NoError(t, err)
	require.NotNil(t, a.rateLimitError("remote.test"))
}

func TestForegroundQueriesStillBypassTransportBackoff(t *testing.T) {
	stats := statistics.NewStatistics(test.NewInMemoryFederationDatabase(), 8, 3, false)
	s := stats.ForServer("remote.test")
	s.Failure()
	t.Cleanup(s.ClearBackoff)
	a := &FederationInternalAPI{statistics: &stats}
	calls := 0
	_, err := a.doFederationRequest("remote.test", func() (any, error) {
		calls++
		return nil, nil
	}, false)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}
