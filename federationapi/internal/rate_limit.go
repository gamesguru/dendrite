package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"

	"codefloe.com/pat-s/zendrite/federationapi/api"
)

const defaultFederationRateLimit = time.Minute

// federationRetryAfter reads the delay retained in the HTTP error body.
// The federation client does not retain response headers, so Retry-After is
// unavailable here. Invalid or absent delays use a conservative fallback.
func federationRetryAfter(err error) time.Duration {
	var contents []byte
	var specErr spec.HTTPError
	var mxErr gomatrix.HTTPError
	switch {
	case errors.As(err, &specErr):
		contents = specErr.Contents
	case errors.As(err, &mxErr):
		contents = mxErr.Contents
	}
	var body struct {
		RetryAfterMS int64 `json:"retry_after_ms"`
	}
	if json.Unmarshal(contents, &body) != nil || body.RetryAfterMS <= 0 || body.RetryAfterMS > math.MaxInt64/int64(time.Millisecond) {
		return defaultFederationRateLimit
	}
	return time.Duration(body.RetryAfterMS) * time.Millisecond
}

// rateLimitError checks a cooldown shared by all federation API request paths
// for this destination. Expiry needs no timer, goroutine, or database write.
func (a *FederationInternalAPI) rateLimitError(s spec.ServerName) *api.FederationClientError {
	a.rateLimitMutex.Lock()
	defer a.rateLimitMutex.Unlock()
	until, ok := a.rateLimits[s]
	if !ok {
		return nil
	}
	remaining := time.Until(until)
	if remaining <= 0 {
		delete(a.rateLimits, s)
		return nil
	}
	return &api.FederationClientError{
		Err:        fmt.Sprintf("server %q is rate limited", s),
		Code:       http.StatusTooManyRequests,
		RetryAfter: remaining,
	}
}

// recordRateLimit handles 429 independently of failure counts and blacklisting.
// An in-flight response must never shorten an existing cooldown.
func (a *FederationInternalAPI) recordRateLimit(s spec.ServerName, err error) *api.FederationClientError {
	if federationHTTPCode(err) != http.StatusTooManyRequests {
		return nil
	}
	until := time.Now().Add(federationRetryAfter(err))
	a.rateLimitMutex.Lock()
	if a.rateLimits == nil {
		a.rateLimits = make(map[spec.ServerName]time.Time)
	}
	if existing := a.rateLimits[s]; existing.After(until) {
		until = existing
	}
	a.rateLimits[s] = until
	a.rateLimitMutex.Unlock()
	return &api.FederationClientError{
		Err:        err.Error(),
		Code:       http.StatusTooManyRequests,
		RetryAfter: time.Until(until),
	}
}
