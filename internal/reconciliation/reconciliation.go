// Package reconciliation exposes the MSC4521 algebraic digest and bucket
// sketch primitives used by the federation reconciliation endpoints.
//
// The default build does not link the native rezzy library, so the endpoints
// report themselves unsupported. Building with the "rezzy" tag on Linux with
// cgo enables them; see the build-rezzy target in the Makefile.
package reconciliation

import (
	"errors"

	"github.com/gamesguru/gomatrixserverlib"
)

// DigestType is the MSC4521 profile this server speaks.
const DigestType = "algebraic_v1"

// ErrInvalidRequest marks a room_diff request whose sketches, digest or bucket
// parameters could not be decoded. Callers should treat it as a client error
// (HTTP 400); every other error from this package is a backend failure.
var ErrInvalidRequest = errors.New("invalid reconciliation request")

// InvalidRequestError wraps a request-validation failure. Use errors.Is with
// ErrInvalidRequest to detect it.
type InvalidRequestError struct {
	Err error
}

func (e *InvalidRequestError) Error() string {
	return e.Err.Error()
}

func (e *InvalidRequestError) Unwrap() error {
	return e.Err
}

func (e *InvalidRequestError) Is(target error) bool {
	return target == ErrInvalidRequest
}

// BucketRequest is one triage bucket in a room_diff request.
type BucketRequest struct {
	Depth    uint8
	Prefix   uint64
	Capacity int
}

// RoomSnapshot is a responder-side algebraic_v1 view of an event set.
type RoomSnapshot struct {
	Digest string
	Count  uint64
	Strata []string
}

// BucketDiffResult is the decoded symmetric difference for one room_diff.
type BucketDiffResult struct {
	Status                           string
	MissingEventIDs                  []string
	RequesterOnlyShortIDs            []string
	ExpectedRequesterSideAccumulator string
	KnownEventCount                  uint64
}

// EventIDFormat maps a room version onto the MSC4521 reference-hash alphabet.
// Room versions 1 and 2 use sender-chosen IDs and are not reconcilable.
func EventIDFormat(roomVersion gomatrixserverlib.RoomVersion) (string, bool) {
	switch roomVersion {
	case gomatrixserverlib.RoomVersionV3:
		return "v3", true
	case gomatrixserverlib.RoomVersionV4,
		gomatrixserverlib.RoomVersionV5,
		gomatrixserverlib.RoomVersionV6,
		gomatrixserverlib.RoomVersionV7,
		gomatrixserverlib.RoomVersionV8,
		gomatrixserverlib.RoomVersionV9,
		gomatrixserverlib.RoomVersionV10,
		gomatrixserverlib.RoomVersionV11,
		gomatrixserverlib.RoomVersionV12,
		// Unstable versions in gomatrixserverlib that use reference-hash IDs.
		gomatrixserverlib.RoomVersionPseudoIDs,
		gomatrixserverlib.RoomVersionHydra,
		gomatrixserverlib.RoomVersionStateDAGs:
		return "v4plus", true
	default:
		return "", false
	}
}
