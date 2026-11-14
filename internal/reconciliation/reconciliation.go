// Package reconciliation exposes the MSC4521 algebraic digest and bucket
// sketch primitives used by the federation reconciliation endpoints.
//
// The default build does not link the native rezzy library, so the endpoints
// report themselves unsupported. Building with the "rezzy" tag on Linux with
// cgo enables them; see the build-rezzy target in the Makefile.
package reconciliation

import "github.com/gamesguru/gomatrixserverlib"

// DigestType is the MSC4521 profile this server speaks.
const DigestType = "algebraic_v1"

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
	case gomatrixserverlib.RoomVersionV1, gomatrixserverlib.RoomVersionV2:
		return "", false
	case gomatrixserverlib.RoomVersionV3:
		return "v3", true
	default:
		return "v4plus", true
	}
}
