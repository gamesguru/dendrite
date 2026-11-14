//go:build rezzy && linux && cgo

package reconciliation

import (
	"github.com/gamesguru/rezzy/rezzy-ffi/go/rezzy"
)

// Enabled reports whether this build links rezzy.
const Enabled = true

// Snapshot returns the MSC4521 digest, count and strata for an event set.
func Snapshot(eventIDs []string, format string) (*RoomSnapshot, error) {
	snapshot, err := rezzy.ReconSnapshot(eventIDs, format)
	if err != nil {
		return nil, err
	}
	return &RoomSnapshot{
		Digest: snapshot.Digest,
		Count:  snapshot.Count,
		Strata: snapshot.Strata,
	}, nil
}

// BucketSketches builds one base64url sketch per requested bucket.
func BucketSketches(eventIDs []string, format string, requests []BucketRequest) ([]string, error) {
	return rezzy.ReconBucketSketches(eventIDs, format, toRezzyRequests(requests))
}

// BucketDiff decodes the symmetric difference between the local event set and
// the requester's bucket sketches.
func BucketDiff(
	eventIDs []string, format string, requests []BucketRequest,
	localSketches []string, localDigest string, localCount uint64,
) (*BucketDiffResult, error) {
	result, err := rezzy.ReconBucketDiff(
		eventIDs, format, toRezzyRequests(requests),
		localSketches, localDigest, localCount,
	)
	if err != nil {
		return nil, err
	}
	return &BucketDiffResult{
		Status:                           result.Status,
		MissingEventIDs:                  result.MissingEventIDs,
		RequesterOnlyShortIDs:            result.RequesterOnlyShortIDs,
		ExpectedRequesterSideAccumulator: result.ExpectedRequesterSideAccumulator,
		KnownEventCount:                  result.KnownEventCount,
	}, nil
}

func toRezzyRequests(requests []BucketRequest) []rezzy.BucketRequest {
	out := make([]rezzy.BucketRequest, len(requests))
	for i, request := range requests {
		out[i] = rezzy.BucketRequest{
			Depth:    request.Depth,
			Prefix:   request.Prefix,
			Capacity: request.Capacity,
		}
	}
	return out
}
