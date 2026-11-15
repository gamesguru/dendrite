package reconciliation

import (
	"encoding/base64"
	"fmt"

	"github.com/gamesguru/rezzy/rezzy-ffi/go/rezzy"
)

const digestByteLength = 16

const maxRequestSketchCapacity = 32

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
	if err := validateBucketDiffRequest(requests, localSketches, localDigest); err != nil {
		return nil, &InvalidRequestError{Err: err}
	}
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

// validateBucketDiffRequest rejects requester-supplied bucket parameters,
// sketches and digests before they reach the native decoder, so that only
// genuine backend failures propagate as untyped errors.
func validateBucketDiffRequest(requests []BucketRequest, localSketches []string, localDigest string) error {
	if len(localSketches) != len(requests) {
		return fmt.Errorf("`local_sketches` length must match `requests` length")
	}
	if len(localDigest) != base64.RawURLEncoding.EncodedLen(digestByteLength) {
		return fmt.Errorf("invalid `local_digest`")
	}
	if raw, err := base64.RawURLEncoding.DecodeString(localDigest); err != nil || len(raw) != digestByteLength {
		return fmt.Errorf("invalid `local_digest`")
	}
	for i, request := range requests {
		if request.Depth > 32 || request.Capacity <= 0 || request.Capacity > maxRequestSketchCapacity {
			return fmt.Errorf("invalid `requests[%d]`", i)
		}
		if request.Prefix >= uint64(1)<<request.Depth {
			return fmt.Errorf("`requests[%d].prefix` is out of range for its depth", i)
		}
		raw, err := base64.RawURLEncoding.DecodeString(localSketches[i])
		if err != nil || len(raw) != request.Capacity*8 {
			return fmt.Errorf("invalid local sketch %d", i)
		}
	}
	return nil
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
