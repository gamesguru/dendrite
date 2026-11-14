//go:build !(rezzy && linux && cgo)

package reconciliation

import "errors"

// ErrUnavailable is returned when this build does not link rezzy.
var ErrUnavailable = errors.New("reconciliation: build without rezzy does not support MSC4521")

// Enabled reports whether this build links rezzy.
const Enabled = false

// Snapshot is unavailable in this build.
func Snapshot([]string, string) (*RoomSnapshot, error) {
	return nil, ErrUnavailable
}

// BucketSketches is unavailable in this build.
func BucketSketches([]string, string, []BucketRequest) ([]string, error) {
	return nil, ErrUnavailable
}

// BucketDiff is unavailable in this build.
func BucketDiff([]string, string, []BucketRequest, []string, string, uint64) (*BucketDiffResult, error) {
	return nil, ErrUnavailable
}
