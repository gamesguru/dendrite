package caching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// InvalidateEventStateKey unsets both partitions, so both must be mutable.
func TestInvalidateEventStateKey(t *testing.T) {
	c := NewRistrettoCache(8*1024*1024, time.Hour, false)
	c.StoreEventStateKey(types.EventStateKeyNID(7), "@alice:test")

	assert.NotPanics(t, func() { c.InvalidateEventStateKey("@alice:test") })
	// An unknown key must be a no-op, not a panic.
	assert.NotPanics(t, func() { c.InvalidateEventStateKey("@nobody:test") })
}
