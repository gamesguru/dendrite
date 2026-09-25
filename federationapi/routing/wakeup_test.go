package routing_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/federationapi/routing"
)

type wakeupRecorder struct {
	backingOff bool
	wakeups    int
}

func (w *wakeupRecorder) IsServerBackingOff(spec.ServerName) bool { return w.backingOff }

func (w *wakeupRecorder) MarkServersAlive([]spec.ServerName) { w.wakeups++ }

func TestFederationWakeupsRetryBackingOffServersSooner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		api := &wakeupRecorder{}
		wakeups := &routing.FederationWakeups{FsAPI: api}
		ctx := context.Background()

		wakeups.Wakeup(ctx, "remote")
		time.Sleep(30 * time.Second)
		wakeups.Wakeup(ctx, "remote")
		assert.Equal(t, 1, api.wakeups, "a server we are not backing off from is woken at most once a minute")

		// Our retry failed, so we back off again, yet the server keeps
		// contacting us: it must not wait for the rest of the minute.
		api.backingOff = true
		wakeups.Wakeup(ctx, "remote")
		assert.Equal(t, 2, api.wakeups)
		time.Sleep(time.Second)
		wakeups.Wakeup(ctx, "remote")
		assert.Equal(t, 2, api.wakeups, "a server we are backing off from is woken at most every few seconds")
		time.Sleep(5 * time.Second)
		wakeups.Wakeup(ctx, "remote")
		assert.Equal(t, 3, api.wakeups)
	})
}
