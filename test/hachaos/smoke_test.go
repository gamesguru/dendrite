//go:build linux

package hachaos

import (
	"testing"
	"time"
)

// TestEnvironment proves the harness itself: every dependency starts, one
// candidate activates, the others stand by, and a client can write through
// the router.
func TestEnvironment(t *testing.T) {
	s := loadSettings(t)
	ctx := t.Context()
	e := newEnvironment(t, ctx, s, "environment")
	c, err := register(ctx, e.router.url(), "smoke")
	if err != nil {
		t.Fatal(err)
	}
	room, err := c.createRoom(ctx, "smoke")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.send(ctx, room, "smoke-1", "hello"); err != nil {
		t.Fatal(err)
	}
	remote, err := register(ctx, e.remote.httpURL(), "remote-smoke")
	if err != nil {
		t.Fatal(err)
	}
	if err = remote.join(ctx, room, haServerName); err != nil {
		t.Fatalf("remote join over federation: %v", err)
	}
	t.Logf("environment ready; active candidate %s", e.router.activeSlot().name)
	time.Sleep(time.Second)
}
