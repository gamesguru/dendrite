//go:build linux

package hachaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

var (
	portsMu sync.Mutex
	ports   = map[int]bool{}
)

// freePort reserves a port for a server the harness starts later.
//
// Ports come from below the kernel's ephemeral range (32768 and up by
// default), because the kernel hands those out for outbound connections: a
// port that was free when chosen could be taken by one of the many client
// connections before the server binds it. No port is handed out twice.
func freePort() (int, error) {
	portsMu.Lock()
	defer portsMu.Unlock()
	for range 1000 {
		port := 20000 + rand.IntN(12000)
		if ports[port] {
			continue
		}
		l, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		_ = l.Close()
		ports[port] = true
		return port, nil
	}
	return 0, errors.New("no free port below the ephemeral range")
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

// errPermanent stops waitFor from retrying.
func errPermanent(err error) error { return permanentError{err} }

// waitFor retries fn until it succeeds, fails permanently, or timeout passes.
func waitFor(ctx context.Context, timeout time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	var last error
	for {
		last = fn()
		if last == nil {
			return nil
		}
		var permanent permanentError
		if errors.As(last, &permanent) {
			return permanent.error
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s: %w", timeout, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
