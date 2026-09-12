// Package ha coordinates election with fenced, single-use application activation.
package ha

import (
	"context"
	"errors"
	"fmt"
	"time"

	"codefloe.com/pat-s/zendrite/setup/lifecycle"
)

// ErrLeadershipLost requires termination, even if application draining succeeds.
var ErrLeadershipLost = errors.New("lease leadership lost; process must exit")

// ErrFencingUnavailable prevents production activation until issue #349 supplies a fencer.
var ErrFencingUnavailable = errors.New("HA promotion blocked: no enforceable ownership/fencing provider is installed (issue #349); keep HA disabled until fencing is available")

// Candidate binds a unique process to a stable homeserver election scope.
type Candidate struct {
	Identity       string
	LeaseName      string
	LeaseNamespace string
}

// OwnershipGate must establish enforceable fencing before returning nil.
// It must honor cancellation, persist previous-owner identity outside the Lease,
// and cover delayed effects and resumed owners; local cancellation is not fencing.
// Concrete infrastructure identity and evidence handling belong to the provider.
type OwnershipGate interface {
	Acquire(context.Context, Candidate) error
}

// Application is a single-use, bounded lifecycle; Stop closes admission immediately.
type Application interface {
	Run(context.Context) error
	Stop()
}

// election runs until cancellation or ownership loss. It sends at most one
// acquired context, whose cancellation immediately revokes application ownership.
type election interface{ Run(context.Context) }

// control keeps election callbacks free of application work and joins promotion
// even when cancellation races the asynchronous acquisition callback.
//
//nolint:contextcheck // Ownership must descend from client-go's context received over the acquisition channel.
func control(ctx context.Context, elector election, acquired <-chan context.Context, failures <-chan error, candidate Candidate, gate OwnershipGate, app Application, timeout time.Duration) error {
	electionCtx, cancelElection := context.WithCancelCause(ctx)
	defer cancelElection(nil)
	electionDone := make(chan struct{})
	go func() { elector.Run(electionCtx); close(electionDone) }()
	defer func() { cancelElection(nil); <-electionDone }()

	var leader context.Context
	select {
	case <-ctx.Done():
		app.Stop()
		return nil
	case err := <-failures:
		app.Stop()
		return err
	case <-electionDone:
		app.Stop()
		if ctx.Err() != nil {
			return nil
		}
		return ErrLeadershipLost
	case leader = <-acquired:
	}
	ownership, cancelOwnership := context.WithCancelCause(leader)
	defer cancelOwnership(nil)
	// Parent cancellation must also stop promotion for controllable election implementations.
	stopParent := context.AfterFunc(ctx, func() { cancelOwnership(context.Cause(ctx)); app.Stop() })
	defer stopParent()
	stopAdmission := context.AfterFunc(ownership, app.Stop)
	defer stopAdmission()
	promoted := make(chan error, 1)
	go func() {
		if ownership.Err() != nil {
			promoted <- nil
			return
		}
		if gate == nil {
			promoted <- ErrFencingUnavailable
			return
		}
		if err := gate.Acquire(ownership, candidate); err != nil {
			promoted <- fmt.Errorf("establish fenced ownership: %w", err)
			return
		}
		if ownership.Err() != nil {
			promoted <- nil
			return
		}
		err := app.Run(ownership)
		// Stop can win between the ownership check and Run's activation lock.
		// This is an ordinary cancellation, not an attempted lifecycle reuse.
		if ownership.Err() != nil && errors.Is(err, lifecycle.ErrAlreadyStarted) {
			err = nil
		}
		promoted <- err
	}()

	var result error
	select {
	case result = <-promoted:
		app.Stop()
		if ctx.Err() != nil && errors.Is(result, context.Canceled) {
			result = nil
		}
		if ctx.Err() == nil && leader.Err() != nil {
			result = errors.Join(result, ErrLeadershipLost)
		}
		return result
	case result = <-failures:
	case <-ctx.Done():
	case <-leader.Done():
		if ctx.Err() == nil {
			result = ErrLeadershipLost
		}
	case <-electionDone:
		if ctx.Err() == nil {
			result = ErrLeadershipLost
		}
	}
	cancelOwnership(result)
	app.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-promoted:
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			err = nil
		}
		return errors.Join(result, err)
	case <-timer.C:
		return errors.Join(result, lifecycle.ErrShutdownTimeout)
	}
}
